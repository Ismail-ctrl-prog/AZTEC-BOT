import asyncio
import json
import logging
import os
import sys
import time
from decimal import Decimal

from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("EliteBot")

# Load environment variables
load_dotenv()

RPC_WSS_URL = os.getenv("RPC_WSS_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
# Default to 4 Gwei if not set.
MAX_GAS_GWEI = Decimal(os.getenv("MAX_GAS_GWEI", "4"))
CONTRACT_ADDRESS = "0x7C9a7130379F1B5dd6e7A53AF84fC0fE32267B65"
EPOCH_DURATION = 2304  # 38.4 minutes in seconds

# Helper: Load ABI
try:
    with open("abi.json", "r") as f:
        CONTRACT_ABI = json.load(f)
except FileNotFoundError:
    logger.error("abi.json not found.")
    sys.exit(1)

class EliteBot:
    def __init__(self):
        if not RPC_WSS_URL:
            logger.error("RPC_WSS_URL is not set in .env")
            # We don't exit here to allow dry-run without env in CI/Test
            # But in production it will fail to connect.

        if not PRIVATE_KEY:
            logger.error("PRIVATE_KEY is not set in .env")

        # Initialize AsyncWeb3
        if RPC_WSS_URL:
            # Use AsyncWeb3.WebSocketProvider for async context
            self.w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(RPC_WSS_URL))
        else:
            # Fallback for testing/linting without URL
            self.w3 = AsyncWeb3()

        if PRIVATE_KEY:
            self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        else:
            self.account = None

        self.contract = self.w3.eth.contract(address=CONTRACT_ADDRESS, abi=CONTRACT_ABI)
        self.nonce = None

    async def get_last_flush_time(self):
        """
        Queries the contract events to find the last 'Flushed' event timestamp.
        """
        try:
            current_block = await self.w3.eth.block_number
            # Look back approx 5000 blocks (~16 hours)
            from_block = max(0, current_block - 5000)

            # Fetch events
            events = await self.contract.events.Flushed.get_logs(fromBlock=from_block)

            if not events:
                logger.warning("No Flushed events found in the last 5000 blocks.")
                return 0

            # Get the last event
            last_event = events[-1]
            block_number = last_event['blockNumber']
            block = await self.w3.eth.get_block(block_number)
            return block['timestamp']

        except Exception as e:
            logger.error(f"Error fetching last flush time: {e}")
            return 0

    async def prepare_transaction(self):
        """
        Prepares the transaction dictionary.
        Sets gas price and checks against the cap.
        """
        try:
            if not self.account:
                logger.error("No account loaded.")
                return None

            self.nonce = await self.w3.eth.get_transaction_count(self.account.address)

            # Get latest block for base fee
            block = await self.w3.eth.get_block('latest')
            base_fee = Decimal(block['baseFeePerGas'])

            # Priority fee: 1.5 Gwei
            priority_fee_gwei = Decimal("1.5")
            priority_fee_wei = self.w3.to_wei(priority_fee_gwei, 'gwei')

            # Max fee = Base + Priority
            max_fee_wei = int(base_fee + priority_fee_wei)

            # Check Cap
            max_fee_gwei = self.w3.from_wei(max_fee_wei, 'gwei')
            if max_fee_gwei > MAX_GAS_GWEI:
                logger.warning(f"Projected Gas Price ({max_fee_gwei:.2f} Gwei) exceeds Cap ({MAX_GAS_GWEI} Gwei).")
                return None

            # Estimate gas?
            # If the function is not callable yet, estimation will fail.
            # We must use a fallback gas limit to pre-sign.
            gas_limit = 1000000 # Generous limit

            tx_params = {
                'from': self.account.address,
                'nonce': self.nonce,
                'maxPriorityFeePerGas': priority_fee_wei,
                'maxFeePerGas': max_fee_wei,
                'type': '0x2',
                'chainId': await self.w3.eth.chain_id,
                'gas': gas_limit,
            }

            # Build transaction data (data field)
            # We can use build_transaction but populate gas to skip estimation
            tx = await self.contract.functions.flushEntryQueue().build_transaction(tx_params)

            return tx

        except Exception as e:
            logger.error(f"Error preparing transaction: {e}")
            return None

    async def poll_readiness(self, next_epoch_time):
        """
        Waits for the countdown and enters high-frequency polling.
        """
        now = time.time()
        wait_time = next_epoch_time - 30 - now

        if wait_time > 0:
            logger.info(f"Sleeping for {wait_time:.2f}s until pre-strike window...")
            await asyncio.sleep(wait_time)

        logger.info("Entering Pre-Strike Readiness (30s window).")

        # Prepare and sign transaction
        tx = await self.prepare_transaction()
        if not tx:
            logger.error("Transaction preparation failed or gas too high. Skipping this epoch.")
            return

        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
        logger.info("Transaction signed and ready.")

        # High-frequency loop
        # We run until we successfully strike or timeout
        while True:
            try:
                # 1. Check if callable (Simulation)
                # Using call() to simulate execution.
                try:
                    await self.contract.functions.flushEntryQueue().call({'from': self.account.address})
                    # If we reach here, it didn't revert!
                    logger.info("Function is CALLABLE! Striking...")

                    # 2. Instant Broadcast
                    tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
                    logger.info(f"Transaction broadcasted: {self.w3.to_hex(tx_hash)}")

                    # 3. Wait for receipt
                    receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
                    if receipt['status'] == 1:
                        logger.info(f"Success! Flushed in block {receipt['blockNumber']}")
                    else:
                        logger.error(f"Transaction failed (reverted) in block {receipt['blockNumber']}")
                    return

                except ContractLogicError:
                    # Still reverted, meaning not yet ready.
                    pass
                except Exception as e:
                    # Unexpected error in call
                    # logger.debug(f"Simulate error: {e}")
                    pass

                # Check timeout (e.g., 2 minutes past expected time)
                if time.time() > next_epoch_time + 120:
                    logger.warning("Timeout waiting for epoch start.")
                    return

                # Interval
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in poll loop: {e}")
                await asyncio.sleep(0.1)

    async def run(self):
        """
        Main bot loop.
        """
        logger.info("Bot starting...")

        # Connection check
        if not await self.w3.is_connected():
            logger.error("Failed to connect to RPC. Check URL.")
            return

        logger.info(f"Connected to RPC. Address: {self.account.address if self.account else 'None'}")

        while True:
            try:
                # Sync Epoch
                last_flush = await self.get_last_flush_time()

                if last_flush == 0:
                    logger.warning("Could not sync epoch. Retrying in 10s...")
                    await asyncio.sleep(10)
                    continue

                next_epoch = last_flush + EPOCH_DURATION
                now = time.time()
                time_until = next_epoch - now

                logger.info(f"Last Flush: {last_flush}, Next Epoch: {next_epoch}, Time Until: {time_until:.2f}s")

                if time_until < -30:
                    # We are late.
                    logger.info("Current time is past the target. Checking if we can strike immediately.")
                    # Try to poll now
                    await self.poll_readiness(now + 5)
                else:
                    await self.poll_readiness(next_epoch)

                # Sleep a bit to let the chain update before next sync
                await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    bot = EliteBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.error(f"Fatal startup error: {e}")
