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

RPC_URL = os.getenv("RPC_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
# Default to 3 Gwei if not set.
MAX_GAS_GWEI = Decimal(os.getenv("MAX_GAS_GWEI", "3"))
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
        if not RPC_URL:
            logger.error("RPC_URL is not set in .env")
            # We don't exit here to allow dry-run without env in CI/Test
            # But in production it will fail to connect.

        if not PRIVATE_KEY:
            logger.error("PRIVATE_KEY is not set in .env")

        # Initialize AsyncWeb3
        if RPC_URL:
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
        else:
            # Fallback for testing/linting without URL
            self.w3 = AsyncWeb3()

        if PRIVATE_KEY:
            self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        else:
            self.account = None

        self.contract = self.w3.eth.contract(address=CONTRACT_ADDRESS, abi=CONTRACT_ABI)
        self.nonce = None
        self.last_flush_time = 0
        self.last_checked_block = 0

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

    async def execute_strike(self):
        """
        Attempts to execute the flush transaction immediately.
        """
        try:
            # Prepare and sign transaction
            tx = await self.prepare_transaction()
            if not tx:
                logger.error("Transaction preparation failed or gas too high. Skipping strike attempt.")
                return False

            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)

            # Simulation call
            try:
                await self.contract.functions.flushEntryQueue().call({'from': self.account.address})
            except ContractLogicError:
                # Not ready yet
                return False
            except Exception as e:
                logger.warning(f"Simulation error: {e}")
                return False

            logger.info("Function is CALLABLE! Striking...")

            # Instant Broadcast
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"Transaction broadcasted: {self.w3.to_hex(tx_hash)}")

            # Wait for receipt
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
            if receipt['status'] == 1:
                logger.info(f"Success! Flushed in block {receipt['blockNumber']}")
                return True
            else:
                logger.error(f"Transaction failed (reverted) in block {receipt['blockNumber']}")
                return False

        except Exception as e:
            logger.error(f"Error in execute_strike: {e}")
            return False

    async def run(self):
        """
        Main bot loop with manual sync logic.
        """
        logger.info("Bot starting...")

        # Connection check
        if not await self.w3.is_connected():
            logger.error("Failed to connect to RPC. Check URL.")
            return

        logger.info(f"Connected to RPC. Address: {self.account.address if self.account else 'None'}")

        # Manual Start Logic
        self.last_flush_time = time.time()
        try:
            self.last_checked_block = await self.w3.eth.block_number
        except Exception:
            self.last_checked_block = 0

        logger.info(f"Manual Sync Initialized. Base Time: {self.last_flush_time}")

        while True:
            try:
                now = time.time()
                next_window = self.last_flush_time + EPOCH_DURATION

                # Periodically check for external flush events to resync
                try:
                    current_block = await self.w3.eth.block_number
                    if self.last_checked_block and current_block > self.last_checked_block:
                        # Ensure using from_block (snake_case)
                        events = await self.contract.events.Flushed.get_logs(from_block=self.last_checked_block + 1)
                        if events:
                            last_event = events[-1]
                            # Update last_flush_time based on event timestamp
                            blk = await self.w3.eth.get_block(last_event['blockNumber'])
                            self.last_flush_time = blk['timestamp']
                            next_window = self.last_flush_time + EPOCH_DURATION
                            logger.info(f"Detected external flush at {self.last_flush_time}. Resyncing. Next window: {next_window}")
                            self.last_checked_block = current_block
                            # Reset loop to recalculate with new window
                            continue

                        self.last_checked_block = current_block
                except Exception as e:
                    logger.debug(f"Event check failed: {e}")

                # Check Countdown
                time_until = next_window - now
                if time_until <= 0:
                    logger.info("Countdown hit zero. Attempting strike...")
                    success = await self.execute_strike()
                    if success:
                        self.last_flush_time = time.time()
                        logger.info("Strike successful. Timer reset.")
                    else:
                        logger.info("Strike condition not met or failed. Retrying next tick.")
                else:
                    # Log occasional status
                    if int(now) % 60 == 0:
                        logger.info(f"Waiting... Time until next window: {time_until:.2f}s")

                # Poll interval
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    bot = EliteBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.error(f"Fatal startup error: {e}")
