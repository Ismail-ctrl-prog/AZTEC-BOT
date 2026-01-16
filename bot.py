import asyncio
import binascii
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
FLUSHED_TOPIC_HASH = "0xbf22ffbd5b2510ba175b84f0b24b0cf8d8f6eb19e2d498257976a51cc6660bf0"

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
        self.last_scan_time = 0

    async def scan_range(self, from_block, to_block):
        """
        Helper to scan a range of blocks for the Flushed event.
        Returns list of events or empty list.
        """
        try:
            logs = await self.w3.eth.get_logs({
                'address': CONTRACT_ADDRESS,
                'topics': [FLUSHED_TOPIC_HASH],
                'fromBlock': from_block,
                'toBlock': to_block
            })
            return logs
        except (binascii.Error, Exception) as e:
            logger.warning(f"Error scanning range {from_block}-{to_block}: {e}")
            return []

    async def perform_deep_scan(self):
        """
        Deep scan logic:
        1. Check last 1000 blocks.
        2. If failed, loop back 5000 blocks at a time up to 50000.
        Returns: last_flush_timestamp (int) or 0 if not found.
        """
        try:
            current_block = await self.w3.eth.block_number
            logger.info(f"Starting Deep Scan from block {current_block}...")

            # 1. Check last 1000 blocks
            start_block = max(0, current_block - 1000)
            events = await self.scan_range(start_block, current_block)

            if events:
                last_event = events[-1]
                logger.info(f"Event found in initial scan at block {last_event['blockNumber']}")
                blk = await self.w3.eth.get_block(last_event['blockNumber'])
                self.last_checked_block = current_block
                return blk['timestamp']

            # 2. Sync Recovery: Loop back up to 50,000 blocks
            # We already checked up to current-1000.
            # Next chunk: current-1000-5000 to current-1000
            offset = 1000
            limit = 50000

            while offset < limit:
                end_block = max(0, current_block - offset)
                start_block = max(0, end_block - 5000)

                if end_block <= 0:
                    break

                logger.info(f"Scanning history: {start_block} to {end_block}")
                events = await self.scan_range(start_block, end_block)

                if events:
                    last_event = events[-1]
                    logger.info(f"Event found during deep scan at block {last_event['blockNumber']}")
                    blk = await self.w3.eth.get_block(last_event['blockNumber'])
                    self.last_checked_block = current_block
                    return blk['timestamp']

                offset += 5000
                await asyncio.sleep(0.5) # Slight delay to be nice to RPC

            logger.warning("No Flushed event found in the last 50,000 blocks.")
            return 0

        except Exception as e:
            logger.error(f"Deep scan failed: {e}")
            return 0

    async def scan_recent_blocks(self):
        """
        Scans the most recent 1000 blocks.
        """
        try:
            current_block = await self.w3.eth.block_number
            # Avoid re-scanning if we are up to date?
            # User requirement: "Search the most recent 1,000 blocks every 2 seconds."
            # We strictly follow this.

            start_block = max(0, current_block - 1000)
            events = await self.scan_range(start_block, current_block)

            if events:
                last_event = events[-1]
                # Check if this event is newer than our known last_flush_time
                blk = await self.w3.eth.get_block(last_event['blockNumber'])
                ts = blk['timestamp']

                if ts > self.last_flush_time:
                    logger.info(f"New Flushed event detected via poll at {ts}. Resyncing.")
                    self.last_flush_time = ts
                    # Reset last_checked_block to current to avoid gap issues if we used it elsewhere,
                    # though perform_deep_scan sets it.
                    self.last_checked_block = current_block
                    return True
            return False

        except Exception as e:
            logger.error(f"Recent block scan failed: {e}")
            return False

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
        Main bot loop with robust scanning and strike logic.
        """
        logger.info("Bot starting...")

        # Connection check
        if not await self.w3.is_connected():
            logger.error("Failed to connect to RPC. Check URL.")
            return

        logger.info(f"Connected to RPC. Address: {self.account.address if self.account else 'None'}")

        # Initial Sync
        flush_ts = await self.perform_deep_scan()
        if flush_ts > 0:
            self.last_flush_time = flush_ts
            logger.info(f"Synced from chain. Last Flush: {self.last_flush_time}")
        else:
            self.last_flush_time = time.time()
            logger.warning(f"Could not sync from chain. Fallback to Manual Sync: {self.last_flush_time}")

        while True:
            try:
                now = time.time()
                next_window = self.last_flush_time + EPOCH_DURATION

                # Chunked Polling: Search recent 1000 blocks every 2 seconds
                if now - self.last_scan_time >= 2:
                    if await self.scan_recent_blocks():
                        # If resync happened, recalculate next_window immediately
                        next_window = self.last_flush_time + EPOCH_DURATION
                    self.last_scan_time = now

                # Check Countdown
                time_until = next_window - now
                if time_until <= 0:
                    logger.info("Countdown hit zero. Attempting strike...")
                    success = await self.execute_strike()
                    if success:
                        self.last_flush_time = time.time()
                        logger.info("Strike successful. Timer reset.")
                    else:
                        # logger.info("Strike condition not met or failed. Retrying next tick.")
                        pass
                else:
                    # Log occasional status
                    if int(now) % 60 == 0:
                        logger.info(f"Waiting... Time until next window: {time_until:.2f}s")

                # Poll interval for strike logic
                await asyncio.sleep(0.1)

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
