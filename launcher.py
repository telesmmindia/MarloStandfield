import asyncio
import json
import logging
from pathlib import Path
from bot_core import LineBot

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


async def run_bot(config_path: str):
    """Load config and run one bot instance"""
    with open(config_path, 'r') as f:
        config = json.load(f)

    bot = LineBot(config)
    await bot.start()


async def main():
    """Run all bots in parallel"""
    config_dir = Path("configs")
    config_files = list(config_dir.glob("*.json"))

    if not config_files:
        logging.error("❌ No config files found in configs/")
        return

    logging.info(f"🚀 Starting {len(config_files)} bots...")

    tasks = [run_bot(str(config_file)) for config_file in config_files]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
