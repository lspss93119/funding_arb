import asyncio
import logging
import os
from trading_bot.utils.logger import setup_logger
from trading_bot.config.settings import config
from trading_bot.exchanges.backpack import BackpackExchange
from trading_bot.strategies.simple_maker import SimpleMarketMaker

logger = setup_logger()

async def main():
    logger.info("Starting Trading Bot...")

    # 1. Load Config
    api_key = config.get("api_key")
    api_secret = config.get("api_secret")
    
    if not api_key or not api_secret:
        logger.warning("API credentials not found in config.json. Running in Sandbox/Dummy mode (some features may fail).")
        # You might want to exit here or prompt user
    
    # 2. Initialize Exchange
    exchange = BackpackExchange(api_key=api_key or "", api_secret=api_secret or "")
    
    # 3. Initialize Strategy
    strategy_config = config.get("strategy", {})
    strategy = SimpleMarketMaker(exchange=exchange, config=strategy_config)
    
    logger.info(f"Initialized {exchange.name} exchange and {strategy.name} strategy.")
    
    # 4. Start Strategy
    await strategy.on_start()
    
    # 5. Main Loop (Simulation)
    # In a real WebSocket impl, this would be an infinite wait on socket events.
    # Here we simulate a tick loop.
    try:
        logger.info("Entering main loop. Press Ctrl+C to stop.")
        while True:
            # Simulate fetching a ticker every 5 seconds
            ticker = await exchange.fetch_ticker("SOL_USDC")
            if ticker:
                await strategy.on_tick(ticker)
            
            await asyncio.sleep(5)
            
    except asyncio.CancelledError:
        logger.info("Main loop cancelled.")
    finally:
        await strategy.on_stop()
        await exchange.close()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
