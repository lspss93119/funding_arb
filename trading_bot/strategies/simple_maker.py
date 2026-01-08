import asyncio
from typing import Dict, Any
from .base_strategy import Strategy

class SimpleMarketMaker(Strategy):
    @property
    def name(self) -> str:
        return "SimpleMarketMaker"

    async def on_start(self):
        self.logger.info("Strategy started. Fetching initial balance...")
        balance = await self.exchange.get_balance()
        self.logger.info(f"Initial Balance: {balance}")

    async def on_stop(self):
        self.logger.info("Strategy stopped.")

    def __init__(self, exchange, config):
        super().__init__(exchange, config)
        self.has_traded = False

    async def on_tick(self, market_data: Dict[str, Any]):
        symbol = market_data.get('symbol')
        price = market_data.get('last')
        
        self.logger.info(f"Tick received for {symbol}: {price}")

        if not price:
            self.logger.warning("Price is 0 or invalid, skipping logic.")
            return
        
        # Test Logic: Place ONE safe order
        if not self.has_traded:
            safe_price = round(price * 0.9, 2) # 90% of market price (Safe but acceptable)
            quantity = self.config.get("amount", 0.1)
            
            self.logger.info(f"Attempting to place test BUY order at {safe_price}...")
            
            try:
                order = await self.exchange.create_order(
                    symbol=symbol,
                    side="buy",
                    order_type="Limit",
                    price=safe_price,
                    quantity=quantity
                )
                
                if order:
                    self.logger.info(f"Order placed successfully! Details: {order}")
                    self.has_traded = True # Stop after one trade
                else:
                    self.logger.error("Order placement returned None/Empty.")
            except Exception as e:
                self.logger.error(f"Failed to place order: {e}")

