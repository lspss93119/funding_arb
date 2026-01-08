from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from trading_bot.exchanges.base import Exchange
import logging

class Strategy(ABC):
    def __init__(self, exchange: Exchange, config: Dict[str, Any]):
        self.exchange = exchange
        self.config = config
        self.logger = logging.getLogger(f"strategy.{self.name}")

    @property
    @abstractmethod
    def name(self) -> str:
        return "BaseStrategy"

    @abstractmethod
    async def on_tick(self, market_data: Dict[str, Any]):
        """
        Called when new market data is received.
        market_data: { 'symbol': 'SOL_USDC', 'last': 100.5, ... }
        """
        pass

    @abstractmethod
    async def on_start(self):
        """
        Called when the strategy starts.
        """
        pass

    @abstractmethod
    async def on_stop(self):
        """
        Called when the strategy stops.
        """
        pass
