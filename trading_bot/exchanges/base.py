from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

class Exchange(ABC):
    """
    Abstract Base Class for all exchanges.
    Enforces a standard interface for the strategy engine to interact with.
    """

    def __init__(self, api_key: str, api_secret: str, sandbox: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
        self.name = "GenericExchange"

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetches the current ticker for a given symbol.
        Should return a standardized dictionary:
        {
            'symbol': 'SOL_USDC',
            'bid': 100.5,
            'ask': 100.6,
            'last': 100.55,
            'timestamp': 1234567890
        }
        """
        pass

    @abstractmethod
    async def get_balance(self) -> Dict[str, float]:
        """
        Fetches account balance.
        Should return a dictionary of asset -> amount:
        {
            'SOL': 10.5,
            'USDC': 5000.0
        }
        """
        pass

    @abstractmethod
    async def create_order(self, symbol: str, side: str, order_type: str, price: float, quantity: float) -> Dict[str, Any]:
        """
        Places an order.
        side: 'buy' or 'sell'
        order_type: 'limit' or 'market'
        """
        pass

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancels a specific order.
        """
        pass

    @abstractmethod
    async def fetch_funding_rate(self, symbol: str) -> Dict[str, float]:
        """
        Fetch current funding rate.
        Returns:
            {
                "symbol": str,
                "funding_rate": float,
                "next_funding_time": int (timestamp)
            }
        """
        pass

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        Fetch current open positions.
        Returns List of dicts:
        [
            {
                "symbol": "SOL_USDC",
                "size": 1.5,
                "entry_price": 102.5,
                "unrealized_pnl": 0.5
            }
        ]
        """
        pass
