import logging
import time
import asyncio
import aiohttp
from typing import Dict, Any, List, Optional
from .base import Exchange
from edgex_sdk import Client, CreateOrderParams, OrderType, OrderSide, TimeInForce

logger = logging.getLogger("trading_bot.exchanges.edgex")

class EdgeXExchange(Exchange):
    BASE_URL = "https://pro.edgex.exchange"

    def __init__(self, stark_private_key: str, account_id: int, config: Dict[str, Any] = None):
        """
        Initialize EdgeX Exchange.
        stark_private_key: L2 Private Key from EdgeX
        account_id: Account ID from EdgeX
        """
        # Pass private key as api_key for base compatibility
        super().__init__(stark_private_key, str(account_id))
        self.name = "EdgeX"
        self.stark_private_key = stark_private_key
        self.account_id = int(account_id)
        self.ex_config = config or {}
        
        self.client = Client(
            base_url=self.BASE_URL,
            account_id=self.account_id,
            stark_private_key=self.stark_private_key
        )
        
        self.market_map = {} # Symbol (SOL-USDC) -> contractName (SOLUSD)
        self.contract_meta = {} # contractName -> {id: int, stepSize: float}
        self.contract_id_map = {} # Legacy map
        self._metadata_task = asyncio.create_task(self._init_metadata())

    async def _init_metadata(self):
        try:
            res = await self.client.get_metadata()
            data = self._to_dict(res)
            contracts = data.get("data", {}).get("contractList", [])
            for c in contracts:
                c_name = c.get("contractName")
                c_id = c.get("contractId")
                self.contract_id_map[c_name] = c_id
                self.contract_meta[c_name] = {
                    "id": c_id,
                    "stepSize": float(c.get("quantityStepSize") or c.get("stepSize") or 1.0)
                }
            logger.info(f"EdgeX metadata initialized: {len(self.contract_meta)} contracts.")
        except Exception as e:
            logger.error(f"Failed to fetch EdgeX metadata: {e}")

    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        """
        Helper to convert SDK response (dict or Pydantic model) to a dictionary.
        """
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, 'model_dump'):
            return obj.model_dump()
        if hasattr(obj, 'dict'):
             return obj.dict()
        # Fallback for generic objects with __dict__
        if hasattr(obj, '__dict__'):
            return vars(obj)
        return str(obj) # Fallback to string if we can't parse

    def _get_contract_name(self, symbol: str) -> str:
        # Standardize for EdgeX: "SOL-USDC" -> "SOLUSD"
        return symbol.upper().replace("-USDC", "USD").replace("_USDC", "USD").replace("-", "").replace("_", "")

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        try:
            c_name = self._get_contract_name(symbol)
            c_id = self.contract_id_map.get(c_name)
            if not c_id:
                # If metadata task is still running, wait a bit
                await asyncio.sleep(1)
                c_id = self.contract_id_map.get(c_name)
                if not c_id:
                    return {}

            res = await self.client.get_24_hour_quote(str(c_id))
            raw_data = self._to_dict(res).get("data", {})
            
            # data can be a list or a dict
            if isinstance(raw_data, list) and len(raw_data) > 0:
                data = raw_data[0]
            elif isinstance(raw_data, dict):
                data = raw_data
            else:
                data = {}

            return {
                'symbol': symbol,
                'bid': float(data.get('bidPrice', 0)),
                'ask': float(data.get('askPrice', 0)),
                'last': float(data.get('lastPrice', 0)),
                'timestamp': int(time.time() * 1000)
            }
        except Exception as e:
            logger.error(f"EdgeX fetch_ticker error: {e}")
            return {}

    async def get_balance(self) -> Dict[str, float]:
        try:
            res = await self.client.get_account_asset()
            data = self._to_dict(res).get("data", {})
            balances = {"USDC": 0.0}
            
            # Balance is in collateralAssetModelList
            collaterals = data.get("collateralAssetModelList", [])
            for asset in collaterals:
                 asset_dict = self._to_dict(asset)
                 coin_id = str(asset_dict.get("coinId"))
                 # In EdgeX, coinId 1000 is usually the main USD/USDC collateral
                 if coin_id == "1000":
                     balances["USDC"] = float(asset_dict.get("availableAmount", 0))
            return balances
        except Exception as e:
            logger.error(f"EdgeX get_balance error: {e}")
            return {}

    async def get_positions(self) -> List[Dict[str, Any]]:
        try:
            res = await self.client.get_account_positions()
            data = self._to_dict(res).get("data", [])
            positions = []
            
            if isinstance(data, list):
                for pos in data:
                    p = self._to_dict(pos)
                    c_name = p.get("contractName")
                    size = float(p.get("positionSize", 0))
                    # Side can be "BUY" or "SELL"
                    if p.get("side") == "SELL":
                        size = -abs(size)
                    
                    if abs(size) > 0:
                        positions.append({
                            "symbol": c_name,
                            "size": size,
                            "entry_price": float(p.get("entryPrice", 0)),
                            "unrealized_pnl": float(p.get("unrealizedPnl", 0))
                        })
            return positions
        except Exception as e:
            logger.error(f"EdgeX get_positions error: {e}")
            return []

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, float]:
        try:
            c_name = self._get_contract_name(symbol)
            c_id = self.contract_id_map.get(c_name)
            
            # Using public API endpoint
            url = f"{self.BASE_URL}/api/v1/public/funding/getLatestFundingRate"
            async with aiohttp.ClientSession() as session:
                params = {"contractId": c_id} if c_id else {}
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rates = data.get("data", [])
                        for r in rates:
                             if str(r.get("contractId")) == str(c_id):
                                 return {
                                     "symbol": symbol,
                                     "funding_rate": float(r.get("fundingRate", 0)) / 4.0,
                                     "timestamp": int(time.time() * 1000)
                                 }
            return {}
        except Exception as e:
            logger.error(f"EdgeX fetch_funding_rate error: {e}")
            return {}

    async def create_order(self, symbol: str, side: str, order_type: str, price: float, quantity: float) -> Dict[str, Any]:
        try:
            c_name = self._get_contract_name(symbol)
            c_id = self.contract_id_map.get(c_name)
            if not c_id:
                return {"error": f"Contract ID for {c_name} not found"}
                
            p_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
            p_type = OrderType.LIMIT if order_type.lower() == "limit" else OrderType.MARKET
            
            # EdgeX might require a price even for MARKET orders or for signing.
            # If MARKET and price is 0, fetch ticker to get a reference price.
            if p_type == OrderType.MARKET and price <= 0:
                ticker = await self.fetch_ticker(symbol)
                if ticker and ticker.get('last'):
                    ref_price = ticker['last']
                    # Apply 1% slippage for market orders
                    if p_side == OrderSide.BUY:
                        price = ref_price * 1.01
                    else:
                        price = ref_price * 0.99
                else:
                    return {"error": "Could not fetch ticker for MARKET order price"}

            # Truncate quantity to stepSize
            meta = self.contract_meta.get(c_name, {})
            step_size = meta.get("stepSize", 1.0)
            import math
            if step_size < 1:
                precision = int(round(-math.log10(step_size)))
                truncated_qty = math.floor(quantity * (10**precision)) / (10**precision)
                qty_str = f"{truncated_qty:.{precision}f}"
            else:
                qty_str = str(int(quantity / step_size) * int(step_size))
            
            params = CreateOrderParams(
                contract_id=str(c_id),
                side=p_side,
                type=p_type,
                price=str(price),
                size=qty_str,
                time_in_force=TimeInForce.GTC if p_type == OrderType.LIMIT else None
            )
            
            res = await self.client.create_order(params)
            return self._to_dict(res)
        except Exception as e:
            logger.error(f"EdgeX create_order error: {e}")
            return {"error": str(e)}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
             c_name = self._get_contract_name(symbol)
             c_id = self.contract_id_map.get(c_name)
             if not c_id: return False
             await self.client.cancel_order(str(c_id), order_id)
             return True
        except Exception as e:
            logger.error(f"EdgeX cancel_order error: {e}")
            return False

    async def close(self):
        if self._metadata_task:
            self._metadata_task.cancel()
        await self.client.close()
