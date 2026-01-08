import aiohttp
import time
import base64
import json
import logging
from typing import Dict, Any, Optional, List
from .base import Exchange

# Check if we can import our signing utility
try:
    from trading_bot.utils.signing import get_signature
    HAS_CRYPTO = True
except ImportError as e:
    HAS_CRYPTO = False
    print(f"DEBUG: Signing import failed: {e}")

logger = logging.getLogger("trading_bot.exchanges.backpack")

class BackpackExchange(Exchange):
    BASE_URL = "https://api.backpack.exchange"

    def __init__(self, api_key: str, api_secret: str, sandbox: bool = False):
        super().__init__(api_key, api_secret, sandbox)
        self.name = "Backpack"
        self._session = None
        self.market_meta = {} # Cache for market filters
        self.markets = {}
        if not HAS_CRYPTO:
            logger.warning("ed25519 library not found. Signing will fail.")

    async def _get_session(self):
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _generate_signature(self, instruction: str, params: Dict[str, Any], timestamp: int, window: int) -> str:
        if not HAS_CRYPTO:
            return "dummy_sig"
            
        try:
            from trading_bot.utils.signing import get_signature
            return get_signature(instruction, params, self.api_secret, timestamp, window)
        except Exception as e:
            logger.error(f"Signing failed: {e}")
            return ""

    async def _request(self, method: str, endpoint: str, params: Dict[str, Any] = None, signed: bool = False, instruction: str = "") -> Any:
        url = f"{self.BASE_URL}{endpoint}"
        ts = int(time.time() * 1000)
        window = 5000
        headers = {
            "Content-Type": "application/json",
            "X-Timestamp": str(ts),
            "X-Window": str(window)
        }

        if signed:
            headers["X-API-Key"] = self.api_key
            headers["X-Signature"] = self._generate_signature(instruction, params or {}, ts, window)

        session = await self._get_session()
        
        try:
            # print(f"DEBUG: Request {method} {url}")
            if method == "GET":
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        text = await response.text()
                        logger.error(f"API Error {response.status}: {text}")
                        print(f"[API ERROR] {response.status}: {text}")
                        return None
            elif method == "POST":
                async with session.post(url, json=params, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            text = await response.text()
                            logger.error(f"API Error {response.status}: {text}")
                            print(f"[API ERROR] {response.status}: {text}")
                            return None
            elif method == "DELETE":
                # Some DELETE endpoints require JSON body (like orderCancelAll), others might use params.
                # Backpack usually expects JSON body for signed actions.
                async with session.delete(url, json=params, headers=headers) as response:
                    if response.status == 200:
                            return await response.json()
                    else:
                            text = await response.text()
                            logger.error(f"API Error {response.status}: {text}")
                            print(f"[API ERROR] {response.status}: {text}")
                            return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            print(f"Request failed: {e}")
            return None

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch ticker for a symbol.
        """
        data = await self._request("GET", "/api/v1/ticker", params={"symbol": symbol})
        if not data:
            return {}
            
        # Backpack Ticker format: 
        # {'symbol': 'SOL_USDC', 'lastPrice': '127.94', 'low': '124.19', 'high': '129.84', ...}
        return {
            "symbol": data.get("symbol"),
            "bid": float(data.get("bestBid", 0)), # Backpack ticker might not have bid/ask in this endpoint, but let's keep safe get
            "ask": float(data.get("bestAsk", 0)), 
            "last": float(data.get("lastPrice", 0)),
            "timestamp": time.time()
        }

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch ticker info (Last Price).
        Endpoint: /api/v1/ticker
        """
        target_symbol = symbol.replace("-", "_")
        params = {"symbol": target_symbol}
        data = await self._request("GET", "/api/v1/ticker", params=params)
        
        # Responses: {"symbol": "...", "lastPrice": "...", ...}
        if not data:
            return {}
            
        return {
            "symbol": symbol,
            "last_price": float(data.get("lastPrice", 0)),
            "price": float(data.get("lastPrice", 0)),
            "timestamp": time.time()
        }

    async def fetch_markets(self):
        """
        Fetch market metadata to populate tick sizes.
        Endpoint: /api/v1/markets
        """
        try:
            data = await self._request("GET", "/api/v1/markets")
            if data and isinstance(data, list):
                for m in data:
                    symbol = m.get("symbol")
                    filters = m.get("filters", {})
                    # quantity includes stepSize
                    qty_filter = filters.get("quantity", {})
                    if "stepSize" in qty_filter:
                         self.market_meta[symbol] = {
                             "stepSize": float(qty_filter["stepSize"]),
                             "minQty": float(qty_filter.get("minQuantity", 0))
                         }
                    
        except Exception as e:
            logger.error(f"Error fetching Backpack markets: {e}")

    def _truncate(self, value: float, step: float) -> str:
        if step == 0: return str(value)
        # Calculate precision from step size (e.g. 0.01 -> 2)
        import math
        if step >= 1:
            precision = 0
        else:
            precision = int(abs(math.log10(step)))
            
        # Truncate (floor) to precision
        format_str = f"{{:.{precision}f}}"
        return format_str.format(int(value / step) * step)


    async def _get_tick_size(self, symbol: str) -> float:
        """
        Get tick size for a symbol, fetching metadata if missing.
        Default to 0.01 if not found.
        """
        norm_symbol = symbol.replace("-", "_")
        if norm_symbol not in self.market_meta:
            await self.fetch_markets()
            
        meta = self.market_meta.get(norm_symbol, {})
        return meta.get("tickSize", 0.01)

    async def fetch_order_book(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch order book (depth).
        Endpoint: /api/v1/depth
        """
        target_symbol = symbol.replace("-", "_")
        params = {"symbol": target_symbol}
        # Backpack default is 100, but they support param 'limit' if API allows, or we slice client side.
        # Docs say limit defaults to 1000.
        
        data = await self._request("GET", "/api/v1/depth", params=params)
        if not data:
            return {"bids": [], "asks": [], "timestamp": time.time()}

        # Format: {'asks': [['102.5', '1.2'], ...], 'bids': [['102.4', '0.5'], ...], ...}
        # Backpack returns Bids in Ascending order (Worst -> Best), we need Descending (Best -> Worst).
        # Asks are usually Ascending (Best -> Worst), which is correct.
        
        parse_level = lambda x: [float(x[0]), float(x[1])]
        
        raw_bids = data.get("bids", [])
        raw_asks = data.get("asks", [])
        
        # Sort Bids: High Price first (Descending)
        bids = sorted([parse_level(x) for x in raw_bids], key=lambda x: x[0], reverse=True)[:limit]
        
        # Sort Asks: Low Price first (Ascending)
        asks = sorted([parse_level(x) for x in raw_asks], key=lambda x: x[0])[:limit]
        
        return {
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": data.get("timestamp", time.time())
        }

    async def get_balance(self) -> Dict[str, float]:
        """
        Fetch balances, including Spot Capital and Collateral (Borrow/Lend).
        """
        # 1. Fetch Spot Capital
        data = await self._request("GET", "/api/v1/capital", signed=True, instruction="balanceQuery")
        
        balances = {}
        if data:
            for symbol, details in data.items():
                if isinstance(details, dict):
                    balances[symbol] = float(details.get('available', 0))
                    
        # 2. Fetch Collateral (Auto-Borrow/Lend)
        try:
            collateral = await self.get_borrow_lend_positions()
            for symbol, amount in collateral.items():
                # Add collateral to existing balance or set it
                # Logic: Total Balance = Spot Available + Net Collateral
                balances[symbol] = balances.get(symbol, 0.0) + amount
        except Exception as e:
            logger.warning(f"Failed to fetch collateral: {e}")
            
        return balances

    async def create_order(self, symbol: str, side: str, order_type: str, price: float = 0.0, quantity: float = 0.0) -> Dict[str, Any]:
        # Backpack Side: "Bid" (Buy) or "Ask" (Sell)
        side_enum = "Bid" if side.lower() == "buy" else "Ask"
        target_symbol = symbol.replace("-", "_")
        is_converted_market = False
        
        # --- Market-as-Limit Logic ---
        # Backpack rejects Market orders if using Lending Balance (0 Spot).
        # We convert Market -> Aggressive Limit (IOC) to bypass this.
        if order_type.lower() == "market":
            is_converted_market = True
            logger.info(f"Converting MARKET order to LIMIT(IOC) for {target_symbol}...")
            # 1. Try to get Price from Order Book (More accurate than Last Price)
            # Buy -> Want to match Seller (Ask)
            # Sell -> Want to match Buyer (Bid)
            
            logger.info("Backpack: Fetching Order Book for BBO...")
            ob = await self.fetch_order_book(target_symbol)
            reference_price = 0.0
            
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            
            if side_enum == "Bid":
                if asks:
                    # Best Ask is the first one (lowest price)
                    # Format: [['98000', '0.1'], ...]
                    reference_price = float(asks[0][0])
                    # logger.info(f"Backpack: Using Best Ask {reference_price} as Ref")
                else:
                    logger.warning("Backpack: No Asks found, checking Last Price...")
            else:
                if bids:
                    # Best Bid is the first one (highest price)
                    reference_price = float(bids[0][0])
                    # logger.info(f"Backpack: Using Best Bid {reference_price} as Ref")
                else:
                    logger.warning("Backpack: No Bids found, checking Last Price...")
            
            # Fallback to Ticker if Order Book failed
            if reference_price == 0.0:
                logger.warning("Backpack: Fallback to Ticker Last Price")
                ticker = await self.fetch_ticker(target_symbol)
                reference_price = ticker.get("last_price", 0.0)
            
            if reference_price > 0:
                slippage = 0.001 # 0.1%
                if side_enum == "Bid":
                    limit_price = reference_price * (1 + slippage)
                else:
                    limit_price = reference_price * (1 - slippage)
                
                # Dynamic Precision Rounding
                tick_size = await self._get_tick_size(target_symbol)
                
                # Round to nearest tick_size
                raw_price = round(limit_price / tick_size) * tick_size
                
                if tick_size < 1:
                     decimals = len(str(tick_size).split(".")[-1])
                     price = float(f"{raw_price:.{decimals}f}")
                else:
                     price = float(int(raw_price))
                     
                order_type = "Limit"
                logger.info(f"Backpack Market-as-Limit: Side={side}, Ref={reference_price}, Slip={slippage*100}%, Tick={tick_size}, Final={price}")
                # print(f"DEBUG: Backpack Market-as-Limit: {price} (Ref: {reference_price}, Tick: {tick_size})")
            else:
                logger.error("Failed to fetch price for Market-as-Limit conversion.")
                # Fallback to original (will likely fail)
        # -----------------------------
        
        # --- Quantity Precision ---
        # Ensure quantity respects stepSize
        if not self.market_meta:
            await self.fetch_markets()

        step_size = 0.001 # Default fallback
        if target_symbol in self.market_meta:
            step_size = self.market_meta[target_symbol].get("stepSize", 0.001)
        
        # Calculate precision from stepSize (e.g. 0.01 -> 2)
        import math
        qty_precision = max(0, int(round(-math.log10(step_size))))
        
        # Truncate to precision (don't round up to avoid balance issues)
        factor = 10**qty_precision
        truncated_qty = math.floor(quantity * factor) / factor
        formatted_qty = f"{truncated_qty:.{qty_precision}f}"
        logger.debug(f"Backpack Precision: Step={step_size}, Prec={qty_precision}, Raw={quantity}, Final={formatted_qty}")
            
        payload = {
            "symbol": target_symbol,
            "side": side_enum,
            "orderType": order_type.capitalize(), # Ensure Limit/Market is capitalized
            "quantity": formatted_qty
        }
        
        # Only add price if it's NOT a Market order (which it shouldn't be now if converted)
        if order_type.lower() != "market":
            payload["price"] = str(price)
            
            # If we converted Market -> Limit, we want IOC.
            # If original was Limit, default to GTC (unless specified, which we don't support in args yet)
            # Since we overwrote 'order_type' to "Limit" in conversion block, we check the original logic flow.
            # We can infer it: if we calculated specific price and it WAS 'market' originally.
            # Wait, I can just use a flag variable.
           
        if is_converted_market:
             payload["timeInForce"] = "IOC"

        return await self._request("POST", "/api/v1/order", params=payload, signed=True, instruction="orderExecute")

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        payload = {
            "symbol": symbol.replace("-", "_"),
            "orderId": order_id
        }
        res = await self._request("DELETE", "/api/v1/order", params=payload, signed=True, instruction="orderCancel")
        return res is not None

    async def get_open_orders(self, symbol: str = None) -> list:
        """
        Fetch open orders.
        Endpoint: /api/v1/orders
        """
        if symbol:
            params = {}
            params["symbol"] = symbol.replace("-", "_")
            
        data = await self._request("GET", "/api/v1/orders", params=params, signed=True, instruction="orderQueryAll")
        return data if isinstance(data, list) else []

    async def cancel_all_orders(self, symbol: str) -> bool:
        """
        Cancel all open orders for a symbol.
        Endpoint: DELETE /api/v1/orders
        """
        payload = {"symbol": symbol}
        res = await self._request("DELETE", "/api/v1/orders", params=payload, signed=True, instruction="orderCancelAll")
        return res is not None

    async def get_borrow_lend_positions(self) -> Dict[str, float]:
        """
        Fetch borrow/lend positions (Collateral).
        Endpoint: /api/v1/borrowLend/positions
        """
        data = await self._request("GET", "/api/v1/borrowLend/positions", signed=True, instruction="borrowLendPositionQuery")
        if not data or not isinstance(data, list):
            return {}
            
        balances = {}
        for item in data:
            symbol = item.get("symbol")
            # netQuantity is the actual holding (positive for lending/collat, negative for borrowing)
            amount = float(item.get("netQuantity", 0))
            if symbol:
                balances[symbol] = amount
                
        return balances

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, float]:
        """
        Fetch the latest funding rate for a generic symbol.
        """
        target_symbol = symbol
        if "_PERP" not in symbol and "USDC" in symbol:
             target_symbol = f"{symbol}_PERP"

        data = await self._request("GET", "/api/v1/fundingRates", params={"symbol": target_symbol})
        if not data or not isinstance(data, list) or len(data) == 0:
            return {}

        latest = data[0]
        return {
            "symbol": symbol, 
            "funding_rate": float(latest.get("fundingRate", 0)),
            "timestamp": latest.get("intervalEndTimestamp")
        }

    async def fetch_my_trades(self, symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch recent fills.
        Disabled temporarily due to 404 on assumed endpoints (need Research).
        """
        return []
        # try:
        #     target_symbol = symbol.replace("-", "_")
        #     params = {"symbol": target_symbol, "limit": limit}
        #     data = await self._request("GET", "/api/v1/history/fills", params=params, signed=True, instruction="fillHistoryQueryAll")
        # ...

    async def fetch_funding_history(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Fetch funding payments.
        Disabled temporarily due to 404 on assumed endpoints (need Research).
        """
        return []
        # try:
        #     target_symbol = symbol.replace("-", "_")
        #     ...

    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        Fetch open positions.
        Combines Perps (via /api/v1/position) and Spot Balances (fallback).
        """
        positions = []

        # 1. Fetch Perps Positions
        try:
            # Note: Endpoint is singular 'position' for ALL positions
            data = await self._request("GET", "/api/v1/position", signed=True, instruction="positionQuery")
            if data and isinstance(data, list):
                for p in data:
                    # p keys: symbol, netQuantity, entryPrice, pnlUnrealized, ...
                    size = float(p.get("netQuantity", 0))
                    if abs(size) > 0:
                        positions.append({
                            "symbol": p.get("symbol"),
                            "size": size,
                            "entry_price": float(p.get("entryPrice", 0)),
                            "unrealized_pnl": float(p.get("pnlUnrealized", 0))
                        })
        except Exception as e:
            logger.error(f"Error fetching Backpack Perp positions: {e}")
            raise e
        
        # 2. Append Spot Balances
        # Useful if we are holding Spot for Arb
        balances = await self.get_balance()
        for symbol, amount in balances.items():
            if amount > 0.0001: # Filter dust
                # Check if this symbol already exists from Perps (unlikely collision but good safety)
                # Spot symbols usually "SOL", Perps "SOL_USDC_PERP"
                positions.append({
                    "symbol": symbol,
                    "size": amount,
                    "entry_price": 0.0, # Spot doesn't track entry price in balance
                    "unrealized_pnl": 0.0
                })
                
        return positions
