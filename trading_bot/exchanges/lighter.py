import os
import asyncio
import aiohttp
import logging
import time
import json
from typing import Dict, Any, Tuple, List
import hashlib
import binascii
from .base import Exchange
import lighter
import lighter
from lighter import ApiClient, Configuration, OrderApi, AccountApi, CandlestickApi, FundingApi, TransactionApi

from lighter.signer_client import SignerClient

logger = logging.getLogger("trading_bot.exchanges.lighter")

class LighterExchange(Exchange):
    BASE_URL = "https://mainnet.zklighter.elliot.ai"

    def __init__(self, private_key: str, account_index: int = 0, api_key_index: int = 0, config: Dict[str, Any] = None):
        # Lighter doesn't use standard api_key/secret in the same way. 
        # We pass private_key as api_key for base compatibility, generic usage.
        super().__init__(private_key, "") 
        self.name = "Lighter"
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.ex_config = config or {}
        self._nonce_synced = False
        
        self.configuration = Configuration(host=self.BASE_URL)
        self.api_client = ApiClient(self.configuration)
        
        # Initialize SignerClient for order creation
        # Required positional args: url, private_key, api_key_index, account_index
        self.signer_client = SignerClient(
            self.BASE_URL,
            private_key,
            api_key_index,
            account_index
        )
        
        self.market_map = {} # Symbol -> MarketID
        self.id_map = {}     # MarketID -> Symbol
        self.multipliers = {} # MarketID -> {price, size}
        
        # Initialize Multipliers/Markets from Config if present
        if "markets" in self.ex_config:
            for symbol, data in self.ex_config["markets"].items():
                m_id = data.get("id")
                if m_id is not None:
                     self.multipliers[m_id] = {
                         "price": data.get("price_mult", 100),
                         "size": data.get("size_mult", 100)
                     }

    async def close(self):
        await self.api_client.close()

    async def _load_markets(self):
        if self.market_map:
            return

        # Load from Config
        if "markets" in self.ex_config:
             for symbol, data in self.ex_config["markets"].items():
                 m_id = data.get("id")
                 if m_id is not None:
                     self.market_map[symbol] = m_id
        
        # Fallback if config is empty (or mixed usage)
        if not self.market_map:
             logger.warning("No markets configured for Lighter in config.yaml! Using fallbacks.")
             self.market_map = {
                "SOL-USDC": 2, 
                "WBTC-USDC": 1
             }
        
        self.id_map = {v: k for k, v in self.market_map.items()}
        logger.info(f"Lighter Markets loaded: {self.market_map}")

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, float]:
        """
        Fetch the latest funding rate for a symbol.
        Normalizes Lighter's 8-hour rate (fractional) to 1-hour rate for strategy compatibility.
        """
        try:
            # Lighter V1 official endpoint for simplified funding rates
            url = f"{self.BASE_URL}/api/v1/funding-rates"
            
            # Headers to bypass 403 Forbidden
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json"
            }
            
            # Use a one-off session for now to ensure clean request/headers bypass
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"Lighter Funding API Error {resp.status}")
                        return {}
                    
                    data = await resp.json()
                    
                    # Structure: {'funding_rates': [{'exchange': 'lighter', 'symbol': 'BTC', 'rate': ...}, ...]}
                    all_rates = data.get("funding_rates", [])
                    
                    # Strategy passes "SOL-USDC" or "BTC-USDC", Lighter returns "SOL" or "BTC"
                    base_symbol = symbol.split("-")[0].upper()
                    
                    for item in all_rates:
                        if item.get("exchange") == "lighter" and item.get("symbol", "").upper() == base_symbol:
                            # Lighter returns an 8-hour rate as decimal (fractional)
                            # e.g. 0.000096 means 0.0096% every 8 hours.
                            # Bot strategy expects HOURLY rate (rate * 24 * 365 = APR)
                            raw_rate = float(item.get("rate", 0))
                            hourly_rate = raw_rate / 8.0
                            
                            logger.debug(f"Fetched Lighter {base_symbol} Funding: {raw_rate} (8h) -> {hourly_rate} (1h)")
                            
                            return {
                                "symbol": symbol,
                                "funding_rate": hourly_rate,
                                "timestamp": int(time.time() * 1000)
                            }
                        
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching Lighter funding rate: {e}")
            return {}

    async def sync_nonce(self):
        """
        Force a hard refresh of the nonce from the Lighter API for the current API key.
        """
        try:
            # SignerClient initialization already creates a nonce_manager
            self.signer_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
            # Access internal state to verify (optional log)
            new_nonce = self.signer_client.nonce_manager.nonce.get(self.api_key_index)
            logger.info(f"Hard-Refreshed Lighter Nonce for SDK Manager: {new_nonce}")
            self._nonce_synced = True
            return True
        except Exception as e:
            logger.error(f"Failed to hard-refresh Lighter nonce: {e}")
        return False

    async def get_balance(self) -> Dict[str, float]:
        """
        Fetch account balance (USDC only for now).
        """
        try:
            account_api = AccountApi(self.api_client)
            # Account fetch by index
            acc_data = await account_api.account(by="index", value=str(self.account_index))
            
            balances = {"USDC": 0.0}
            if acc_data and acc_data.accounts:
                acc = acc_data.accounts[0]
                balances["USDC"] = float(acc.available_balance)
                
            return balances
        except Exception as e:
            logger.error(f"Error fetching Lighter balance: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error fetching Lighter balance: {e}")
            return {}

    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        Fetch open positions.
        """
        try:
            account_api = AccountApi(self.api_client)
            acc_data = await account_api.account(by="index", value=str(self.account_index))
            
            positions = []
            if acc_data and acc_data.accounts:
                raw_positions = acc_data.accounts[0].positions
                await self._load_markets() # Ensure ID map is loaded
                
                for p in raw_positions:
                    # p has market_id, position (size), avg_price, unrealized_pnl, sign
                    m_id = p.market_id
                    symbol = self.id_map.get(m_id, f"Unknown-{m_id}")
                    
                    # Lighter SDK: 'position' is absolute value, 'sign' is +1 for Long, -1 for Short
                    abs_size = float(p.position)
                    sign = int(p.sign) if hasattr(p, 'sign') else 1
                    size = abs_size * sign
                    
                    if abs(size) > 0:
                        positions.append({
                            "symbol": symbol,
                            "size": size,
                            "entry_price": float(p.avg_entry_price),
                            "unrealized_pnl": float(p.unrealized_pnl)
                        })
                return positions
        except Exception as e:
            logger.error(f"Error fetching Lighter positions: {e}")
            raise e # Strict Mode: Fail if we can't verify

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch ticker (Best Bid/Ask/Last).
        """
        await self._load_markets()
        market_id = self.market_map.get(symbol)
        if market_id is None:
            return {}

        try:
            order_api = OrderApi(self.api_client)
            
            # Fetch BBO via order_book_orders (Limit 5 is enough)
            # This returns an object with .asks and .bids (lists of SimpleOrder)
            book = await order_api.order_book_orders(market_id=market_id, limit=5)
            
            bid = 0.0
            ask = 0.0
            if hasattr(book, 'bids') and book.bids:
                bid = float(book.bids[0].price)
            if hasattr(book, 'asks') and book.asks:
                ask = float(book.asks[0].price)
                
            # Optional: Fetch last price via details if needed, or approximate
            last = 0.0
            if bid > 0 and ask > 0:
                last = (bid + ask) / 2
            elif bid > 0:
                last = bid
            elif ask > 0:
                last = ask
                
            # If we really want exact last price, we need order_book_details, but let's save the call for speed
            # unless BBO is empty.
            
            # Sanity Check for Data Integrity
            # Sanity Check for Data Integrity
            # Lighter IDs can be confusing, so hard check price range to prevent cross-pair contamination
            if last > 0:
                if "SOL" in symbol and not (10 < last < 500):
                     logger.error(f"Integrity Check Failed: {symbol} price {last} is out of bounds (10-500)")
                     return {}
                elif "ETH" in symbol and not (1000 < last < 10000):
                     logger.error(f"Integrity Check Failed: {symbol} price {last} is out of bounds (1000-10000)")
                     return {}
                elif "BTC" in symbol and not (20000 < last < 200000):
                     logger.error(f"Integrity Check Failed: {symbol} price {last} is out of bounds (20000-200000)")
                     return {}

            return {
                "symbol": symbol,
                "last_price": last,
                "bid": bid,
                "ask": ask,
                "timestamp": time.time()
            }
        except Exception as e:
            logger.error(f"Error fetching ticker: {e}")
            return {}

    async def fetch_order_book(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch order book depth.
        """
        await self._load_markets()
        market_id = self.market_map.get(symbol)
        if market_id is None:
            return {}
            
        try:
            order_api = OrderApi(self.api_client)
            # order_book_orders(market_id=...)
            book = await order_api.order_book_orders(market_id=market_id)
            # Transform to standard structure
            # book.asks, book.bids
            
            # Parsing helpers needed for exact Lighter format
            return {
                "symbol": symbol,
                "bids": [], # TODO: Parse
                "asks": [],
                "timestamp": time.time()
            }
        except Exception as e:
            logger.error(f"Error fetching order book: {e}")
            return {}

    async def create_order(self, symbol: str, side: str, order_type: str, price: float, quantity: float) -> Dict[str, Any]:
        """
        Create an order using SignerClient.
        """
        await self._load_markets()
        market_id = self.market_map.get(symbol)
        if market_id is None:
            logger.error(f"Symbol {symbol} not found in map")
            return {}

        # Get Multipliers
        # Defaults based on Market ID if config missing
        default_mults = {"price": 1000, "size": 1000} # SOL default
        if market_id == 1: # BTC
            default_mults = {"price": 10, "size": 100000}
            
        mults = self.multipliers.get(market_id, default_mults)
        price_mult = mults["price"]
        size_mult = mults["size"]

        # Generate Nonce / Client Order Index
        # Pattern from gold-arb: 3.2B + ms_offset + counter
        epoch = 1767312000 
        ms_since_epoch = int((time.time() - epoch) * 1000)
        if ms_since_epoch < 0: ms_since_epoch = int(time.time() % 100000)
        
        # Simple random/time based nonce for now
        client_order_index =  int(time.time() * 1000) & 0xFFFFFFFF
        
        # Params
        is_ask = (side.lower() == 'sell')
        
        # Determine Lighter Order Type
        # SignerClient: ORDER_TYPE_LIMIT = 0, ORDER_TYPE_MARKET = 1
        l_order_type = 0 # Default Limit
        l_tif = 0 # IOC usually? Or GTC?
        # gold-arb uses GoodTillTime (1) for Limits
        
        if order_type.lower() == 'market' or price == 0:
             # Market Order Handling: Convert to LIMIT with Slippage
             # 1. Fetch Ticker
             ticker = await self.fetch_ticker(symbol)
             if not ticker:
                 logger.error(f"Cannot create Market Order: Ticker fetch failed for {symbol}")
                 return {"error": "Ticker Failed"}
             
             # 2. Get Best Price
             best_ask = ticker.get('ask', 0.0)
             best_bid = ticker.get('bid', 0.0)
             
             if is_ask: # SELL -> Sell into BID
                 ref_price = best_bid
                 if ref_price == 0: ref_price = ticker.get('last', 0)
             else: # BUY -> Buy from ASK
                 ref_price = best_ask
                 if ref_price == 0: ref_price = ticker.get('last', 0)
                 
             if ref_price == 0:
                 logger.error(f"Cannot create Market Order: No price data for {symbol}")
                 return {"error": "No Price Data"}

             # 3. Apply Slippage
             slippage = self.ex_config.get("slippage_tolerance", 0.005) # Default 0.5%
             
             if is_ask: # SELL: Price LOWER than Bid (to cross)
                 price = ref_price * (1.0 - slippage) 
             else: # BUY: Price HIGHER than Ask
                 price = ref_price * (1.0 + slippage)
                 
             logger.info(f"Market-as-Limit: Ref={ref_price}, Slip={slippage*100}%, Final={price:.4f}")
             
             # Force Limit Order
             l_order_type = 0 # Limit
             l_tif = 1 # GTC (Good Till Cancel) - Lighter rejected IOC(3)
             # "Market-as-Limit" with aggressive price will fill immediately anyway.
        else:
             l_tif = 1 # GTC (GoodTillTime)
        
        for attempt in range(2): # 1 initial + 1 retry
            try:
                if not self._nonce_synced:
                    await self.sync_nonce()

                logger.info(f"Creating Order (Attempt {attempt+1}): {symbol} {side} {quantity} @ {price}")
                
                # create_order(market_index, client_order_index, base_amount, price, is_ask, order_type, time_in_force, ...)
                # inputs must be scaled INTEGERS
                base_amount_int = int(quantity * size_mult)
                price_int = int(price * price_mult)
                
                # 1. Get Synchronized Nonce from Manager
                # sdk nonce_manager.next_nonce() returns (api_key_index, nonce)
                _, fresh_nonce = self.signer_client.nonce_manager.next_nonce()
                
                # 2. Sign Order
                tx_info, error = self.signer_client.sign_create_order(
                    market_index=market_id,
                    client_order_index=client_order_index,
                    base_amount=base_amount_int,
                    price=price_int,
                    is_ask=is_ask,
                    order_type=l_order_type,
                    time_in_force=l_tif,
                    reduce_only=False,
                    trigger_price=0,
                    order_expiry=-1,
                    nonce=fresh_nonce
                )
                    
                # 3. Send Transaction
                logger.info(f"Sending Signed Transaction to Lighter (Nonce: {fresh_nonce})...")
                api_resp = await self.signer_client.send_tx(
                    tx_type=14, 
                    tx_info=tx_info
                )
                
                # 3. Handle Response manually
                if not api_resp:
                    logger.error("Lighter API returned None (Likely HTTP Error in SDK logs)")
                    return {"error": "API Send Failed (Check Logs)"}
                    
                if hasattr(api_resp, 'code') and api_resp.code != 200:
                    logger.error(f"Lighter API Error Code {api_resp.code}: {getattr(api_resp, 'message', 'Unknown')}")
                    return {"error": f"API Error {api_resp.code}"}

                logger.info("Lighter Order Sent Successfully!")
                return {
                    "id": str(client_order_index),
                    "symbol": symbol,
                    "status": "OPEN",
                    "tx_info": tx_info
                }

            except Exception as e:
                err_msg = str(e)
                if "21104" in err_msg or "invalid nonce" in err_msg.lower():
                    logger.warning(f"Lighter Nonce Error detected on attempt {attempt+1}. Syncing and Retrying...")
                    success = await self.sync_nonce()
                    if success:
                        continue # Retry
                
                logger.error(f"Error creating order: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return {"error": str(e)}

        return {"error": "Max retries exceeded for Lighter order"}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancel order using SignerClient manual bypass.
        order_id: Can be tx_hash or client_order_index? 
        The SDK sign_cancel_order usually expects 'order_index' or 'nonce'? 
        Actually, Lighter usually cancels by ID (OrderIndex) or ClientID?
        
        SDK signature: sign_cancel_order(market_index, order_index, nonce)
        'order_index' is the Exchange Order ID.
        """
        await self._load_markets()
        market_id = self.market_map.get(symbol)
        if market_id is None: return False

        try:
            logger.info(f"Cancelling Order {order_id} on Market {market_id}")
            # Ensure order_id is int (Lighter Order Index)
            # If we don't have the exchange Order Index, we might fail if we only have ClientID?
            # The 'create_order' returned '2318832626' which was ClientOrderIndex.
            # Lighter 'cancel_order' usually requires the Exchange-assigned ID (unless we use CancelAll)
            
            # Note: If order_id is ClientOrderIndex, we might not be able to cancel specific order easily 
            # without fetching OpenOrders to find the Exchange ID.
            # But let's assume order_id passed here IS the Exchange ID?
            
            # However, my create_order returns 'client_order_index' as ID. 
            # I can't cancel by Client ID directly in SDK (sign_cancel_order takes order_index).
            
            # Temporary Workaround: Fetch Open Orders to resolve ClientID -> OrderID?
            # Or just return True to simulate success for verification if we can't do it easily.
            # But for live bot, we need real cancel.
            
            # Let's try to pass it as int
            try:
                oid = int(order_id)
            except:
                logger.error(f"Invalid Order ID format: {order_id}")
                return False

            # Get Synchronized Nonce
            _, fresh_nonce = self.signer_client.nonce_manager.next_nonce()

            tx_info, error = self.signer_client.sign_cancel_order(
                market_index=market_id,
                order_index=oid,
                nonce=fresh_nonce
            )
            
            if error:
                logger.error(f"Sign Cancel Error: {error}")
                self.signer_client.nonce_manager.acknowledge_failure(self.api_key_index)
                return False
                
            api_resp = await self.signer_client.send_tx(tx_type=15, tx_info=tx_info)
            
            if not api_resp:
                logger.error("Cancel API returned None")
                return False
                
            logger.info("Order Cancel Sent!")
            return True

        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return False

    async def get_open_orders(self, symbol: str = None) -> list:
        """
        Fetch open orders.
        """
        try:
            order_api = OrderApi(self.api_client)
            # requires auth token?
            return []
        except Exception:
            return []

    async def cancel_all_orders(self, symbol: str) -> bool:
        return False

    async def get_pending_order_count(self) -> int:
        """
        Fetch number of pending orders.
        """
        try:
            from lighter import AccountApi
            acc_api = AccountApi(self.api_client)
            acc_data = await acc_api.account(by="index", value=str(self.account_index))
            if acc_data and acc_data.accounts:
                return acc_data.accounts[0].pending_order_count
            return 0
        except Exception as e:
            logger.error(f"Error fetching pending count: {e}")
            return 0

    async def fetch_my_trades(self, symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch recent trades/fills.
        Disabled: Requires Auth config on ApiClient (Pending implementation).
        """
        return []

    async def fetch_funding_history(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Fetch funding payments.
        Disabled: Requires Auth config on ApiClient (Pending implementation).
        """
        return []
