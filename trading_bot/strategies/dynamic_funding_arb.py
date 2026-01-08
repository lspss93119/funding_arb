import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from .base_strategy import Strategy
from trading_bot.data.db import DatabaseManager
from trading_bot.exchanges.base import Exchange

logger = logging.getLogger("trading_bot.strategies.dynamic_funding_arb")

class DynamicFundingArbitrageStrategy(Strategy):
    def __init__(self, exchanges: Dict[str, Exchange], config: Dict):
        """
        Args:
            exchanges: Pool of all available exchanges (e.g. {'lighter': lighter_ex, ...})
            config: Strategy configuration
        """
        self.pair = config.get("pair", "SOL-USDC")
        # Pass the first exchange as base, but we will handle swapping
        first_ex = list(exchanges.values())[0] if exchanges else None
        super().__init__(first_ex, config)
        
        self.all_exchanges = exchanges
        self.available_ex_names = config.get("available_exchanges", ["lighter", "backpack", "edgex"])
        
        # Parameters - Ensure these are read correctly
        self.entry_threshold_apr = config.get("entry_threshold_apr", 0.05)
        self.exit_threshold_apr = config.get("exit_threshold_apr", 0.0)
        self.order_size_usd = config.get("order_size_usd", 100)
        self.max_position_size_usd = config.get("max_position_size_usd", 100.0) # Default to 100
        self.is_simulation = config.get("is_simulation", True)
        self.execution_window_minutes = config.get("execution_window_minutes", 5)
        
        logger.info(f"[{self.pair}] Strategy Param Check: MaxPos={self.max_position_size_usd} Entry={self.entry_threshold_apr} Exit={self.exit_threshold_apr} Simulation={self.is_simulation} Window={self.execution_window_minutes}m")
        
        # State tracking
        self.current_pair: Optional[Tuple[str, str]] = None # (Short Exchange Name, Long Exchange Name)
        self.current_position_size = 0.0 # Asset Qty
        self.has_unbalanced_position = False
        self.is_checking = False
        
        # Registry mapping standard pair to exchange-specific symbols
        self.exchange_symbol_maps = {} 
        for name in self.available_ex_names:
            if name in self.all_exchanges:
                ex_cfg = config.get("exchange_configs", {}).get(name, {})
                s_map = ex_cfg.get("symbol_map", {})
                self.exchange_symbol_maps[name] = s_map

        # Callback for UI
        self.on_state_update = config.get("on_state_update")
        
        self.db = DatabaseManager()
        self.realized_pnl = self.db.get_total_pnl(self.name)
        self.last_execution_time = 0
        self.execution_cooldown = 60
        self.is_running = False

    @property
    def name(self) -> str:
        return f"DynamicArb_{self.pair}"

    async def on_start(self):
        self.is_running = True
        logger.info(f"Dynamic Funding Arb Strategy ({self.pair}) Started.")
        # Sync initial positions across all exchanges
        await self.sync_state_from_exchanges()

    async def on_stop(self):
        self.is_running = False
        logger.info("Dynamic Funding Arb Strategy Stopped.")

    async def on_tick(self, market_data: Dict[str, Any]):
        pass

    def get_ex_symbol(self, ex_name: str) -> str:
        s_map = self.exchange_symbol_maps.get(ex_name, {})
        return s_map.get(self.pair, self.pair)

    async def sync_state_from_exchanges(self):
        """
        Check all exchanges for existing positions in the target pair.
        If multiple positions found, we might be in an inconsistent state or intentional multi-leg.
        For now, we look for the largest Short and largest Long.
        """
        best_short_ex = None
        max_short_size = 0.0
        best_long_ex = None
        max_long_size = 0.0

        tasks = []
        enabled_ex = [name for name in self.available_ex_names if name in self.all_exchanges]
        for name in enabled_ex:
            tasks.append(self.all_exchanges[name].get_positions())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error(f"Failed to sync positions from {enabled_ex[i]}: {res}")
                continue
            
            ex_name = enabled_ex[i]
            ex_symbol = self.get_ex_symbol(ex_name)
            found_symbols = [p.get('symbol') for p in res]
            logger.info(f"[{self.pair}] Syncing {ex_name}: Target={ex_symbol}, Found Assets={found_symbols}")
            
            for pos in res:
                size = float(pos.get("size", 0))
                p_sym = pos.get("symbol")
                if p_sym == ex_symbol:
                    logger.info(f"[{self.pair}] Found Matching Position on {ex_name}: {size} {p_sym}")
                    if size < -0.000001: # Short
                        if abs(size) > max_short_size:
                            max_short_size = abs(size)
                            best_short_ex = ex_name
                    elif size > 0.000001: # Long
                        if size > max_long_size:
                            max_long_size = size
                            best_long_ex = ex_name
                else:
                    # Log or ignore other symbol positions
                    if abs(size) > 0.0001:
                        logger.debug(f"Ignoring non-target position on {ex_name}: {size} {p_sym}")
        
        self.has_unbalanced_position = False
        if best_short_ex and best_long_ex:
            self.current_pair = (best_short_ex, best_long_ex)
            self.current_position_size = min(max_short_size, max_long_size)
            logger.info(f"State Restored: Short {best_short_ex} / Long {best_long_ex} | Size: {self.current_position_size}")
            
            # Check for imbalance
            if abs(max_short_size - max_long_size) > self.current_position_size * 0.1: # 10% tolerance
                logger.warning(f"Position Imbalance Detected: Short {max_short_size} / Long {max_long_size}")
        elif best_short_ex or best_long_ex:
            self.current_pair = None
            self.current_position_size = 0.0
            self.has_unbalanced_position = True
            logger.error(f"⚠️ UNBALANCED POSITION DETECTED: Short={best_short_ex}({max_short_size}), Long={best_long_ex}({max_long_size}). New entries disabled.")
        else:
            self.current_pair = None
            self.current_position_size = 0.0
            logger.info("State Restored: No active cross-exchange position.")

    async def check_opportunity(self):
        if not self.is_running: return
        if getattr(self, "is_checking", False):
            logger.debug("Check already in progress, skipping.")
            return
            
        self.is_checking = True
        try:
            # 1. Fetch rates from all exchanges
            enabled_ex = [name for name in self.available_ex_names if name in self.all_exchanges]
            rate_tasks = []
            for name in enabled_ex:
                rate_tasks.append(self.all_exchanges[name].fetch_funding_rate(self.get_ex_symbol(name)))
            
            rate_results = await asyncio.gather(*rate_tasks, return_exceptions=True)
            ex_rates = {} # name -> hourly_rate
            for i, res in enumerate(rate_results):
                if not isinstance(res, Exception) and res:
                    ex_rates[enabled_ex[i]] = float(res.get("funding_rate", 0))
            
            if not ex_rates:
                logger.warning("No funding rates fetched.")
                return

            # Convert to APR for easier comparison
            ex_apr = {name: rate * 24 * 365 for name, rate in ex_rates.items()}
            
            # 2. Find best spread
            best_spread = -1.0
            best_pair = None # (Short Ex, Long Ex)

            for ex1 in ex_apr: # Potential Short
                for ex2 in ex_apr: # Potential Long
                    if ex1 == ex2: continue
                    spread = ex_apr[ex1] - ex_apr[ex2]
                    if spread > best_spread:
                        best_spread = spread
                        best_pair = (ex1, ex2)

            # 3. UI Update
            ticker_ex = self.all_exchanges.get(enabled_ex[0])
            ticker = await ticker_ex.fetch_ticker(self.get_ex_symbol(enabled_ex[0]))
            # Handle different exchange ticker formats
            price = ticker.get("last_price", 0) or ticker.get("last", 0) or ticker.get("lastPrice", 0)
            
            if price == 0:
                logger.warning(f"[{self.pair}] Price fetch returned 0. Ticker data: {ticker}")

            if self.on_state_update:
                status = "Monitoring"
                if self.current_pair:
                    status = f"Arb: {self.current_pair[0]}->{self.current_pair[1]}"
                
                # Show top rates in status
                top_rate_info = " | ".join([f"{n}: {ex_apr[n]*100:.1f}%" for n in ex_apr])

                self.on_state_update({
                    "symbol": self.pair,
                    "price": price,
                    "spread": best_spread * 100,
                    "rate_lighter": ex_apr.get("lighter", 0) * 100,
                    "rate_backpack": ex_apr.get("backpack", 0) * 100,
                    "rate_edgex": ex_apr.get("edgex", 0) * 100,
                    "status": status,
                    "position_size": self.current_position_size,
                    "max_position": f"${self.max_position_size_usd}"
                })

            # 4. Entry/Exit Logic
            if self.current_pair:
                # We have a position. Monitor for exit.
                # Current pair spread: APR(Short Ex) - APR(Long Ex)
                s_ex, l_ex = self.current_pair
                current_spread = ex_apr.get(s_ex, 0) - ex_apr.get(l_ex, 0)
                
                if current_spread < self.exit_threshold_apr:
                    logger.info(f">>> EXIT SIGNAL: Spread {current_spread*100:.2f}% < {self.exit_threshold_apr*100:.2f}%")
                    await self.execute_exit()
            elif not self.has_unbalanced_position:
                # No position. Monitor for entry.
                if best_spread > self.entry_threshold_apr:
                    # Check Execution Window
                    now = datetime.now()
                    minutes_from_hour = now.minute if now.minute <= 30 else 60 - now.minute
                    
                    if minutes_from_hour <= self.execution_window_minutes:
                        logger.info(f"[{self.pair}] 🚀 OPPORTUNITY FOUND: {best_pair[0]} (Short) vs {best_pair[1]} (Long) | Spread: {best_spread*100:.2f}% | Within Window: {now.strftime('%H:%M:%S')}")
                        await self.execute_entry(best_pair, price)
                    else:
                        logger.info(f"[{self.pair}] Opportunity found ({best_spread*100:.2f}%) but outside execution window ({now.minute}m). Next window starts at :55")

        except Exception as e:
            logger.error(f"Error in check_opportunity: {e}", exc_info=True)
        finally:
            self.is_checking = False

    async def execute_entry(self, pair: Tuple[str, str], price: float):
        s_ex_name, l_ex_name = pair
        s_ex = self.all_exchanges[s_ex_name]
        l_ex = self.all_exchanges[l_ex_name]
        
        qty = self.order_size_usd / price if price > 0 else 0
        if qty <= 0: return

        logger.info(f"[{self.pair}] Executing Entry: Short {s_ex_name} / Long {l_ex_name} | Qty: {qty}")
        
        if self.is_simulation:
            self.current_pair = pair
            self.current_position_size = qty
            logger.info("✅ Simulation Entry Successful.")
            return

        tasks = [
            s_ex.create_order(self.get_ex_symbol(s_ex_name), "SELL", "MARKET", 0, qty),
            l_ex.create_order(self.get_ex_symbol(l_ex_name), "BUY", "MARKET", 0, qty)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Results can be [OrderDict, {'error': '...'}] or [Exception, OrderDict]
        logger.info(f"[{self.pair}] Entry Results: {results}")
        
        # Robust success check: Ensure all results are dicts AND none have 'error' key
        success = True
        for r in results:
            if isinstance(r, Exception):
                success = False
                break
            if isinstance(r, dict) and r.get("error"):
                success = False
                break
            if r is None: # Some exchanges might return None on unexpected failure
                success = False
                break
        
        if success:
             self.current_pair = pair
             self.current_position_size = qty
             self.has_unbalanced_position = False
             logger.info(f"[{self.pair}] ✅ Entry Successful.")
        else:
             self.has_unbalanced_position = True
             logger.error(f"[{self.pair}] ❌ Partial or Failed Entry. Manual intervention might be needed. Retries disabled until sync.")

    async def execute_exit(self):
        if not self.current_pair: return
        
        s_ex_name, l_ex_name = self.current_pair
        s_ex = self.all_exchanges[s_ex_name]
        l_ex = self.all_exchanges[l_ex_name]
        qty = self.current_position_size

        logger.info(f"[{self.pair}] Executing Exit: Close {s_ex_name} (Short) and {l_ex_name} (Long) | Qty: {qty}")

        if self.is_simulation:
            self.current_pair = None
            self.current_position_size = 0.0
            logger.info("✅ Simulation Exit Successful.")
            return

        tasks = [
            s_ex.create_order(self.get_ex_symbol(s_ex_name), "BUY", "MARKET", 0, qty),
            l_ex.create_order(self.get_ex_symbol(l_ex_name), "SELL", "MARKET", 0, qty)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"[{self.pair}] Exit Results: {results}")
        
        # Robust success check
        success = True
        for r in results:
            if isinstance(r, Exception):
                success = False
                break
            if isinstance(r, dict) and r.get("error"):
                success = False
                break
            if r is None:
                success = False
                break

        if success:
             self.current_pair = None
             self.current_position_size = 0.0
             logger.info(f"[{self.pair}] ✅ Exit Successful.")
        else:
             logger.error(f"[{self.pair}] ❌ Partial or Failed Exit. Manual intervention might be needed.")
