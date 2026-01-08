import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional
from .base_strategy import Strategy
from trading_bot.config.settings import config
from trading_bot.data.db import DatabaseManager
from trading_bot.exchanges.base import Exchange

logger = logging.getLogger("trading_bot.strategies.funding_arb")

class FundingArbitrageStrategy(Strategy):
    def __init__(self, exchanges: Dict[str, Exchange], config: Dict):
        """
        Args:
            exchanges: Dict with 'primary' (Perp) and optional 'secondary'
            config: Strategy configuration
        """
        super().__init__(exchanges.get('primary'), config)
        
        self.exchanges = exchanges
        self.primary = exchanges.get('primary')
        self.secondary = exchanges.get('secondary') # Can be None
        
        # Symbols
        self.symbol_primary = config.get("symbol_primary", "SOL-USDC")
        self.symbol_secondary = config.get("symbol_secondary", None)
        
        # Parameters
        self.min_apr = config.get("min_apr", 0.10)
        self.entry_threshold = config.get("entry_threshold", 0.01)
        self.exit_threshold = config.get("exit_threshold", 0.005)
        self.position_size = config.get("position_size", 0.0) # Fixed Asset Qty
        self.order_size_usd = config.get("order_size_usd") # Dynamic USD Size
        self.is_simulation = config.get("is_simulation", True)
        self.is_simulation = config.get("is_simulation", True)
        self.max_position_size = config.get("max_position_size", 0.0)
        self.max_position_size_usd = config.get("max_position_size_usd")
        self.auto_revert = config.get("auto_revert", False)
        
        # Callback for UI
        self.on_state_update = config.get("on_state_update")
        
        self.current_position = None
        self.current_position_size = 0.0
        self.is_running = False
        self.is_running = False
        self.is_running = False
        self.last_error_time = 0
        self.error_backoff = 10 # Initial backoff seconds
        
        # Database
        self.db = DatabaseManager()
        
        # PnL Tracking (Load from DB)
        self.realized_pnl = self.db.get_total_pnl(self.name)
        
        self.p_entry_price = 0.0
        self.s_entry_price = 0.0
        self.entry_time = None
        self.last_sync_time = 0 # For periodic history sync
        self.quarantine_mode = False # Emergency block on new trades
        self.quarantine_reason = ""
        self.last_execution_time = 0 # Unix TS of last entry/exit
        self.execution_cooldown = 120 # 2 minutes minimum between major state changes

    @property
    def name(self) -> str:
        return "FundingArb_Generic"

    async def on_start(self):
        self.is_running = True
        mode_str = "SIMULATION" if self.is_simulation else "LIVE"
        sec_name = "None" if not self.secondary else f"Secondary ({self.symbol_secondary})"
        logger.info(f"Funding Arb Strategy ({mode_str}) Started.")
        logger.info(f"Primary (Perp): {self.symbol_primary} | {sec_name}")
        
        if self.on_state_update:
            self.on_state_update({
                "status": "Starting...",
                "spread": 0,
                "rate_primary": 0,
                "rate_secondary": 0,
                "max_position": self.max_position_size
            })

        # --- Position Sync ---
        # Initialize internal state from actual exchange positions
        # This prevents opening new positions if already at limit after restart
        try:
            logger.info("Syncing positions from exchange...")
            real_positions = await self.verify_positions()
            real_pos_p = real_positions.get("real_pos_primary", 0.0)
            real_pos_s = real_positions.get("real_pos_secondary", 0.0)
            
            # Update Size (use MAX of both exchanges to be safe against API failures)
            # If one exchange says 0 but the other has position, we assume we HAVE position.
            size_p = abs(real_pos_p)
            size_s = abs(real_pos_s)
            self.current_position_size = max(size_p, size_s)
            
            # Determine Direction
            # Logic: If Primary is Short (<0) or Secondary is Long (>0) -> SHORT_P_LONG_S
            #        If Primary is Long (>0) or Secondary is Short (<0) -> LONG_P_SHORT_S
            
            # Heuristic: Check significant position (> 0.0001)
            is_p_short = real_pos_p < -0.000001
            is_p_long = real_pos_p > 0.000001
            is_s_long = real_pos_s > 0.000001
            is_s_short = real_pos_s < -0.000001
            
            if is_p_short or is_s_long: 
                 self.current_position = "SHORT_PRIMARY_LONG_SECONDARY"
                 logger.info(f"State Restored: {self.current_position} | Size: {self.current_position_size} (P:{real_pos_p} S:{real_pos_s})")
            elif is_p_long or is_s_short:
                 self.current_position = "LONG_PRIMARY_SHORT_SECONDARY"
                 logger.info(f"State Restored: {self.current_position} | Size: {self.current_position_size} (P:{real_pos_p} S:{real_pos_s})")
            else:
                 self.current_position = None
                 logger.info("State Restored: No Position")
                 
        except Exception as e:
            logger.error(f"Failed to sync positions on start: {e}")
            raise e # Strict Mode: Don't start bot if we can't verify balance/positions
        
    async def on_stop(self):
        self.is_running = False
        logger.info("Funding Arb Strategy Stopped.")

    async def on_tick(self, tick_data: Dict):
        pass

    def _calculate_pnl(self, position_type, entry_p, entry_s, exit_p, exit_s):
        """
        Calculate realized PnL for a closed position.
        """
        if not position_type or not self.current_position_size:
            return 0.0
        
        # PnL = (PriceDiff) * Size
        if position_type == "SHORT_PRIMARY_LONG_SECONDARY":
            pnl_p = (entry_p - exit_p) * self.current_position_size
            pnl_s = (exit_s - entry_s) * self.current_position_size if exit_s > 0 and entry_s > 0 else 0
            return pnl_p + pnl_s
            
        elif position_type == "LONG_PRIMARY_SHORT_SECONDARY":
            pnl_p = (exit_p - entry_p) * self.current_position_size
            pnl_s = (entry_s - exit_s) * self.current_position_size if exit_s > 0 and entry_s > 0 else 0
            return pnl_p + pnl_s
            
        return 0.0

    async def fetch_all_funding_rates(self):
        """
        Fetch rates from both Primary and Secondary exchanges.
        """
        tasks = []
        tasks.append(self.primary.fetch_funding_rate(self.symbol_primary))
        
        if self.secondary:
            tasks.append(self.secondary.fetch_funding_rate(self.symbol_secondary))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process Primary
        res_a = results[0]
        rate_primary = 0.0
        if isinstance(res_a, dict):
            rate_primary = float(res_a.get("funding_rate", 0.0))
        else:
            logger.error(f"Error fetching Primary rate: {res_a}")

        # Process Secondary
        rate_secondary = 0.0
        if self.secondary and len(results) > 1:
            res_b = results[1]
            if isinstance(res_b, dict):
                rate_secondary = float(res_b.get("funding_rate", 0.0))
            else:
                 logger.error(f"Error fetching Secondary rate: {res_b}")

        return {
            "primary": rate_primary,
            "secondary": rate_secondary
        }

    def _validate_price_sanity(self, symbol: str, price: float) -> bool:
        """
        Hard constraint check to prevent Data Crossover (e.g. ETH strategies reading SOL price).
        """
        try:
            # Normalized ranges
            if "SOL" in symbol:
                if not (10 < price < 500): return False
            elif "ETH" in symbol:
                if not (1000 < price < 10000): return False
            elif "BTC" in symbol:
                if not (20000 < price < 200000): return False
            return True
        except:
            return True # Open fail if regex fails? Better safe than sorry?
            # Actually default to True if symbol not recognized? 
            # Safe Default: True for unknown symbols, strict for known major pairs
            return True

    async def check_opportunity(self):
        if not self.is_running:
            return

        try:
            # 0. Sync History periodically
            await self._sync_history()

            # 0. Cooldown Check (Prevent Spamming after error)
            import time
            if time.time() - self.last_error_time < self.error_backoff: 
                 remaining = int(self.error_backoff - (time.time() - self.last_error_time))
                 if self.on_state_update:
                     self.on_state_update({"status": f"Cooldown ({remaining}s)"})
                 return

            # 0. Check Quarantine
            if self.quarantine_mode:
                if self.on_state_update:
                    self.on_state_update({"status": "QUARANTINED", "reason": self.quarantine_reason})
                return

            # 1. Fetch Rates (Do this ALWAYS to monitor spread)
            rates = await self.fetch_all_funding_rates()
            r_primary = rates["primary"]
            r_secondary = rates["secondary"]
            
            # 1.5 Fetch Price (Used for Sizing, Max Pos Check, and UI Display)
            ticker = await self.primary.fetch_ticker(self.symbol_primary)
            price = ticker.get("last_price", 0)
            
            # 2. Calculate Yields (APR)
            apr_primary = r_primary * 24 * 365
            apr_secondary = r_secondary * 24 * 365
            
            # 3. Calculate Spread
            spread_apr = apr_primary - apr_secondary
            
            # Reduce Log Clutter: Rates are shown in TUI, no need to log every cycle
            # logger.info(f"Rates (APR): P={apr_primary*100:.2f}% | S={apr_secondary*100:.2f}% | Spread={spread_apr*100:.2f}%")
            
            # 3b. Verify Positions (Strict Mode)
            try:
                real_positions = await self.verify_positions()
            except Exception as e:
                # If verification fails, we STALL. We do NOT update state to 0.
                logger.error(f"Failed to verify positions: {e}. Stalling strategy state.")
                if self.on_state_update:
                    self.on_state_update({"status": "SYNC_ERROR"})
                return
            
            # --- Runtime State Self-Healing ---
            # Continuous synchronization of internal state with external reality
            # Extracts the max position size from either exchange to be safe
            real_pos_p = real_positions.get("real_pos_primary", 0.0)
            real_pos_s = real_positions.get("real_pos_secondary", 0.0)

            # --- Time Window Check ---
            # Default to 5 minutes if not set, user set 1 minute
            window_min = self.config.get("execution_window_minutes", 5) 
            now = datetime.now()
            current_min = now.minute
            
            # Allow if within +/- window_min of the hour (e.g. 59, 00, 01 for window=1)
            # 0..window OR 60-window..59
            is_in_window = (current_min < window_min) or (current_min >= 60 - window_min)
            
            # Update Status if waiting
            if not is_in_window:
                if self.on_state_update:
                     self.on_state_update({ "status": "Waiting (Window)" })
            
            size_p = abs(real_pos_p)
            size_s = abs(real_pos_s)
            
            # Update internal size (using max logic)
            self.current_position_size = max(size_p, size_s)
            
            # Update internal direction state
            # Heuristic: Check significant position (> 0.0001)
            is_p_short = real_pos_p < -0.000001
            is_p_long = real_pos_p > 0.000001
            is_s_long = real_pos_s > 0.000001
            is_s_short = real_pos_s < -0.000001
            
            if is_p_short or is_s_long: 
                 self.current_position = "SHORT_PRIMARY_LONG_SECONDARY"
            elif is_p_long or is_s_short:
                 self.current_position = "LONG_PRIMARY_SHORT_SECONDARY"
            else:
                 self.current_position = None
            
            # logger.info(f"Runtime Sync: {self.current_position} | Size: {self.current_position_size:.4f}")
            
            # --- UI Update ---
            if self.on_state_update:
                # Determine display for Max Pos
                max_pos_display = self.max_position_size
                pos_display = None
                
                if self.max_position_size_usd:
                    max_pos_display = f"${self.max_position_size_usd:.0f}"
                    # Calculate estimated USD value of current position
                    if price > 0:
                        est_usd = self.current_position_size * price
                        pos_display = f"${est_usd:.2f}"
                        # logger.info(f"UI POS DEBUG: Size={self.current_position_size} Price={price} EstUSD={est_usd}")

                # Map specific exchange rates for the new dashboard layout
                p_name = self.primary.name.lower()
                s_name = self.secondary.name.lower() if self.secondary else None
                
                rates_data = {
                    f"rate_{p_name}": apr_primary * 100,
                }
                if s_name:
                    rates_data[f"rate_{s_name}"] = apr_secondary * 100

                self.on_state_update({
                    "symbol": self.symbol_primary, # Identity
                    "price": price, # For UI valuation
                    "spread": spread_apr * 100,
                    **rates_data,
                    "position_size": self.current_position_size,
                    "position_display": pos_display,
                    "max_position": max_pos_display,
                    "real_pos_primary": real_positions.get("real_pos_primary", 0),
                    "real_pos_secondary": real_positions.get("real_pos_secondary", 0),
                    "realized_pnl": self.realized_pnl, # New Field
                    "status": "Scanning" if not self.current_position else "Monitoring Exit"
                })

            # --- EXIT LOGIC ---
            if self.current_position and is_in_window: # Apply Window Restriction to Exit too
                # Case 1: Holding SHORT_P / LONG_S (Entered at High Positive Spread)
                # Exit when spread drops below 0 (Rate Reversal)
                if self.current_position == "SHORT_PRIMARY_LONG_SECONDARY":
                    if spread_apr < 0: # Rate Reversal
                        logger.info(f">>> EXIT signal: Net Rate {spread_apr*100:.2f}% < 0% (Reversal)")
                        # To Close: BUY Primary, SELL Secondary
                        await self.execute_position_exit("SHORT_PRIMARY_LONG_SECONDARY", real_positions)
                    # else:
                        # logger.info(f"Holding Position (Net Rate {spread_apr*100:.2f}% > 0%)")
                        
                # Case 2: Holding LONG_P / SHORT_S (Entered at Low Negative Spread)
                # Exit when spread rises above 0 (Rate Reversal)
                elif self.current_position == "LONG_PRIMARY_SHORT_SECONDARY":
                    if spread_apr > 0: # Rate Reversal
                        logger.info(f">>> EXIT signal: Net Rate {spread_apr*100:.2f}% > 0% (Reversal)")
                        # To Close: SELL Primary, BUY Secondary
                        await self.execute_position_exit("LONG_PRIMARY_SHORT_SECONDARY", real_positions)
                    # else:
                        # logger.info(f"Holding Position (Net Rate {spread_apr*100:.2f}% < 0%)")
                
                return # Stop here if holding position (don't check entry)
            elif self.current_position:
                 return # Holding but outside window

            # --- ENTRY LOGIC ---
            
            if not is_in_window:
                return # Already handled status update above
            
            # 1. Pending Order Check (Safety Gate)
            # If we have open orders on Primary, DO NOT initiate new entries.
            # This prevents "Stacking" of limit orders if API is slow or orders don't fill immediately.
            pending = real_positions.get("pending_orders_primary", 0)
            if pending > 0:
                logger.warning(f"Pending Orders Detected ({pending}). Skipping entry to prevent stacking.")
                if self.on_state_update:
                    self.on_state_update({"status": f"Pending Orders ({pending})"})
                return

            trade_qty = self.position_size # Default to fixed
            
            # 4. Dynamic Sizing Logic
            if self.order_size_usd:
                 # Use cached price from Step 1.5
                 if price > 0:
                     # --- SANITY CHECK ---
                     if not self._validate_price_sanity(self.symbol_primary, price):
                         logger.critical(f"🛑 HALT: Insane Price {price} for {self.symbol_primary}. Possible Data Cross-Contamination.")
                         if self.on_state_update: self.on_state_update({"status": "Price Error"})
                         self.error_backoff = 300 # Max backoff immediately
                         return

                     
                     # --- COMMON STEP SIZE LOGIC ---
                     # Align Lighter precision to Backpack's coarser precision (e.g. 0.01 SOL vs 0.001 SOL)
                     # Default coarse step
                     common_step = 0.0001
                     
                     # Try to fetch from Lighter (1/size_mult)
                     l_step = 0
                     if hasattr(self.primary, 'multipliers'):
                         # Need to find market ID for symbol
                         m_id = getattr(self.primary, 'market_map', {}).get(self.symbol_primary)
                         if m_id is not None and m_id in self.primary.multipliers:
                             size_mult = self.primary.multipliers[m_id].get("size", 100)
                             l_step = 1.0 / size_mult
                             
                     # Try to fetch from Backpack (markets stepSize)
                     b_step = 0
                     if hasattr(self.secondary, 'markets'):
                         # Use target symbol logic from verify_positions
                         target_perp = self.symbol_secondary.replace("-", "_")
                         b_meta = self.secondary.markets.get(target_perp)
                         if b_meta:
                             b_step = b_meta.get("stepSize", 0)
                             
                     # Take the LARGER step size (coarser precision)
                     # e.g. max(0.001, 0.01) = 0.01
                     if l_step > 0 or b_step > 0:
                         common_step = max(l_step, b_step)
                         
                     # logger.info(f"Step Size Calc: L={l_step}, B={b_step} => Common={common_step}")

                     # Calculate size = USD / Price
                     raw_size = self.order_size_usd / price
                     
                     # Truncate to Common Step
                     import math
                     if common_step >= 1:
                         precision = 0
                     else:
                         precision = int(abs(math.log10(common_step)))
                         
                     # Floor truncation
                     calc_size = int(raw_size / common_step) * common_step
                     calc_size = round(calc_size, precision)
                     
                     trade_qty = calc_size
                     # logger.info(f"Dynamic Sizing: Qty={trade_qty} (Step={common_step})")
                 else:
                     logger.error("Failed to fetch price for dynamic sizing. strict mode: skipping.")
                     return

            if trade_qty <= 0:
                logger.warning("Trade Quantity is 0. Check config (order_size_usd or order_size_sol).")
                return

            # Determine Max Position Limit (Dynamic or Fixed)
            effective_max_pos = self.max_position_size
            if self.max_position_size_usd:
                 # Use cached price from Step 1.5
                 if price > 0:
                     effective_max_pos = self.max_position_size_usd / price
                     # logger.info(f"Dynamic Max Pos: {self.max_position_size_usd} USD / {price} = {effective_max_pos}")
                 else:
                     logger.warning("Could not fetch price for Max Pos calc. Using fallback/fixed limit.")

            if self.current_position_size + trade_qty > effective_max_pos:
                 # Logic to allow scaling could go here, but for now we limit
                 msg = f"Max Position Limit Reached: Current={self.current_position_size:.4f} + Trade={trade_qty:.4f} > Max={effective_max_pos:.4f}"
                 logger.info(msg) 
                 if self.on_state_update:
                     self.on_state_update({"status": "Max Pos Reached"})
                 return

            # 5. Check Threshold
            threshold = self.entry_threshold
            
            if spread_apr > threshold:
                logger.info(f"🚀 [OPPORTUNITY] {self.name}: Spread {spread_apr*100:.2f}% > {threshold*100:.2f}% (Short P / Long S)")
                await self.execute_dual_leg_entry("SHORT_PRIMARY_LONG_SECONDARY", trade_qty)
            elif spread_apr < -threshold:
                logger.info(f"🚀 [OPPORTUNITY] {self.name}: Spread {spread_apr*100:.2f}% < -{threshold*100:.2f}% (Long P / Short S)")
                await self.execute_dual_leg_entry("LONG_PRIMARY_SHORT_SECONDARY", trade_qty)
            else:
                pass 
            
            # Reset Backoff on successful cycle (if we got this far without error)
            self.error_backoff = 10 

        except Exception as e:
            err_str = str(e)
            is_rate_limit = "429" in err_str or "Too Many Requests" in err_str or "429" in getattr(e, 'message', '')
            
            if is_rate_limit:
                 self.last_error_time = time.time()
                 logger.warning(f"API Rate Limit Triggered (429). Backing off for {self.error_backoff}s.")
                 # Exponential Backoff
                 self.error_backoff = min(self.error_backoff * 2, 300) # Max 5 mins
            else:
                 # Standard Error Handling
                 logger.error(f"Error in check_opportunity: {e}")
                 # Fixed short cooldown for other errors
                 self.last_error_time = time.time()
                 self.error_backoff = 10
            
            if self.on_state_update:
                self.on_state_update({"status": "Error (Wait)"})

    async def _sync_history(self):
        """
        Sync trade history and funding payments to DB.
        """
        if self.is_simulation: return
        
        try:
            # Sync Interval: 5 minutes
            if time.time() - self.last_sync_time < 300:
                return

            logger.debug("🔄 Syncing History & Funding Data...")
            
            # 1. Primary Exchange
            p_trades = await self.primary.fetch_my_trades(self.symbol_primary, limit=50) 
            if p_trades:
                for t in p_trades: t['strategy'] = self.name
                self.db.record_fills(p_trades)
                
            p_funding = await self.primary.fetch_funding_history(self.symbol_primary, limit=50)
            if p_funding:
                for f in p_funding: f['strategy'] = self.name
                self.db.record_funding(p_funding)
                
            # 2. Secondary Exchange
            if self.secondary:
                s_trades = await self.secondary.fetch_my_trades(self.symbol_secondary, limit=50)
                if s_trades:
                    for t in s_trades: t['strategy'] = self.name
                    self.db.record_fills(s_trades)
                
                s_funding = await self.secondary.fetch_funding_history(self.symbol_secondary, limit=50)
                if s_funding:
                    for f in s_funding: f['strategy'] = self.name
                    self.db.record_funding(s_funding)
            
            self.last_sync_time = time.time()
            # logger.info("✅ History Sync Complete.")
            
        except Exception as e:
            logger.error(f"History Sync Error: {e}")

    async def verify_positions(self) -> Dict[str, float]:
        """
        Verify actual positions on exchanges.
        """
        try:
            # Task A: Primary
            # Task A: Primary
            tasks = [self.primary.get_positions()]
            # Task B: Secondary
            if self.secondary:
                tasks.append(self.secondary.get_positions())
                
            # Task C: Primary Pending Count (Only if Lighter)
            if hasattr(self.primary, 'get_pending_order_count'):
                tasks.append(self.primary.get_pending_order_count())
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Strict Check: If any task failed, we MUST abort to prevent state desync
            if isinstance(results[0], Exception):
                raise results[0] # Primary failed
            pos_primary_list = results[0]

            idx_sec = 1 if self.secondary else -1
            if self.secondary:
                if isinstance(results[idx_sec], Exception):
                     raise results[idx_sec] # Secondary failed
                pos_secondary_list = results[idx_sec]
            else:
                pos_secondary_list = []
                
            idx_pending = -1
            if hasattr(self.primary, 'get_pending_order_count'):
                idx_pending = len(results) - 1
                if isinstance(results[idx_pending], Exception):
                    # Pending count failure might be tolerable? 
                    # No, let's be strict to prevent stacking order bug
                    raise results[idx_pending]

            pending_count = 0
            if idx_pending != -1 and isinstance(results[idx_pending], int):
                pending_count = results[idx_pending]
            
            # Filter for current symbol
            real_pos_p = 0.0
            for p in pos_primary_list:
                # Debug logging to see what we got
                # logger.info(f"Primary Pos Check: {p}")
                if p.get('symbol') == self.symbol_primary:
                    real_pos_p = float(p.get('size', 0))
                    self.p_entry_price = float(p.get('entry_price', 0.0))
                    
            real_pos_s = 0.0
            if self.secondary:
                for p in pos_secondary_list:
                     # logger.info(f"Secondary Pos Check: {p}")
                    # Flexible matching for secondary
                    # Backpack Spot returns "SOL", "USDC"
                    # Backpack Perp returns "SOL_USDC"
                    # Config uses "SOL-USDC"
                    
                    target_base = self.symbol_secondary.split("-")[0] # SOL
                    target_perp = self.symbol_secondary.replace("-", "_") # SOL_USDC
                    
                    s_sym = p.get('symbol', '')
                    
                    # Match exact, perp, or base asset (for spot)
                    # Backpack returns symbols like "BTC_USDC_PERP"
                    if (s_sym == self.symbol_secondary or 
                        s_sym == target_perp or 
                        s_sym == f"{target_perp}_PERP"): # Added target_base check removed as it's too broad for perp
                        
                        real_pos_s = float(p.get('size', 0))
                        self.s_entry_price = float(p.get('entry_price', 0.0))

            return {
                "real_pos_primary": real_pos_p,
                "real_pos_secondary": real_pos_s,
                "pending_orders_primary": pending_count
            }
        except Exception as e:
            logger.error(f"Error verifying positions: {e}")
            raise e # No silent fallback to 0.0

    async def execute_position_exit(self, exit_direction: str, sizes: Dict = None):
        """
        Execute exit orders (Close Position).
        """
        if self.quarantine_mode:
            logger.warning(f"[{self.name}] Quarantine Mode Active. Skipping exit.")
            return

        # Cooldown Check
        now = time.time()
        if now - self.last_execution_time < self.execution_cooldown:
            logger.warning(f"[{self.name}] Exit skipped due to cooldown ({int(now - self.last_execution_time)}s < {self.execution_cooldown}s)")
            return

        logger.info(f"=== EXIT EXECUTION ({'SIMULATION' if self.is_simulation else 'LIVE'}) ===")
        logger.info(f"Closing Direction: {exit_direction} | Size: {self.current_position_size}")
        
        self.last_execution_time = now
        
        if self.is_simulation:
            logger.info("✅ [SIMULATION] Exit simulated.")
            self.current_position = None
            self.current_position_size = 0.0
            self.entry_time = None
            return

        # Determine sides for closing
        side_primary = "BUY"
        side_secondary = "SELL"
        
        if exit_direction == "SHORT_PRIMARY_LONG_SECONDARY":
            side_primary = "BUY"
            side_secondary = "SELL"
        elif exit_direction == "LONG_PRIMARY_SHORT_SECONDARY":
            side_primary = "SELL"
            side_secondary = "BUY"
            
        tasks = []
        # Close specific sizes to fix imbalances during exit
        size_to_close_p = self.current_position_size
        size_to_close_s = self.current_position_size
        
        if sizes:
            size_to_close_p = abs(sizes.get("real_pos_primary", 0.0))
            size_to_close_s = abs(sizes.get("real_pos_secondary", 0.0))
            logger.info(f"Precision Closure: P={size_to_close_p}, S={size_to_close_s}")
        
        # Task A: Primary
        if size_to_close_p > 0:
            tasks.append(self.primary.create_order(
                self.symbol_primary, 
                side_primary, 
                "MARKET", 
                0, 
                size_to_close_p
            ))
        
        # Task B: Secondary
        if self.secondary and size_to_close_s > 0:
            tasks.append(self.secondary.create_order(
                self.symbol_secondary,
                side_secondary,
                "MARKET",
                0, 
                size_to_close_s
            ))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Exit Results: {results}")
        
        # Helper
        def is_success(res):
            if isinstance(res, Exception): return False
            if isinstance(res, dict) and "error" in res: return False
            return True
            
        p_ok = is_success(results[0])
        s_ok = True
        if self.secondary: s_ok = is_success(results[1] if len(results) > 1 else None)
        
        if p_ok and s_ok:
            logger.info("✅ Position Closed Successfully!")
            pnl = 0.0
            try:
                t_p = await self.primary.fetch_ticker(self.symbol_primary)
                curr_price_p = t_p.get("last_price", 0)
                
                # Estimate Secondary Price
                curr_price_s = 0.0
                if self.secondary:
                    try:
                        t_s = await self.secondary.fetch_ticker(self.symbol_secondary)
                        curr_price_s = t_s.get("last_price", 0)
                    except: pass
                
                pnl = self._calculate_pnl(self.current_position, self.p_entry_price, self.s_entry_price, curr_price_p, curr_price_s)
                # Wait, _calculate_pnl needed secondary price. 
                # Let's just log what we have.
                
                self.realized_pnl += pnl
                
                # --- DB RECORDING ---
                self.db.record_trade({
                    "strategy": self.name,
                    "symbol": self.symbol_primary,
                    "direction": self.current_position, # e.g. SHORT_PRIMARY_... which acts as invalid direction? No, stick to raw string.
                    "entry_time": self.entry_time,
                    "size": self.current_position_size,
                    "entry_price_p": self.p_entry_price,
                    "entry_price_s": self.s_entry_price,
                    "exit_price_p": curr_price_p, 
                    "exit_price_s": curr_price_s,
                    "pnl_realized": pnl
                })
                
            except Exception as e:
                logger.error(f"PnL Calc/DB Error: {e}")
            
            logger.info(f"💰 Session PnL: ${self.realized_pnl:.2f} (Trade: ${pnl:.2f})")
            
            self.current_position = None
            self.current_position_size = 0.0
            self.entry_time = None
        else:
            logger.critical(f"⚠️ EXIT FAILED or PARTIAL! Primary={p_ok}, Secondary={s_ok}")
            if not self.is_simulation:
                self.quarantine_mode = True
                logger.error(f"[{self.name}] ENTERING QUARANTINE due to failed exit. Manual check required.")
            
            if p_ok: self.current_position_size -= size_to_close_p
            return False

    async def execute_dual_leg_entry(self, direction: str, quantity: float):
        """
        Execute dual-leg orders based on direction.
        """
        logger.info(f"=== EXECUTION ({'SIMULATION' if self.is_simulation else 'LIVE'}) ===")
        logger.info(f"Direction: {direction} | Size: {quantity}")
        
        if self.is_simulation:
            logger.info("✅ [SIMULATION] Orders simulated.")
            self.current_position = direction
            self.current_position_size += quantity
            self.entry_time = datetime.now()
            return

        logger.info("Placing orders...")
        
        side_primary = "BUY"
        side_secondary = "SELL"
        
        if direction == "SHORT_PRIMARY_LONG_SECONDARY":
            side_primary = "SELL"
            side_secondary = "BUY"
        elif direction == "LONG_PRIMARY_SHORT_SECONDARY":
            side_primary = "BUY"
            side_secondary = "SELL"
        else:
            logger.error(f"Unknown direction: {direction}")
            return
            
        tasks = []
        
        # Task A: Primary
        tasks.append(self.primary.create_order(
            self.symbol_primary, 
            side_primary, 
            "MARKET", 
            0, 
            quantity
        ))
        
        # Task B: Secondary
        if self.secondary:
            tasks.append(self.secondary.create_order(
                self.symbol_secondary,
                side_secondary,
                "MARKET",
                0,
                quantity
            ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"Execution Results: {results}")
        
        # Analyze Results
        # P = Primary (Index 0), S = Secondary (Index 1)
        res_p = results[0]
        res_s = results[1] if len(results) > 1 else None
        
        # Helper to check success: Dict with 'id' or NOT Exception/ErrorDict
        def is_success(res):
            if isinstance(res, Exception): return False
            if isinstance(res, dict) and "error" in res: return False
            return True
        
        p_ok = is_success(res_p)
        s_ok = True
        if self.secondary: s_ok = is_success(res_s)
        
        if p_ok and s_ok:
            logger.info("✅ Both Legs Executed Successfully.")
            self.current_position = direction
            self.current_position_size += quantity
            self.entry_time = datetime.now()
            
            # Reset error backoff
            self.error_backoff = 10
            
        elif not p_ok and not s_ok:
            logger.error("❌ Both legs FAILED. No position taken.")
            import time
            self.last_error_time = time.time()
        else:
            # Partial Fill
            logger.critical(f"⚠️ PARTIAL FILL DETECTED! Primary={p_ok}, Secondary={s_ok}")
            import time
            self.last_error_time = time.time()
            
            if self.auto_revert:
                logger.warning("🔄 Initiating AUTO-REVERT of filled leg (Max 3 retries)...")
                
                async def try_revert(exchange, symbol, side, qty):
                    for i in range(3):
                        try:
                            logger.info(f"Revert Attempt {i+1}/3: Closing {side} {qty} on {symbol}")
                            res = await exchange.create_order(symbol, side, "MARKET", 0, qty)
                            if is_success(res):
                                logger.warning(f"✅ Revert Successful on Attempt {i+1}")
                                return True
                            else:
                                logger.error(f"Revert Attempt {i+1} Failed: {res}")
                        except Exception as e:
                            logger.error(f"Revert Attempt {i+1} Exception: {e}")
                        
                        await asyncio.sleep(1) # Wait before retry
                    return False

                if p_ok:
                    # Close Primary
                    rev_side = "SELL" if side_primary == "BUY" else "BUY"
                    rev_ok = await try_revert(self.primary, self.symbol_primary, rev_side, quantity)
                    if not rev_ok:
                        logger.critical("🚨 CRITICAL: Primary Revert FAILED! ENTERING QUARANTINE.")
                        self.quarantine_mode = True
                        self.quarantine_reason = "Primary Revert Failed"
                    
                elif s_ok:
                    # Close Secondary
                    rev_side = "SELL" if side_secondary == "BUY" else "BUY"
                    rev_ok = await try_revert(self.secondary, self.symbol_secondary, rev_side, quantity)
                    if not rev_ok:
                        logger.critical("🚨 CRITICAL: Secondary Revert FAILED! ENTERING QUARANTINE.")
                        self.quarantine_mode = True
                        self.quarantine_reason = "Secondary Revert Failed"
            else:
                logger.critical("MANUAL INTERVENTION REQUIRED. Auto-revert is OFF. ENTERING QUARANTINE.")
                self.quarantine_mode = True
                self.quarantine_reason = "Partial fill / Auto-revert off"
