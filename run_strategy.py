import asyncio
import logging
import sys
import os
from typing import Dict, List
from logging.handlers import RotatingFileHandler
from trading_bot.config.settings import config
from trading_bot.exchanges.base import Exchange
from trading_bot.exchanges.backpack import BackpackExchange
from trading_bot.exchanges.lighter import LighterExchange
from trading_bot.exchanges.edgex import EdgeXExchange
from trading_bot.strategies.funding_arb import FundingArbitrageStrategy
from trading_bot.strategies.dynamic_funding_arb import DynamicFundingArbitrageStrategy
from trading_bot.ui.dashboard import TradingDashboard

# Initialize Dashboard
dashboard = TradingDashboard()

class DashboardLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        dashboard.add_log(msg)

# Configure Logging
# Use Dashboard Handler instead of stdout
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers = [] # Clear default

# 1. Dashboard Handler (For TUI)
handler = DashboardLogHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(name)s: %(message)s'))
root_logger.addHandler(handler)

# 2. File Handler (For Persistence)
try:
    log_file = "bot.log"
    file_handler = RotatingFileHandler(log_file, maxBytes=20*1024*1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
except Exception as e:
    # Fallback to TUI log if file setup fails
    pass

async def run_strategy_loop(strat_name: str, shared_exchanges: Dict[str, Exchange]):
    """
    Runs a single strategy loop.
    """
    logger = logging.getLogger(f"runner.{strat_name}")
    logger.info(f"--- Running Strategy: {strat_name} ---")
    
    strat_cfg = config.get_strategy_config(strat_name)
    if not strat_cfg:
        logger.error(f"Config not found for {strat_name}")
        return

    # Use Shared Exchanges
    lighter_ex = shared_exchanges.get("lighter")
    spot_ex = shared_exchanges.get("backpack")
    
    p_name = strat_cfg.get("primary_exchange", "lighter")
    s_name = strat_cfg.get("secondary_exchange", "backpack")
    pair = strat_cfg.get("pair") 

    try:
        if strat_cfg.get("type") == "dynamic_funding_arbitrage":
             logger.info(f"[{strat_name}] Config Read: max_pos={strat_cfg.get('max_position_size_usd')} order={strat_cfg.get('order_size_usd')}")
             strategy_params = {
                "pair": pair,
                "available_exchanges": strat_cfg.get("available_exchanges", ["lighter", "backpack", "edgex"]),
                "entry_threshold_apr": strat_cfg.get("entry_threshold_apr", 0.05),
                "exit_threshold_apr": strat_cfg.get("exit_threshold_apr", 0.0),
                "order_size_usd": strat_cfg.get("order_size_usd", 100),
                "max_position_size_usd": strat_cfg.get("max_position_size_usd", 100.0),
                "is_simulation": strat_cfg.get("is_simulation", True),
                "exchange_configs": config.get("exchanges", {}),
                "on_state_update": lambda data: dashboard.update_state(strat_name, data)
             }
             strategy = DynamicFundingArbitrageStrategy(shared_exchanges, strategy_params)
        else:
             primary_ex = shared_exchanges.get(p_name)
             secondary_ex = shared_exchanges.get(s_name)
             
             if not primary_ex or not secondary_ex:
                 logger.error(f"[{strat_name}] Required exchanges ({p_name}, {s_name}) not initialized.")
                 return

             s_ex_cfg = config.get_exchange_config(s_name)
             s_map = s_ex_cfg.get("symbol_map", {})
             secondary_symbol = s_map.get(pair, pair)
             
             strategy_params = {
                 "symbol_primary": pair,
                 "symbol_secondary": secondary_symbol,
                 "position_size": 0.0,
                 "order_size_usd": strat_cfg.get("order_size_usd"),
                 "entry_threshold": strat_cfg.get("entry_threshold", 0.01),
                 "exit_threshold": strat_cfg.get("exit_threshold", 0.005),
                 "is_simulation": strat_cfg.get("is_simulation", True),
                 "min_apr": 0.05,
                 "max_position_size": strat_cfg.get("max_position_size", 0.0), 
                 "max_position_size_usd": strat_cfg.get("max_position_size_usd"),
                 "auto_revert": strat_cfg.get("auto_revert", False),
                 "on_state_update": lambda data: dashboard.update_state(strat_name, data)
             }
             
             exchanges = {
                 "primary": primary_ex,
                 "secondary": secondary_ex
             }
             strategy = FundingArbitrageStrategy(exchanges, strategy_params)
             
        logger.info(f"[{pair}] Strategy ({strat_cfg.get('type', 'fixed')}) Initialized.")
        
        # Force initial update to populate TUI row immediately
        # Fixed: Use correct max_position for dynamic
        def get_initial_max():
            if strat_cfg.get("type") == "dynamic_funding_arbitrage":
                return f"${strategy_params.get('max_position_size_usd', 0)}"
            return f"${strategy_params.get('max_position_size_usd', 0)}" # Standardize on USD display
            
        dashboard.update_state(strat_name, {
            "symbol": pair,
            "status": "Initializing...",
            "spread": 0.0,
            "position_size": 0.0,
            "max_position": get_initial_max()
        })
        await strategy.on_start()
        
        while True:
            await strategy.check_opportunity()
            await asyncio.sleep(60) # 1 Minute Interval
            
    except asyncio.CancelledError:
        logger.info(f"[{strat_name}] Stopping...")
    except Exception as e:
        logger.error(f"[{strat_name}] Error: {e}", exc_info=True)
    finally:
        logger.info(f"[{strat_name}] Stopped.")

def check_single_instance():
    pid_file = "bot.pid"
    if os.path.exists(pid_file):
        try:
            with open(pid_file, "r") as f:
                old_pid = int(f.read().strip())
            
            # Check if process is running
            try:
                os.kill(old_pid, 0) # Signal 0 checks existence
                print(f"⚠️  Another instance is running (PID {old_pid}). Exiting.")
                return False
            except OSError:
                pass
        except ValueError:
            pass

    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    return True

async def main():
    if not check_single_instance():
        return

    print("=== Starting Multi-Pair Bot ===")
    
    strategies = config.get_active_strategies()
    if not strategies:
        print("No active strategies configured.")
        return
        
    print(f"Active Strategies: {strategies}")
    
    # --- Shared Exchange Initialization ---
    shared_exchanges = {}
    
    # Lighter
    l_cfg = config.get_exchange_config("lighter")
    l_pk = os.getenv("LIGHTER_PRIVATE_KEY")
    l_acc = int(os.getenv("LIGHTER_ACCOUNT_INDEX", 0))
    l_api = int(os.getenv("LIGHTER_API_KEY_INDEX", 0))
    
    # Backpack
    bp_cfg = config.get_exchange_config("backpack")
    bp_key = config.get("backpack_api_key")
    bp_sec = config.get("backpack_api_secret")
    
    if not l_pk or not bp_key:
        print("CRITICAL: Missing credentials in .env or config.yaml")
        return

    shared_exchanges["lighter"] = LighterExchange(l_pk, l_acc, l_api, config=l_cfg)
    shared_exchanges["backpack"] = BackpackExchange(bp_key, bp_sec)
    
    # EdgeX (Check if keys exist)
    ex_pk = os.getenv("EDGEX_L2_PRIVATE_KEY")
    ex_acc = os.getenv("EDGEX_ACCOUNT_ID")
    if ex_pk and ex_acc:
        shared_exchanges["edgex"] = EdgeXExchange(ex_pk, int(ex_acc))
        print("EdgeX Exchange Initialized.")

    tasks = []
    for s in strategies:
        tasks.append(asyncio.create_task(run_strategy_loop(s, shared_exchanges)))
        
    # Balance Monitor Task
    async def balance_monitor():
        while True:
            try:
                for name, ex in shared_exchanges.items():
                    try:
                        bal = await ex.get_balance()
                        # User requested to hide SOL for Backpack
                        if name == "backpack":
                            bal = {k: v for k, v in bal.items() if k in ["USD", "USDC"]}
                        dashboard.update_balance(name.capitalize(), bal)
                    except Exception as e:
                        logging.getLogger("runner").warning(f"Balance fetch failed for {name}: {e}")
            except Exception as e:
                logging.getLogger("runner").error(f"Balance monitor error: {e}")
            await asyncio.sleep(60)

    monitor_task = asyncio.create_task(balance_monitor())

    # --- UI 運行區塊 (與核心邏輯去耦合) ---
    try:
        # 使用 Rich Live 作為顯示介面
        with dashboard.create_live():
            # 這裡我們監聽任務，但如果 UI 結束了，我們不讓任務結束
            try:
                # 這裡只是單純等待，直到發生中斷
                while True:
                    # 檢查背景任務是否全部崩潰
                    if all(t.done() for t in tasks) and monitor_task.done():
                        break
                    await asyncio.sleep(1)
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception as ui_err:
                logging.getLogger("runner").error(f"UI 介面發生錯誤: {ui_err}")
                logging.getLogger("runner").info("切換至背景交易模式（Headless Mode）...")
                # UI 崩潰後，我們進入無限等待，保持核心任務運行
                while True:
                    await asyncio.sleep(3600)
                    
    except KeyboardInterrupt:
        logging.getLogger("runner").info("檢測到使用者停止信號，正在安全關閉程式...")
    finally:
        # 清理實體進程
        for t in tasks:
            t.cancel()
        monitor_task.cancel()
        
        # 等待清理
        await asyncio.gather(*tasks, monitor_task, return_exceptions=True)
        
        # Shared Cleanup
        for ex in shared_exchanges.values():
            await ex.close()
        
        if os.path.exists("bot.pid"):
            os.remove("bot.pid")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
