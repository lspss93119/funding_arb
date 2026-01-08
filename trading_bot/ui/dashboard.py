from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.console import Console
from rich import box
from rich.text import Text
from datetime import datetime
from typing import Dict, Any

class TradingDashboard:
    def __init__(self):
        self.console = Console()
        self.layout = Layout()
        self.states: Dict[str, Dict[str, Any]] = {}
        self.prices: Dict[str, float] = {} # Symbol -> Price
        self.balances: Dict[str, Dict[str, float]] = {}
        self.logs = []
        self.max_logs = 10
        
        # Initial Setup
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="main", size=10),      # Fixed: Strategies
            Layout(name="balances", size=8),   # Fixed: Balances
            Layout(name="footer", ratio=1)     # Flexible: Logs
        )
        
    def update_state(self, strategy_name: str, data: Dict[str, Any]):
        if strategy_name not in self.states:
            self.states[strategy_name] = {}
        self.states[strategy_name].update(data)
        self.states[strategy_name]['last_update'] = datetime.now().strftime("%H:%M:%S")

        # Update Price Cache
        if "symbol" in data and "price" in data:
            sym = data["symbol"] # e.g. SOL-USDC
            price = data["price"]
            if price > 0:
                 # Map base asset to price
                 base = sym.split("-")[0] # SOL
                 self.prices[base] = price
                 self.prices["USDC"] = 1.0
                 self.prices["USD"] = 1.0


    def update_balance(self, exchange_name: str, balances: Dict[str, float]):
        self.balances[exchange_name] = balances
        # self.add_log(f"Bal Update {exchange_name}: {balances}")

    def add_log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

    def generate_table(self) -> Table:
        table = Table(expand=False, show_lines=True, box=box.ROUNDED)
        table.add_column("Strategy", style="cyan")
        table.add_column("Net Rate", justify="right")
        table.add_column("Rates (P / S)", justify="center")
        table.add_column("Position", justify="right", no_wrap=True)
        table.add_column("Real P", justify="right", style="bold")
        table.add_column("Real S", justify="right", style="bold")
        table.add_column("PnL", justify="right", style="bold green") # New Column
        table.add_column("Status", style="magenta")
        table.add_column("Last Update", style="dim")

        for name, data in self.states.items():
            # Spread Coloring
            spread = data.get('spread', 0)
            spread_color = "green" if spread < -0.5 else "red" if spread > 0.5 else "yellow"
            spread_text = f"[{spread_color}]{spread:.4f}%[/{spread_color}]"
            
            # Position
            pos = data.get('position_size', 0)
            pos_disp = data.get('position_display') # Optional override
            max_pos = data.get('max_position', 0)
            
            # Format pos depending on type
            if pos_disp:
                pos_text = f"{pos_disp} / {max_pos}"
            else:
                # Fallback: Try to calculate USD-equivalent if we have price and MaxPos is USD
                pos_val = pos
                is_usd_limit = str(max_pos).startswith("$")
                
                # Try to find price
                sym = data.get('symbol', '')
                base = sym.split("-")[0] if "-" in sym else sym
                price = self.prices.get(base, 0)
                
                if is_usd_limit and price > 0:
                     est_val = pos * price
                     pos_text = f"${est_val:.2f} / {max_pos}"
                else:
                     pos_str = f"{pos:.4f}" if isinstance(pos, (int, float)) else str(pos)
                     pos_text = f"{pos_str} / {max_pos}"
            
            # Rates
            r_p = data.get('rate_primary', 0)
            r_s = data.get('rate_secondary', 0)
            rates_text = f"{r_p:.2f}% / {r_s:.2f}%"
            
            # Real Positions
            rp = data.get('real_pos_primary', 0)
            rs = data.get('real_pos_secondary', 0)
            
            rp_disp = data.get('real_pos_primary_disp')
            rs_disp = data.get('real_pos_secondary_disp')
            
            # Helper for color
            def get_pos_color(val, bot_pos_size):
                # 1. Mismatch Checks
                # Bot thinks open (>0), Real is 0 -> DANGER
                if abs(bot_pos_size) > 0.0001 and abs(val) < 0.0001:
                    return "bold red" # Danger: Position missing
                # Bot thinks closed (0), Real is open -> WARNING
                if abs(bot_pos_size) < 0.0001 and abs(val) > 0.0001:
                    return "yellow" # Warning: Residual position
                    
                # 2. Normal State Color (Green = Long, Red = Short)
                if val > 0.0001: return "green"
                if val < -0.0001: return "red"
                return "dim white"

            bot_pos_size = data.get('position_size', 0)
            
            rp_color = get_pos_color(rp, bot_pos_size)
            # For Secondary, we check against bot_pos_size too, but secondary side might be opposite?
            # Actually bot_pos_size is just magnitude. 
            # Ideally we check against expected side, but for now just sign coloring is key.
            # Mismatch logic for secondary might be tricky if we don't track expected secondary side explicitly in data.
            # But the 'residual' check (bot says 0, real says >0) is valid for both.
            # The 'missing' check (bot says >0, real says 0) is also valid.
            
            rs_color = get_pos_color(rs, bot_pos_size)

            real_text_p = f"[{rp_color}]{rp}[/{rp_color}]"
            status = data.get('status', 'Waiting')
            last_update = data.get('last_update', '')
            
            # PnL
            pnl = data.get('realized_pnl', 0.0)
            pnl_color = "green" if pnl >= 0 else "red"
            pnl_text = f"[{pnl_color}]${pnl:.2f}[/{pnl_color}]"

            table.add_row(
                name, 
                spread_text, 
                rates_text, 
                pos_text,
                Text(str(rp), style=get_pos_color(rp, pos)),
                Text(str(rs), style="dim" if rs == 0 else "white"),
                pnl_text,
                status,
                last_update
            )
        return table

    def generate_balance_table(self) -> Table:
        table = Table(expand=False, show_lines=True, box=box.ROUNDED)
        table.add_column("Exchange", style="cyan")
        table.add_column("Asset", style="yellow")
        table.add_column("Balance", justify="right", style="green", no_wrap=True)

        for exchange, bals in self.balances.items():
            first = True
            
            # Sort Assets: USD/USDC first, then Value High->Low? 
            # Or just Alphabetical?
            # User wants "Lent USDC" visible.
            
            def sort_key(item):
                k, v = item
                if k in ["USD", "USDC"]: return " AAAAA" # Force top
                if k == "SOL": return " BBBBB"
                return k
                
            sorted_items = sorted(bals.items(), key=sort_key)

            for asset, amount in sorted_items:
                # Filter: Allow major assets
                allowed = ["USDC", "SOL", "BTC", "ETH", "WBTC"]
                if exchange == "Backpack" and asset not in allowed:
                    continue
                # Skip dust
                if amount < 0.001: continue
                
                # Cosmetic: Show USDC as USD
                display_asset = "USD" if asset == "USDC" else asset
                
                # Value Calc
                val_str = f"{amount:.4f}"
                try:
                    if asset in self.prices:
                        val = amount * self.prices[asset]
                        # Format: $123.45 (1.23 SOL)
                        val_str = f"[green]${val:.2f}[/green] ({amount:.4f} {display_asset})"
                    elif asset == "USDC" or asset == "USD":
                         val_str = f"[green]${amount:.2f}[/green]"
                except Exception as e:
                     val_str = f"{amount:.4f} (Err)"
                     # self.add_log(f"Display Error: {e}")

                table.add_row(
                    exchange if first else "",
                    display_asset,
                    val_str
                )
                first = False
        return table

    def render_layout(self) -> Layout:
        # Header
        self.layout["header"].update(
            Panel(Text("🚀 Low Gravity Arbitrage Bot 🚀", justify="center", style="bold white"), style="blue")
        )
        
        # Main Table
        self.layout["main"].update(
            Panel(self.generate_table(), title="Active Strategies", border_style="green")
        )
        
        # Balance Table
        self.layout["balances"].update(
             Panel(self.generate_balance_table(), title="Account Balances", border_style="cyan")
        )
        
        # Logs
        log_text = "\n".join(self.logs)
        self.layout["footer"].update(
            Panel(log_text, title="Log Output", border_style="yellow")
        )
        
        return self.layout

    def __rich__(self):
        return self.render_layout()

    def create_live(self):
        return Live(self, refresh_per_second=4, screen=False) # screen=True for fullscreen
