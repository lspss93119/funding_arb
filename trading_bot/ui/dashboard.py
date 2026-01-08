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
        
        # Initial Setup - Using Ratios for better scaling
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="monitoring", ratio=2, minimum_size=5),  # Proportional
            Layout(name="positions", ratio=2, minimum_size=5),   # Proportional
            Layout(name="balances", ratio=2, minimum_size=5),    # Proportional
            Layout(name="footer", ratio=1, minimum_size=3)       # Logs
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

    def generate_monitoring_table(self) -> Table:
        table = Table(expand=True, show_lines=True, box=box.ROUNDED)
        table.add_column("Symbol", style="cyan", no_wrap=True, min_width=15)
        table.add_column("Lighter (H)", justify="right", min_width=10)
        table.add_column("Backpack (H)", justify="right", min_width=10)
        table.add_column("EdgeX (H)", justify="right", min_width=10)
        table.add_column("Best Spread", justify="right", style="bold yellow", min_width=12)

        for name, data in self.states.items():
            # Rates
            r_lighter = data.get('rate_lighter', 0.0)
            r_backpack = data.get('rate_backpack', 0.0)
            r_edgex = data.get('rate_edgex', 0.0)
            
            def fmt_rate(val):
                if val == 0 and "rate_lighter" not in data and "rate_backpack" not in data and "rate_edgex" not in data:
                    return "-"
                color = "green" if val > 0 else "red" if val < 0 else "white"
                return f"[{color}]{val:.2f}%[/{color}]"

            # Spread (Best Spread)
            spread = data.get('spread', 0)
            spread_color = "green" if spread < -0.5 else "red" if spread > 0.5 else "yellow"
            spread_text = f"[{spread_color}]{spread:.4f}%[/{spread_color}]"

            table.add_row(
                name,
                fmt_rate(r_lighter),
                fmt_rate(r_backpack),
                fmt_rate(r_edgex),
                spread_text
            )
        return table

    def generate_status_table(self) -> Table:
        table = Table(expand=True, show_lines=True, box=box.ROUNDED)
        table.add_column("Symbol", style="cyan", no_wrap=True, min_width=15)
        table.add_column("Position (Size / Max)", justify="right", no_wrap=True, min_width=20)
        table.add_column("PnL ($)", justify="right", style="bold green", min_width=10)
        table.add_column("Current Status", style="magenta", min_width=15)
        table.add_column("Update", style="dim", width=8)

        for name, data in self.states.items():
            # Position
            pos = data.get('position_size', 0)
            pos_disp = data.get('position_display')
            max_pos = data.get('max_position', 0)
            
            if pos_disp:
                pos_text = f"{pos_disp} / {max_pos}"
            else:
                sym = data.get('symbol', '')
                base = sym.split("-")[0] if "-" in sym else sym
                price = self.prices.get(base, 0)
                is_usd_limit = str(max_pos).startswith("$")
                
                if is_usd_limit and price > 0:
                     est_val = pos * price
                     pos_text = f"${est_val:.2f} / {max_pos}"
                else:
                     pos_str = f"{pos:.4f}" if isinstance(pos, (int, float)) else str(pos)
                     pos_text = f"{pos_str} / {max_pos}"
            
            # PnL
            pnl = data.get('realized_pnl', 0.0)
            pnl_color = "green" if pnl >= 0 else "red"
            pnl_text = f"[{pnl_color}]${pnl:.2f}[/{pnl_color}]"

            status = data.get('status', 'Waiting')
            last_update = data.get('last_update', '')

            table.add_row(
                name,
                pos_text,
                pnl_text,
                status,
                last_update
            )
        return table

    def generate_balance_table(self) -> Table:
        table = Table(expand=True, show_lines=True, box=box.ROUNDED)
        table.add_column("Exchange", style="cyan")
        table.add_column("Asset", style="yellow")
        table.add_column("Balance / Value", justify="right", style="green", no_wrap=True)

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
        
        # Monitoring Block
        self.layout["monitoring"].update(
            Panel(self.generate_monitoring_table(), title="Funding Rate Monitoring (APR)", border_style="green")
        )

        # Positions Block
        self.layout["positions"].update(
            Panel(self.generate_status_table(), title="Portfolio & Strategy Status", border_style="magenta")
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
