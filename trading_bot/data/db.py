import sqlite3
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger("trading_bot.data.db")

class DatabaseManager:
    def __init__(self, db_path: str = "trades.db"):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Initialize database schema."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time DATE,
            exit_time DATE,
            size REAL,
            entry_price_p REAL,
            entry_price_s REAL,
            exit_price_p REAL,
            exit_price_s REAL,
            fee_p REAL DEFAULT 0,
            fee_s REAL DEFAULT 0,
            funding_p REAL DEFAULT 0,
            funding_s REAL DEFAULT 0,
            pnl_realized REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fills (
             id TEXT PRIMARY KEY, -- Exchange ID
             symbol TEXT,
             side TEXT,
             price REAL,
             amount REAL,
             fee REAL,
             timestamp INTEGER, -- UNIX MS
             strategy TEXT
        );

        CREATE TABLE IF NOT EXISTS funding_payments (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             symbol TEXT,
             amount REAL,
             timestamp INTEGER, -- UNIX MS
             strategy TEXT,
             UNIQUE(symbol, timestamp) -- Avoid duplicates
        );
        """
        try:
            with self._get_conn() as conn:
                conn.executescript(create_table_sql)
            logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to init DB: {e}")

    def record_trade(self, trade_data: Dict[str, Any]):
        """
        Record a completed trade.
        """
        sql = """
        INSERT INTO trades (
            strategy, symbol, direction, entry_time, exit_time, size,
            entry_price_p, entry_price_s, exit_price_p, exit_price_s, 
            fee_p, fee_s, funding_p, funding_s,
            pnl_realized
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            trade_data.get("strategy"),
            trade_data.get("symbol"),
            trade_data.get("direction"),
            trade_data.get("entry_time"),
            datetime.now().isoformat(), # Exit time is now
            trade_data.get("size"),
            trade_data.get("entry_price_p"),
            trade_data.get("entry_price_s"),
            trade_data.get("exit_price_p"),
            trade_data.get("exit_price_s"),
            trade_data.get("fee_p", 0.0),
            trade_data.get("fee_s", 0.0),
            trade_data.get("funding_p", 0.0),
            trade_data.get("funding_s", 0.0),
            trade_data.get("pnl_realized")
        )
        
        try:
            with self._get_conn() as conn:
                conn.execute(sql, params)
            logger.info(f"Trade recorded: {trade_data.get('symbol')} PnL={trade_data.get('pnl_realized')}")
        except Exception as e:
            logger.error(f"Failed to record trade: {e}")

    def get_total_pnl(self, strategy: str = None) -> float:
        """
        Get total realized PnL.
        """
        sql = "SELECT SUM(pnl_realized) FROM trades"
        params = ()
        if strategy:
            sql += " WHERE strategy = ?"
            params = (strategy,)
            
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(sql, params)
                result = cursor.fetchone()[0]
                return result if result else 0.0
        except Exception as e:
            logger.error(f"Failed to get total PnL: {e}")
            return 0.0

    def record_fills(self, fills: List[Dict[str, Any]]):
        if not fills: return
        sql = """
        INSERT OR IGNORE INTO fills (id, symbol, side, price, amount, fee, timestamp, strategy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        data = [
            (f['id'], f['symbol'], f['side'], f['price'], f['amount'], f['fee'], f['timestamp'], f.get('strategy')) 
            for f in fills
        ]
        try:
            with self._get_conn() as conn:
                conn.executemany(sql, data)
        except Exception as e:
            logger.error(f"Failed to record fills: {e}")

    def record_funding(self, payments: List[Dict[str, Any]]):
        if not payments: return
        sql = """
        INSERT OR IGNORE INTO funding_payments (symbol, amount, timestamp, strategy)
        VALUES (?, ?, ?, ?)
        """
        data = [
            (p['symbol'], p['amount'], p['timestamp'], p.get('strategy')) 
            for p in payments
        ]
        try:
            with self._get_conn() as conn:
                conn.executemany(sql, data)
        except Exception as e:
            logger.error(f"Failed to record funding: {e}")
