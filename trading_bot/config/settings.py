import yaml
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

class Config:
    def __init__(self, config_path: str = "config.yaml"):
        # Resolve absolute path for config file to avoid "not found" errors
        # Assuming config.yaml is in the ROOT (agents/) or relative to cwd? 
        # But settings.py is in trading_bot/config/
        # base_dir is agents/
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.config_path = os.path.join(base_dir, config_path)
        self.data = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        config_data = {}
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config_data = yaml.safe_load(f)
            except Exception as e:
                print(f"Error loading config: {e}")
        else:
             print(f"Config file {self.config_path} not found. Using defaults.")
        
        # Inject Sensitive Data from ENV -> Config
        # This allows code to just call config.get('api_key') transparently
        if os.getenv("BACKPACK_API_KEY"):
            config_data["backpack_api_key"] = os.getenv("BACKPACK_API_KEY")
        if os.getenv("BACKPACK_API_SECRET"):
            config_data["backpack_api_secret"] = os.getenv("BACKPACK_API_SECRET")
            
        return config_data

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def get_strategy_config(self, strategy_name: str = None) -> Dict[str, Any]:
        """Get config for specific strategy (or active one if None)."""
        if not strategy_name:
            strategies = self.get_active_strategies()
            strategy_name = strategies[0] if strategies else None
        
        strategies = self.data.get("strategies", {})
        return strategies.get(strategy_name, {})

    def get_active_strategies(self) -> list:
        """Get list of active strategy names."""
        active = self.data.get("active_strategies")
        if active:
            return active if isinstance(active, list) else [active]
        
        # Fallback to legacy key
        single = self.data.get("active_strategy")
        return [single] if single else []

    def get_exchange_config(self, exchange_name: str) -> Dict[str, Any]:
        exchanges = self.data.get("exchanges", {})
        return exchanges.get(exchange_name, {})

# Global instance
config = Config()
