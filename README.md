# 💰 Funding Arbitrage Bot

A high-performance trading bot designed for funding rate arbitrage between Lighter (Perp) and Backpack (Spot/Perp).

## 🚀 Key Features
- **Lighter Integration**: Specialized handling for Lighter's unique API and nonce management.
- **Backpack Integration**: Reliable spot/perp leg execution.
- **TUI Dashboard**: Real-time monitoring of spreads, positions, and account balances.
- **Safety First**: Implements execution cooldowns and a quarantine mode for anomalous conditions.
- **Multi-Pair Support**: Capable of running SOL, BTC, and ETH strategies concurrently.

## 📥 Installation

1. **Clone the repository:**
   ```bash
   git clone <your-new-repo-url>
   cd agents
   ```

2. **Setup virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Environment:**
   Create a `.env` file with your API keys:
   ```env
   LIGHTER_PRIVATE_KEY=your_key
   LIGHTER_ACCOUNT_INDEX=0
   LIGHTER_API_KEY_INDEX=0
   BACKPACK_API_KEY=your_key
   BACKPACK_API_SECRET=your_secret
   ```

4. **Adjust Strategy:**
   Edit `config.yaml` to set your symbols and risk parameters.

## 🏁 Running the Bot

Start the bot in the foreground with the TUI dashboard:
```bash
python3 run_strategy.py
```

## 🛡️ Best Practices
Refer to [development_best_practices.md](development_best_practices.md) for safe development and deployment workflows.
