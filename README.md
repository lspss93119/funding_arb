# 💰 資金費率套利機器人 (Funding Arbitrage Bot)

這是一款高效能的交易機器人，專為 Lighter (永續合約) 與 Backpack (現貨/永續合約) 之間的資金費率套利而設計。

## 🚀 核心功能
- **Lighter 深度整合**：針對 Lighter 獨特的 API 與 Nonce 管理機制進行優化，確保下單穩定。
- **Backpack 整合**：可靠的現貨或對沖端執行邏輯。
- **TUI 儀表板**：提供即時的價差 (Spread)、持倉狀態 (Positions) 與帳戶餘額監控。
- **安全第一**：內建交易冷卻時間 (Cooldown) 與異常隔離模式 (Quarantine Mode)，防止極端行情下的錯誤循環。
- **多交易對支持**：可同時併發運行 SOL、BTC 與 ETH 等多個交易對策略。

## 📥 安裝說明

1. **複製儲存庫：**
   ```bash
   git clone <你的 GitHub 倉庫網址>
   cd agents
   ```

2. **建立虛擬環境：**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **配置環境變數：**
   建立 `.env` 檔案並填入你的 API 金鑰：
   ```env
   LIGHTER_PRIVATE_KEY=你的私鑰
   LIGHTER_ACCOUNT_INDEX=0
   LIGHTER_API_KEY_INDEX=0
   BACKPACK_API_KEY=你的金鑰
   BACKPACK_API_SECRET=你的密鑰
   ```

4. **調整策略參數：**
   編輯 `config.yaml` 檔案，設定想要交易的幣種與風險參數。

## 🏁 啟動機器人

在終端機執行以下指令啟動，並進入 TUI 監控面板：
```bash
python3 run_strategy.py
```

## 🛡️ 開發最佳實踐
為了確保交易穩定與私鑰安全，請務必參考 [development_best_practices.md](development_best_practices.md) 中的開發與部署工作流建議。
