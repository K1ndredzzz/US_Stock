# US Stock Earnings Insight Pipeline

批量提取美股上市公司年报（10-K/20-F）中的 MD&A 和 Risk Factors 章节，使用 Gemini API 生成结构化商业洞察。

## 功能特性

- **自动下载**：从 SEC EDGAR 自动获取 10-K/20-F 年报
- **智能提取**：使用 Gemini API 提取 AI 投资、宏观风险、业务增长等关键信息
- **增量运行**：支持断点续传，已提取数据不会重复处理
- **灵活配置**：通过 `stocks.toml` 轻松管理公司列表和年份范围

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 API Key

复制 `.env.example` 为 `.env` 并填入你的 Gemini API Key：

```bash
cp .env.example .env
```

编辑 `.env`：

```env
GEMINI_API_KEY=your-actual-api-key-here
HTTPS_PROXY=http://127.0.0.1:7890  # 可选，如不需要代理可删除此行
```

### 3. 配置公司和年份

编辑 `stocks.toml`：

```toml
years = [2025, 2024, 2023, 2022, 2021]

[companies.Tier1_AI_Tech]
tickers = ["NVDA", "AMD", "MSFT", "GOOGL"]
```

### 4. 运行 Pipeline

```bash
python main.py
```

## 数据输出

- **SQLite 数据库**：`data/insights.db`
- **JSONL 备份**：`data/insights.jsonl`
- **原始年报**：`data/filings/{TICKER}/{YEAR}/filing.htm`

## 配置说明

### stocks.toml

- `years`：抓取的财年列表
- `foreign_filers`：提交 20-F 的外国公司（截止日期延长到 6 月 30 日）
- `ipo_year_floor`：IPO 年份下限，早于此年的数据自动跳过
- `companies.*`：按 Tier 分组的公司列表

### 提取字段

- `ai_investment_focus`：AI 投资重点
- `ai_monetization_status`：AI 商业化状态
- `capex_guidance_tone`：资本支出指引
- `china_exposure_risk`：中国市场风险
- `supply_chain_bottlenecks`：供应链瓶颈
- `restructuring_plans`：重组计划
- `efficiency_initiatives`：效率提升举措
- `mda_sentiment_score`：MD&A 情绪评分（1-10）
- `macro_concerns`：宏观经济担忧
- `growing_segments`：增长业务
- `shrinking_segments`：萎缩业务

## 技术架构

- **异步下载**：8 个并发 SEC EDGAR 下载器
- **异步提取**：6 个并发 Gemini API 工作器
- **分页支持**：自动检索 SEC 历史分页文件
- **容错机制**：4 次重试 + 指数退避
- **增量更新**：基于 SQLite `processing_log` 表跳过已完成项

## 许可证

MIT License
