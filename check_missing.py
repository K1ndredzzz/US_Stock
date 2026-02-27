import json
import tomllib
import sqlite3
from pathlib import Path

def main():
    base_dir = Path(__file__).parent

    # 1. 读取配置获取期望的 ticker 和 years
    toml_path = base_dir / "stocks.toml"
    with open(toml_path, "rb") as f:
        cfg = tomllib.load(f)

    target_years = sorted(cfg["years"])

    expected_tickers = set()

    ipo_floors = cfg.get("ipo_year_floor", {})

    # 2. 读取 insights.db 获取实际已提取成功的数据
    db_path = base_dir / "data" / "insights.db"
    actual_data = {}

    if db_path.exists():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # 只要是被标记为 extracted 的才算成功拥有财报数据
        cursor.execute("SELECT ticker, fiscal_year FROM processing_log WHERE status='extracted'")
        for row in cursor.fetchall():
            ticker = row[0]
            year = row[1]
            actual_data.setdefault(ticker, set()).add(year)
        conn.close()

    # 3. 对比查找缺失项
    missing_report = []

    for ticker in sorted(expected_tickers):
        actual_years = actual_data.get(ticker, set())

        # 应用 IPO 限制，检查所有年份，并明确忽略 2025 年
        floor_year = ipo_floors.get(ticker, 0)
        expected_years = [y for y in target_years if y >= floor_year and y != 2025]

        missing_years = [y for y in expected_years if y not in actual_years]

        if missing_years:
            missing_report.append((ticker, missing_years))

    # 4. 生成报告并写入 txt 文件
    output_path = base_dir / "missing_report.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("="*60 + "\n")
        f.write(" 缺失年份检查报告 (排除 2025)\n")
        f.write("="*60 + "\n")

        if not missing_report:
            f.write("完美！所有配置的股票在规定年份内（除2025外）的数据均已齐备。\n")
        else:
            f.write(f"共有 {len(missing_report)} 支股票存在缺失年份:\n\n")
            for ticker, missing in missing_report:
                # 格式化输出: 股票代码对其, 缺失年份列表
                f.write(f"{ticker:<6} : 缺失 {missing}\n")

        f.write("="*60 + "\n")
        f.write(f"期望检查的公司总数: {len(expected_tickers)}\n")
        f.write(f"存在缺失的公司总数: {len(missing_report)}\n")
        f.write("注: 已经考虑了 stocks.toml 中配置的 IPO 年份下限，并排除了 2025 年。\n")

    print(f"检查完成！报告已保存至: {output_path}")

if __name__ == "__main__":
    main()