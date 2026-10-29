#!/usr/bin/env python3
"""
ETF 份额数据获取脚本（交易所官方源 + 新浪净值源 · 本机定时版）
===============================================================
本版本专为「本机定时运行 + 结果推送 GitHub Pages」设计，
彻底绕开东方财富（其行情接口对高频/海外 IP 有严格反爬）。

数据源（全部为国内可直连、反爬宽松的接口）：
  - 份额 shares：上海证券交易所 fund_etf_scale_sse
                + 深圳证券交易所 fund_scale_daily_szse
  - 净值 nav   ：新浪财经 fund_etf_hist_sina（close 收盘价 ≈ 单位净值）
  - 规模 aum   ：shares × nav（亿份 × 净值 = 亿元）

净值抓取使用并发（ThreadPoolExecutor），避免原版「20 次串行查历史
净值」导致的卡死。

数据输出格式与原项目完全一致：
  docs/data/etf_shares.json
  { "last_update": "...", "records": { "2026-10-28": { "上证50": {shares, nav, aum} } } }
"""

import json
import os
import sys
import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# 在 import akshare 之前先抑制 tqdm 进度条
os.environ['TQDM_DISABLE'] = '1'

import tqdm


class _NullIO:
    def write(self, *a, **k):
        pass

    def flush(self, *a, **k):
        pass


_orig_tqdm = tqdm.tqdm


def _silent_tqdm(iterable=None, *args, **kwargs):
    kwargs['disable'] = True
    kwargs['file'] = _NullIO()
    if iterable is not None:
        return _orig_tqdm(iterable, *args, **kwargs)
    return _orig_tqdm(*args, **kwargs)


tqdm.tqdm = _silent_tqdm

try:
    import akshare as ak
    import pandas as pd
except Exception as e:
    print(f"导入 akshare 失败: {e}")
    sys.exit(1)


# ============================================================
# 标的映射配置（单一指数精确锁 · 见 target_components.py）
# ============================================================
try:
    from target_components import (
        TARGET_COMPONENT_CODES,
        COMPONENT_CODE_TO_TARGET,
        TARGET_TO_CODE,
    )
except ImportError:
    # 兼容独立运行时找不到模块的情况（极少见），回退到空映射
    TARGET_COMPONENT_CODES = {}
    COMPONENT_CODE_TO_TARGET = {}
    TARGET_TO_CODE = {}


def match_target(name: str):
    """根据 ETF 名称匹配所属标的（兜底用，正常情况下不触发）。
    精确锁已通过成分代码反向索引完成，此函数仅作极少数历史兼容。"""
    return None


def _sina_symbol(code: str) -> str:
    """把纯代码转成新浪所需的 sh/sz 前缀格式。"""
    code = str(code).strip()
    if code.startswith(("5", "6", "9")):
        return "sh" + code
    return "sz" + code


def fetch_sse_data(date_str: str) -> pd.DataFrame:
    """上海证券交易所 ETF 份额日报。"""
    try:
        df = ak.fund_etf_scale_sse(date=date_str)
        if df is not None and len(df) > 0:
            return df[["基金代码", "基金简称", "基金份额"]].copy()
    except Exception as e:
        print(f"[warn] 获取上交所数据失败: {e}")
    return pd.DataFrame(columns=["基金代码", "基金简称", "基金份额"])


def fetch_szse_data(date_str: str) -> pd.DataFrame:
    """深圳证券交易所份额日报（ETF + LOF，LOF 含白银 161226）。"""
    frames = []
    for sym in ("ETF", "LOF"):
        try:
            df = ak.fund_scale_daily_szse(
                start_date=date_str, end_date=date_str, symbol=sym
            )
            if df is not None and len(df) > 0:
                frames.append(df[["基金代码", "基金简称", "基金份额"]].copy())
        except Exception as e:
            print(f"[warn] 获取深交所 {sym} 数据失败: {e}")
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=["基金代码", "基金简称", "基金份额"])


def fetch_nav_sina(code: str) -> float | None:
    """从新浪获取指定 ETF 的最新收盘价（≈ 单位净值）。"""
    try:
        df = ak.fund_etf_hist_sina(symbol=_sina_symbol(code))
        if df is not None and len(df) > 0 and "close" in df.columns:
            val = df["close"].iloc[-1]
            if pd.notna(val):
                return float(val)
    except Exception:
        pass
    return None


def aggregate_by_target(df: pd.DataFrame) -> dict:
    """按标的汇总份额（同一标的不同基金公司求和）。"""
    result = {}

    for _, row in df.iterrows():
        code = str(row["基金代码"]).strip()
        shares = row["基金份额"]

        if pd.isna(shares) or shares == 0:
            continue

        # 精确锁定：成分代码反向索引（已含全部非增强非联接 ETF）
        target = COMPONENT_CODE_TO_TARGET.get(code)
        if target is None:
            continue

        result[target] = result.get(target, 0.0) + float(shares)

    return result


def shares_to_yi(shares: float) -> float:
    return round(shares / 1e8, 4)


def load_data(filepath: str) -> dict:
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_update": "", "records": {}}


def save_data(filepath: str, data: dict):
    import math
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    def clean_for_json(obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        elif isinstance(obj, dict):
            return {k: clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_for_json(v) for v in obj]
        return obj

    cleaned = clean_for_json(data)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2, allow_nan=False)


def fetch_daily_data(date_str: str = None) -> dict:
    """
    获取指定日期的 ETF 份额 + 净值，汇总出 shares/nav/aum。
    净值口径：成分加权综合净值（与 backfill_data.py 彻底版一致）。
    """
    if date_str is None:
        date_str = datetime.datetime.now().strftime("%Y%m%d")

    print(f"正在获取 {date_str} 的 ETF 份额数据（交易所官方源）...", flush=True)

    # 1. 份额：上交所 + 深交所（成分级）
    sse_df = fetch_sse_data(date_str)
    szse_df = fetch_szse_data(date_str)
    combined_df = pd.concat([sse_df, szse_df], ignore_index=True)

    if len(combined_df) == 0:
        print("[error] 交易所数据获取失败，无数据可处理！", flush=True)
        return {}

    print(f"共获取 {len(combined_df)} 条 ETF 份额数据", flush=True)

    # 2. 成分级份额聚合 {code: 份}
    code_shares = {}
    for _, row in combined_df.iterrows():
        code = str(row["基金代码"]).strip()
        shares = row["基金份额"]
        if pd.isna(shares) or shares == 0:
            continue
        if code not in COMPONENT_CODE_TO_TARGET:
            continue
        code_shares[code] = code_shares.get(code, 0.0) + float(shares)

    # 3. 加载成分净值缓存（nav_cache.json，含成分级拆分校正后的净值）
    from weighted_nav import fetch_all_component_nav, adjust_component_splits, aggregate_weighted

    nav_cache = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nav_cache.json")
    all_codes = sorted(set(
        c for codes in TARGET_COMPONENT_CODES.values() for c in codes
    ))
    nav_hist = fetch_all_component_nav(all_codes, cache_file=nav_cache)
    nav_adjusted, _ = adjust_component_splits(nav_hist)

    date_key = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    # 4. 逐标的加权聚合
    result = {}
    for target, codes in TARGET_COMPONENT_CODES.items():
        shares_by_code = {c: code_shares.get(c, 0.0) for c in codes}
        nav_by_code = {c: nav_adjusted.get(c, {}).get(date_key) for c in codes}
        rec = aggregate_weighted(codes, shares_by_code, nav_by_code)
        result[target] = rec

    print(f"汇总后得到 {len(result)} 个标的:", flush=True)
    for target, info in sorted(result.items(), key=lambda x: (x[1]["aum"] or 0), reverse=True):
        nav_str = f"净值:{info['nav']:.4f}" if info["nav"] else "净值:--"
        aum_str = f"{info['aum']:.2f}亿元" if info["aum"] else "规模:--"
        print(f"  {target}: {info['shares']} 亿份 | {nav_str} | {aum_str}", flush=True)

    return result


def update_data_file(filepath: str = None, date_str: str = None):
    if filepath is None:
        filepath = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "docs", "data", "etf_shares.json",
        )

    data = load_data(filepath)

    if date_str is None:
        today = datetime.datetime.now()
        date_key = today.strftime("%Y-%m-%d")
        date_api = today.strftime("%Y%m%d")
    else:
        if "-" in date_str:
            date_key = date_str
            date_api = date_str.replace("-", "")
        else:
            date_api = date_str
            date_key = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    daily_data = fetch_daily_data(date_api)

    if not daily_data:
        print(f"[warn] 未能获取 {date_key} 的数据，不更新文件", flush=True)
        return

    data["records"][date_key] = daily_data
    data["last_update"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sorted_records = dict(sorted(data["records"].items()))
    data["records"] = sorted_records

    save_data(filepath, data)
    print(f"[ok] 数据已更新并保存到 {filepath}", flush=True)


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    update_data_file(date_str=date_arg)
