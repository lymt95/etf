#!/usr/bin/env python3
"""
ETF 历史份额数据回填脚本（交易所官方源 + 新浪净值源）
=========================================================
用于回填 2026-07-09 ~ 2026-10-27 这段停更期间的历史数据。

数据源（全部绕开东财）：
  - 份额：上交所 fund_etf_scale_sse（逐日串行）+ 深交所 fund_scale_daily_szse（区间）
  - 净值：新浪 fund_etf_hist_sina（逐日 close，可算准 aum）

用法：
  python backfill_data.py 20260709 20261027
"""

import json
import os
import sys
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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

import akshare as ak
import pandas as pd


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
    TARGET_COMPONENT_CODES = {}
    COMPONENT_CODE_TO_TARGET = {}
    TARGET_TO_CODE = {}


def match_target(name: str):
    return None


def aggregate_by_target(df: pd.DataFrame) -> dict:
    result = {}
    for _, row in df.iterrows():
        code = str(row["基金代码"]).strip()
        shares = row["基金份额"]
        if pd.isna(shares) or shares == 0:
            continue
        target = COMPONENT_CODE_TO_TARGET.get(code)
        if target is None:
            continue
        result[target] = result.get(target, 0.0) + float(shares)
    return result


def aggregate_by_code(df: pd.DataFrame) -> dict:
    """成分级份额聚合：返回 {code: shares(份)}，只保留成分清单内的代码。
    用于成分加权综合净值的份额权重计算。份额单位保持「份」（原始值）。"""
    result = {}
    for _, row in df.iterrows():
        code = str(row["基金代码"]).strip()
        shares = row["基金份额"]
        if pd.isna(shares) or shares == 0:
            continue
        if code not in COMPONENT_CODE_TO_TARGET:
            continue
        result[code] = result.get(code, 0.0) + float(shares)
    return result


def shares_to_yi(shares: float) -> float:
    return round(shares / 1e8, 4)


def _sina_symbol(code: str) -> str:
    code = str(code).strip()
    if code.startswith(("5", "6", "9")):
        return "sh" + code
    return "sz" + code


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


def fetch_sse_one_day(date_api: str):
    """上交所单日份额。返回 (date_key, 成分级份额 {code: 份})。"""
    try:
        df = ak.fund_etf_scale_sse(date=date_api)
        if df is not None and len(df) > 0:
            sub = df[["基金代码", "基金份额"]].copy()
            code_shares = aggregate_by_code(sub)
            date_key = f"{date_api[:4]}-{date_api[4:6]}-{date_api[6:8]}"
            return date_key, code_shares
    except Exception as e:
        print(f"  [warn] 上交所 {date_api} 失败: {e}")
    return None, None


def backfill_sse(trade_days: list) -> dict:
    """逐日回填上交所（串行 + 间隔，避免限流）。
    返回 {date_key: {code: 份}}（成分级份额）。"""
    result = {}
    print(f"回填上交所 {len(trade_days)} 个交易日（串行，每 1.5 秒一次）...", flush=True)
    done = 0
    for d in trade_days:
        date_key, code_shares = fetch_sse_one_day(d)
        done += 1
        if date_key and code_shares:
            result[date_key] = code_shares
        if done % 10 == 0:
            print(f"  [{done}/{len(trade_days)}] 已处理，成功 {len(result)} 个", flush=True)
        time.sleep(1.5)
    print(f"上交所回填完成，得到 {len(result)} 个日期", flush=True)
    return result


def _split_ranges(start_date: str, end_date: str, months: int = 4) -> list:
    """把大区间按 N 个月切成小段，返回 [(start, end), ...]（YYYYMMDD 格式）。"""
    start = datetime.datetime.strptime(start_date, "%Y%m%d")
    end = datetime.datetime.strptime(end_date, "%Y%m%d")
    segments = []
    cur = start
    while cur <= end:
        # 往后推 months 个月
        y, m = cur.year, cur.month + months - 1
        while m > 12:
            m -= 12
            y += 1
        seg_end = datetime.datetime(y, m, min(cur.day, 28))
        seg_end = min(seg_end, end)
        segments.append((cur.strftime("%Y%m%d"), seg_end.strftime("%Y%m%d")))
        cur = seg_end + datetime.timedelta(days=1)
    return segments


def backfill_szse(start_date: str, end_date: str) -> dict:
    """深交所区间回填。深交所接口单次返回上限 65535 行，
    跨年/超长区间会返回空，因此必须分段查询（每段约 4 个月）。
    symbol 同时取 ETF 和 LOF（LOF 含白银 161226）。
    返回 {date_key: {code: 份}}（成分级份额）。"""
    result = {}
    segments = _split_ranges(start_date, end_date, months=4)
    print(f"回填深交所 {start_date}~{end_date}（分 {len(segments)} 段，ETF+LOF）...", flush=True)
    for seg_start, seg_end in segments:
        for sym in ("ETF", "LOF"):
            try:
                df = ak.fund_scale_daily_szse(start_date=seg_start, end_date=seg_end, symbol=sym)
                if df is not None and len(df) > 0:
                    for date_val, group in df.groupby("日期"):
                        date_key = pd.Timestamp(date_val).strftime("%Y-%m-%d")
                        code_shares = aggregate_by_code(group)
                        if code_shares:
                            if date_key not in result:
                                result[date_key] = {}
                            for c, s in code_shares.items():
                                result[date_key][c] = result[date_key].get(c, 0.0) + s
                else:
                    print(f"  [warn] 深交所 {sym} {seg_start}~{seg_end} 返回空", flush=True)
            except Exception as e:
                print(f"  [warn] 深交所 {sym} {seg_start}~{seg_end} 失败: {e}", flush=True)
            time.sleep(0.5)
        time.sleep(0.5)
    print(f"深交所回填完成，得到 {len(result)} 个日期", flush=True)
    return result


def fetch_nav_history_sina(code: str):
    """新浪获取指定 ETF 全历史 close，返回 {date_key: close}。"""
    try:
        df = ak.fund_etf_hist_sina(symbol=_sina_symbol(code))
        if df is not None and len(df) > 0:
            df = df.copy()
            df['date'] = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d")
            return code, dict(zip(df['date'], df['close']))
    except Exception:
        pass
    return code, {}


def build_target_nav_history(targets: list, trade_days: set) -> dict:
    """抓取所有标的历史净值，整理成 {date_key: {target: nav}}。
    注意：新浪接口内部用 mini_racer（JS 引擎），不是线程安全的，
    必须串行调用，否则会崩溃。"""
    print(f"抓取 {len(targets)} 个标的历史净值（新浪，串行）...", flush=True)
    target_codes = {t: TARGET_TO_CODE[t] for t in targets if t in TARGET_TO_CODE}

    code_nav_history = {}
    done = 0
    for t, code in target_codes.items():
        _, hist = fetch_nav_history_sina(code)
        code_nav_history[code] = hist
        done += 1
        if done % 5 == 0:
            print(f"  [{done}/{len(target_codes)}] 已抓取", flush=True)

    target_by_code = {v: k for k, v in target_codes.items()}
    nav_by_date = {}
    for code, hist in code_nav_history.items():
        target = target_by_code.get(code)
        if not target:
            continue
        for date_key, nav in hist.items():
            if date_key in trade_days:
                nav_by_date.setdefault(date_key, {})[target] = round(float(nav), 4)

    print(f"净值历史整理完成，覆盖 {len(nav_by_date)} 个交易日", flush=True)
    return nav_by_date


def _gen_trade_days(start_date: str, end_date: str) -> list:
    """生成 start~end 之间的工作日（YYYYMMDD 格式），跳过周末。"""
    start = datetime.datetime.strptime(start_date, "%Y%m%d")
    end = datetime.datetime.strptime(end_date, "%Y%m%d")
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur.strftime("%Y%m%d"))
        cur += datetime.timedelta(days=1)
    return days


def backfill_all(start_date: str, end_date: str, filepath: str = None):
    if filepath is None:
        filepath = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "docs", "data", "etf_shares.json"
        )

    print(f"开始回填历史数据: {start_date} ~ {end_date}")
    print("=" * 60)

    # 1. 先回填上交所（串行 + 间隔）—— 放在深交所之前，
    #    避免深交所接口的 JS 引擎状态影响上交所请求
    print("\n[1/4] 回填上交所数据（成分级份额）...")
    sse_data = backfill_sse(_gen_trade_days(start_date, end_date))

    # 2. 再回填深交所（区间一次拿全）
    print("\n[2/4] 回填深交所数据（成分级份额）...")
    szse_data = backfill_szse(start_date, end_date)

    # 3. 合并成分级份额
    all_days = sorted(set(list(szse_data.keys()) + list(sse_data.keys())))
    print(f"\n[3/4] 合并成分级份额，共 {len(all_days)} 个交易日")
    # code_shares_by_day = {date: {code: 份}}
    code_shares_by_day = {}
    for d in all_days:
        merged = {}
        if d in szse_data:
            merged.update(szse_data[d])
        if d in sse_data:
            for c, s in sse_data[d].items():
                merged[c] = merged.get(c, 0.0) + s
        code_shares_by_day[d] = merged

    # 4. 抓取所有成分 ETF 的历史净值（成分加权综合净值 + 成分级拆分校正）
    print("\n[4/4] 抓取成分净值 + 成分级拆分校正 + 加权聚合...")
    from weighted_nav import (
        fetch_all_component_nav, adjust_component_splits, aggregate_weighted,
    )

    all_codes = sorted(set(
        c for codes in TARGET_COMPONENT_CODES.values() for c in codes
    ))
    nav_cache = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nav_cache.json")
    nav_hist = fetch_all_component_nav(all_codes, cache_file=nav_cache)
    nav_adjusted, split_report = adjust_component_splits(nav_hist)

    if split_report:
        print(f"  检测到 {len(split_report)} 处成分拆分（成分级校正）:")
        for code, reg_dt, ex_dt, factor in split_report:
            print(f"    {code}: 登记日{reg_dt} 除权日{ex_dt} ×{factor}", flush=True)

    # 5. 逐日加权聚合
    print(f"\n逐日加权聚合（{len(all_days)} 个交易日）...")
    data = load_data(filepath)
    for date_key in all_days:
        day_code_shares = code_shares_by_day.get(date_key, {})
        merged = {}
        for target, codes in TARGET_COMPONENT_CODES.items():
            # 该标的所有成分在该日的份额
            shares_by_code = {c: day_code_shares.get(c, 0.0) for c in codes}
            # 该日各成分的校正后净值
            nav_by_code = {c: nav_adjusted.get(c, {}).get(date_key) for c in codes}
            # 加权聚合
            rec = aggregate_weighted(codes, shares_by_code, nav_by_code)
            merged[target] = rec
        data["records"][date_key] = merged

    # 6. 排序 + 保存
    data["records"] = dict(sorted(data["records"].items()))
    data["last_update"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    save_data(filepath, data)
    print(f"\n回填完成！总日期数: {len(data['records'])}")
    print(f"数据已保存到 {filepath}")


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        start = sys.argv[1]
        end = sys.argv[2]
    else:
        start = "20260709"
        end = "20261027"
        print(f"未指定日期，默认回填停更区间: {start} ~ {end}")

    backfill_all(start, end)
