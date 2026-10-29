#!/usr/bin/env python3
"""
成分加权综合净值 + 成分级拆分校正（彻底版核心模块）
====================================================
解决 v3 的残留问题：聚合标的（科创半导体/科创芯片/芯片）拆分日
仍有 -19%~-24% 负跳变。根因是 v3 用「单只代表 ETF」的净值判断拆分
系数，但聚合标的只有部分成分拆分，代表 ETF 的拆分倍数 ≠ 标的整体
份额跳变倍数，导致 aum 对不齐。

正确口径（本模块）：
  1. 净值 = 成分加权综合净值 = Σ(成分份额 × 成分净值) / Σ成分份额
     —— 不再用单只代表 ETF，而是该指数全部成分 ETF 的加权
  2. 拆分校正下沉到成分级：对每只成分 ETF 独立检测净值腰斩点，
     修正其拆分登记日（份额翻倍日）的净值（÷拆分倍数）
  3. aum = Σ(成分份额 × 校正后成分净值)

这样拆分时各成分独立校正，聚合后标的 aum 天然连续，
彻底消除 -19%~-24% 负跳变。

用法：
  作为库被 backfill_data.py / fetch_data.py 导入，
  核心函数：
    fetch_all_component_nav(codes) -> {code: {date: nav}}
    adjust_component_splits(nav_hist) -> {code: {date: nav(校正后)}}
    aggregate_weighted(day, shares_by_code, nav_adjusted, codes) -> (shares, nav, aum)
"""

import json
import os
import time
import datetime

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


def _sina_symbol(code: str) -> str:
    code = str(code).strip()
    if code.startswith(("5", "6", "9")):
        return "sh" + code
    return "sz" + code


def fetch_all_component_nav(codes: list, cache_file: str = None) -> dict:
    """抓取所有成分 ETF 的新浪历史净值，返回 {code: {date(YYYY-MM-DD): close}}。
    新浪接口内部 mini_racer 非线程安全，必须串行。带磁盘缓存续跑。"""
    nav_hist = {}
    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                nav_hist = json.load(f)
            print(f"  从缓存加载 {len(nav_hist)} 只成分净值", flush=True)
        except Exception:
            nav_hist = {}

    done = 0
    for code in codes:
        if code in nav_hist:
            done += 1
            continue
        try:
            df = ak.fund_etf_hist_sina(symbol=_sina_symbol(code))
            if df is not None and len(df) > 0:
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                nav_hist[code] = {k: float(v) for k, v in zip(df["date"], df["close"]) if pd.notna(v)}
            else:
                nav_hist[code] = {}
        except Exception:
            nav_hist[code] = {}
        done += 1
        time.sleep(0.2)
        if done % 50 == 0:
            print(f"  成分净值进度 {done}/{len(codes)}", flush=True)
            if cache_file:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(nav_hist, f, ensure_ascii=False)

    if cache_file:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(nav_hist, f, ensure_ascii=False)

    print(f"  成分净值抓取完成，共 {len(nav_hist)} 只", flush=True)
    return nav_hist


def adjust_component_splits(nav_hist: dict) -> dict:
    """成分级拆分校正：对每只成分独立检测净值腰斩点，修正登记日净值。
    返回校正后的 {code: {date: nav}}。

    检测：nav 单日比值 < 0.6 视为拆分除权（除权日）。
    修正：拆分登记日（除权日前一交易日）的净值 ÷ 拆分倍数（= prev/current）。"""
    adjusted = {}
    report = []
    for code, seq in nav_hist.items():
        if not seq:
            adjusted[code] = {}
            continue
        sdates = sorted(seq.keys())
        new_seq = dict(seq)
        prev_nav = None
        prev_dt = None
        for dt in sdates:
            nv = seq[dt]
            if prev_nav is not None and prev_nav > 0 and nv > 0:
                ratio = nv / prev_nav
                if ratio < 0.6:
                    factor = prev_nav / nv
                    if prev_dt in new_seq:
                        new_seq[prev_dt] = round(seq[prev_dt] / factor, 6)
                        report.append((code, prev_dt, dt, round(factor, 2)))
            prev_nav = nv
            prev_dt = dt
        adjusted[code] = new_seq
    return adjusted, report


def aggregate_weighted(codes: list, shares_by_code: dict, nav_adjusted: dict) -> dict:
    """按成分加权聚合单个标的，返回 {shares(亿份), nav, aum(亿元)}。

    shares_by_code: {code: shares(份)}
    nav_adjusted:   {code: {date: nav}}
    注意：本函数按「单个交易日」聚合，date 由调用方保证。"""
    total_shares = 0.0
    nav_num = 0.0  # Σ(份额 × 净值)
    nav_den = 0.0  # Σ份额（有净值部分）

    for code in codes:
        sh = shares_by_code.get(code, 0.0)
        if sh is None or sh <= 0:
            sh = 0.0
        total_shares += sh

        # nav_adjusted[code] 是单日净值（由调用方传入对应日期的值）
        nv = nav_adjusted.get(code)
        if nv is not None and nv > 0:
            nav_num += sh * nv
            nav_den += sh

    nav = (nav_num / nav_den) if nav_den > 0 else None
    aum = nav_num  # 单位：份 × 元 = 元

    return {
        "shares": round(total_shares / 1e8, 4),
        "nav": round(nav, 4) if nav is not None else None,
        "aum": round(aum / 1e8, 2),
    }


if __name__ == "__main__":
    # 自测：抓取科创半导体 3 成分净值，展示拆分校正
    from target_components import TARGET_COMPONENT_CODES
    codes = TARGET_COMPONENT_CODES["科创半导体"]
    print("科创半导体成分:", codes)
    nav = fetch_all_component_nav(codes)
    adj, report = adjust_component_splits(nav)
    print("拆分校正报告:")
    for r in report:
        print(f"  {r[0]}: 登记日{r[1]} 除权日{r[2]} ×{r[3]}")
