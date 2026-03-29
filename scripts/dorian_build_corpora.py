#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import glob
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import yaml


# ============================================================
# CONFIG
# ============================================================

def load_config(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================================
# HELPERS
# ============================================================

def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.replace("•", " - ").replace("·", " - ").replace("–", " - ")
    text = re.sub(r"\s+", " ", text.strip())
    return text


def sent_tokenize_ish(text: str) -> list[str]:
    parts = re.split(r"(?<=[\.\?\!\:\;])\s+|\n+", text)
    return [p.strip() for p in parts if p and p.strip()]


def chunk_text_sentence_aware(
    text: str,
    max_words: int,
    overlap_words: int,
    min_chunk_words: int,
) -> list[str]:
    text = clean_text(text)
    if not text:
        return []

    sents = sent_tokenize_ish(text)
    if not sents:
        return []

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for sent in sents:
        words = sent.split()
        if not words:
            continue

        if current_len + len(words) <= max_words or current_len == 0:
            current.append(sent)
            current_len += len(words)
        else:
            chunk = " ".join(current).strip()
            if chunk:
                chunks.append(chunk)

            back = " ".join(chunk.split()[-overlap_words:]) if overlap_words > 0 and chunks else ""
            current = [x for x in [back, sent] if x]
            current_len = sum(len(x.split()) for x in current)

    if current:
        chunk = " ".join(current).strip()
        if chunk:
            chunks.append(chunk)

    out = []
    for c in chunks:
        wc = len(c.split())
        if wc >= min_chunk_words or wc >= max(8, min_chunk_words // 2):
            out.append(c)
    return out


def pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def excel_file_safely(path: str | Path) -> Optional[pd.ExcelFile]:
    try:
        return pd.ExcelFile(path)
    except Exception:
        return None


def parse_any_datetime(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    if dt.notna().sum() > 0:
        return dt

    num = pd.to_numeric(series, errors="coerce")
    if num.notna().sum() == 0:
        return dt

    med = num.dropna().median()
    if med > 1e11:
        return pd.to_datetime(num, unit="ms", errors="coerce", utc=True)
    if med > 1e8:
        return pd.to_datetime(num, unit="s", errors="coerce", utc=True)
    if 20000 < med < 90000:
        return pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce", utc=True)
    return dt


def canon_fund(name: str, cfg: dict) -> str:
    if not name:
        return "Unknown"
    s = str(name).strip()
    if s in cfg["funds"]:
        return s
    low = s.lower()
    if low in cfg["fund_aliases"]:
        return cfg["fund_aliases"][low]
    for k, v in cfg["fund_aliases"].items():
        if k in low:
            return v
    return "Unknown"


def fund_to_path(fund: str, cfg: dict) -> str:
    return cfg["fund_path_fix"].get(fund, fund.replace(" ", ""))


def latest_by_mtime(paths: list[str]) -> Optional[str]:
    if not paths:
        return None
    return sorted(set(paths), key=os.path.getmtime)[-1]


def ensure_run_dir(base_runs_dir: Path, fund: str, ts: str, cfg: dict) -> Path:
    out = base_runs_dir / fund_to_path(fund, cfg) / ts
    out.mkdir(parents=True, exist_ok=True)
    return out


def safe_numeric(val, default=0.0) -> float:
    try:
        x = float(val)
        if pd.isna(x):
            return default
        return x
    except Exception:
        return default


# ============================================================
# BRAND
# ============================================================

def discover_brand_files(brand_input_dir: Path) -> list[str]:
    return sorted(glob.glob(str(brand_input_dir / "*.xlsx")))


def brand_fund_from_filename(path: Path, cfg: dict) -> str:
    stem = path.stem.lower()
    for alias, fund in cfg["fund_aliases"].items():
        if alias in stem:
            return fund
    return "Unknown"


def load_brand_rows(path: str, cfg: dict) -> tuple[str, list[dict]]:
    p = Path(path)
    fund = brand_fund_from_filename(p, cfg)

    xl = excel_file_safely(path)
    if xl is None or not xl.sheet_names:
        return fund, []

    try:
        df = xl.parse(xl.sheet_names[0])
    except Exception:
        return fund, []

    if df is None or df.empty:
        return fund, []

    text_col = pick_col(df, ["text", "full_text", "content", "body", "copy"])
    if text_col is None:
        return fund, []

    title_col = pick_col(df, ["title"])
    url_col = pick_col(df, ["url", "url_or_source", "link"])
    date_col = pick_col(df, ["date"])
    source_type_col = pick_col(df, ["source_type"])

    rows = []
    for _, row in df.iterrows():
        text = clean_text(str(row.get(text_col, "") or ""))
        if not text:
            continue

        rows.append({
            "fund": fund,
            "title": str(row.get(title_col, "") or "") if title_col else "",
            "url": str(row.get(url_col, "") or "") if url_col else "",
            "date": row.get(date_col, "") if date_col else "",
            "source_type": str(row.get(source_type_col, "brand") or "brand"),
            "text": text,
            "weight": 1.0,
        })

    if not rows:
        return fund, []

    out_df = pd.DataFrame(rows).drop_duplicates(subset=["text"]).reset_index(drop=True)
    return fund, out_df.to_dict(orient="records")


def build_brand_chunks(rows: list[dict], fund: str, ts: str, cfg: dict) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    c = cfg["chunking"]
    out = []

    for i, row in enumerate(rows):
        chunks = chunk_text_sentence_aware(
            row["text"],
            max_words=c["max_words"],
            overlap_words=c["overlap_words"],
            min_chunk_words=c["min_chunk_words"],
        )
        for j, ch in enumerate(chunks):
            out.append({
                "fund": fund,
                "type": "brand",
                "row_id": i,
                "chunk_index": j,
                "chunk_id": f"{fund_to_path(fund, cfg)}_brand_{ts}_{i}_{j}",
                "title": row.get("title", ""),
                "url": row.get("url", ""),
                "date": row.get("date", ""),
                "source_type": row.get("source_type", "brand"),
                "weight": row.get("weight", 1.0),
                "text": ch,
                "chunk_length": len(ch.split()),
            })

    return pd.DataFrame(out)


# ============================================================
# NEWS / QWASS
# ============================================================

def discover_ultra_qwass(qwass_roots: list[str]) -> Optional[str]:
    patterns = []
    for root in qwass_roots:
        patterns.extend([
            str(Path(root) / "**" / "ULTRA.QWASS*.xlsx"),
            str(Path(root) / "**" / "ULTRA.Qwass*.xlsx"),
        ])

    hits = []
    for pat in patterns:
        hits.extend(glob.glob(pat, recursive=True))
    return latest_by_mtime(hits)


def load_news_rows_from_ultra(path: str, fund: str, cfg: dict) -> list[dict]:
    out = []
    xl = excel_file_safely(path)
    if xl is None:
        return out

    target_sheets = [s for s in xl.sheet_names if canon_fund(s, cfg) == fund]
    sheets = target_sheets if target_sheets else xl.sheet_names

    for sheet in sheets:
        try:
            df = xl.parse(sheet)
        except Exception:
            continue
        if df is None or df.empty:
            continue

        title_col = pick_col(df, ["title", "headline"])
        summ_col = pick_col(df, ["summary", "abstract", "deck"])
        text_col = pick_col(df, ["text", "full_text", "content", "body"])
        date_col = pick_col(df, ["date", "date_published", "pub_date"])
        url_col = pick_col(df, ["url", "link"])
        pre_col = pick_col(df, ["precheck"])
        qual_col = pick_col(df, ["quality_weight", "quality", "quality_score"])

        if not (title_col or summ_col or text_col):
            continue

        for _, row in df.iterrows():
            title = clean_text(str(row.get(title_col, "") or "")) if title_col else ""
            body = clean_text(str(row.get(summ_col, "") or row.get(text_col, "") or ""))
            if not (title or body):
                continue

            out.append({
                "fund": fund,
                "title": title,
                "text": (title + "\n\n" + body).strip(),
                "date": row.get(date_col, "") if date_col else "",
                "url": row.get(url_col, "") if url_col else "",
                "source_type": "news",
                "subreddit": "",
                "rating": "",
                "precheck": row.get(pre_col, "") if pre_col else "",
                "quality_weight": row.get(qual_col, 1.0) if qual_col else 1.0,
            })

    return out


# ============================================================
# SCUM / REDDIT
# ============================================================

def find_latest_scum_extract(fund: str, cfg: dict, scum_roots: list[str]) -> Optional[str]:
    pkg = cfg["scum_package_dir"].get(fund)
    if not pkg:
        return None

    hits = []
    for root in scum_roots:
        pkg_dir = Path(root) / pkg
        if not pkg_dir.exists():
            continue
        hits.extend(glob.glob(str(pkg_dir / "*.SCUM.DorianExtract_*.xlsx")))

    return latest_by_mtime(hits)


def load_scum_rows(fund: str, cfg: dict, scum_roots: list[str]) -> list[dict]:
    path = find_latest_scum_extract(fund, cfg, scum_roots)
    if not path:
        return []

    xl = excel_file_safely(path)
    if xl is None:
        return []

    out = []

    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet)
        except Exception:
            continue
        if df is None or df.empty:
            continue

        label_col = pick_col(df, ["dorian_subject_label"])
        snippet_col = pick_col(df, ["dorian_snippets_for_firm"])
        title_col = pick_col(df, ["title"])
        date_col = pick_col(df, ["created_utc", "date", "created_at"])
        permalink_col = pick_col(df, ["permalink", "url"])
        subreddit_col = pick_col(df, ["subreddit"])

        if not label_col or not snippet_col:
            continue

        for _, row in df.iterrows():
            label = str(row.get(label_col, "") or "").strip().upper()
            if label not in {"CENTRAL", "PERIPHERAL"}:
                continue

            snippet = clean_text(str(row.get(snippet_col, "") or ""))
            if not snippet:
                continue

            permalink = str(row.get(permalink_col, "") or "") if permalink_col else ""
            url = permalink
            if permalink and permalink.startswith("/r/"):
                url = f"https://www.reddit.com{permalink}"

            out.append({
                "fund": fund,
                "title": clean_text(str(row.get(title_col, "") or "")) if title_col else "",
                "text": snippet,
                "date": row.get(date_col, "") if date_col else "",
                "url": url,
                "source_type": "reddit",
                "subreddit": row.get(subreddit_col, "") if subreddit_col else "",
                "rating": "",
                "precheck": label,
                "quality_weight": 1.0,
            })

    return out


# ============================================================
# WERK / GLASSDOOR
# ============================================================

def discover_werk_files(werk_roots: list[str]) -> list[str]:
    hits = []
    for root in werk_roots:
        hits.extend(glob.glob(str(Path(root) / "**" / "*.xlsx"), recursive=True))
    hits = sorted(set(hits), key=os.path.getmtime, reverse=True)
    return hits


def werk_fund_from_filename(path: Path, cfg: dict) -> str:
    stem = path.stem.lower().replace("-", "_")
    for alias, fund in cfg["fund_aliases"].items():
        if alias in stem:
            return fund
    return "Unknown"


def load_werk_rows(fund: str, cfg: dict, werk_roots: list[str]) -> list[dict]:
    rows = []

    for path in discover_werk_files(werk_roots):
        if werk_fund_from_filename(Path(path), cfg) != fund:
            continue

        xl = excel_file_safely(path)
        if xl is None:
            continue

        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet)
            except Exception:
                continue
            if df is None or df.empty:
                continue

            cols = {c.lower(): c for c in df.columns}
            text_parts = [cols[c] for c in ["review_text", "text", "pros", "cons", "headline", "title", "review"] if c in cols]
            if not text_parts:
                continue

            date_col = cols.get("date")
            url_col = cols.get("url")
            rating_col = cols.get("rating")
            title_col = cols.get("headline") or cols.get("title")

            for _, row in df.iterrows():
                pieces = [str(row.get(c, "") or "").strip() for c in text_parts]
                body = clean_text(" ".join([p for p in pieces if p]))
                title = str(row.get(title_col, "") or "").strip() if title_col else ""
                if not body and not title:
                    continue

                rows.append({
                    "fund": fund,
                    "title": title,
                    "text": (title + "\n\n" + body).strip(),
                    "date": row.get(date_col, "") if date_col else "",
                    "url": row.get(url_col, "") if url_col else "",
                    "source_type": "glassdoor",
                    "subreddit": "",
                    "rating": row.get(rating_col, "") if rating_col else "",
                    "precheck": "CENTRAL",
                    "quality_weight": 1.0,
                })

    return rows


# ============================================================
# REPUTATION
# ============================================================

def apply_reputation_filters(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    df["text"] = df["text"].astype(str).map(clean_text)
    df = df[df["text"].str.len() > 0].drop_duplicates(subset=["text"]).reset_index(drop=True)

    months = cfg["filters"]["reputation_months"]
    news_quality_cutoff = cfg["filters"]["news_quality_cutoff"]

    df["date_parsed"] = parse_any_datetime(df["date"])
    cutoff = pd.Timestamp.utcnow() - pd.DateOffset(months=months)
    df = df[df["date_parsed"].isna() | (df["date_parsed"] >= cutoff)].reset_index(drop=True)

    df["quality_weight"] = pd.to_numeric(df.get("quality_weight", 1.0), errors="coerce")
    is_news = df["source_type"].str.lower().eq("news")

    df.loc[~is_news & df["quality_weight"].isna(), "quality_weight"] = 1.0
    df.loc[is_news & df["quality_weight"].isna(), "quality_weight"] = 0.0

    df = df[~is_news | (df["quality_weight"] >= news_quality_cutoff)].reset_index(drop=True)

    def subject_weight(row) -> float:
        src = str(row["source_type"]).lower()
        pre = str(row.get("precheck", "") or "").strip().upper()

        if src == "glassdoor":
            return 1.0
        if src == "news":
            if pre == "CENTRAL":
                return 1.0
            if pre == "PERIPHERAL":
                return 0.3
            return 0.0
        if src == "reddit":
            if pre == "CENTRAL":
                return 1.0
            if pre == "PERIPHERAL":
                return 0.7
            return 0.0
        return 0.0

    df["precheck_weight"] = df.apply(subject_weight, axis=1)

    is_news_or_reddit = df["source_type"].str.lower().isin(["news", "reddit"])
    df = df[~is_news_or_reddit | (df["precheck_weight"] > 0)].reset_index(drop=True)

    source_base = cfg["weights"]["source_base"]

    def base_w(src: str) -> float:
        return safe_numeric(source_base.get(str(src).lower(), 1.0), 1.0)

    df["source_base_weight"] = df["source_type"].map(base_w).fillna(1.0)
    df["rep_weight"] = (
        df["source_base_weight"]
        * df["precheck_weight"]
        * df["quality_weight"].fillna(1.0)
    )

    return df.reset_index(drop=True)


def build_reputation_chunks(rows: list[dict], fund: str, ts: str, cfg: dict) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = apply_reputation_filters(df, cfg)

    if df.empty:
        return df

    c = cfg["chunking"]
    out = []

    for i, row in df.iterrows():
        chunks = chunk_text_sentence_aware(
            row["text"],
            max_words=c["max_words"],
            overlap_words=c["overlap_words"],
            min_chunk_words=c["min_chunk_words"],
        )

        for j, ch in enumerate(chunks):
            out.append({
                "fund": fund,
                "type": "reputation",
                "row_id": i,
                "chunk_index": j,
                "chunk_id": f"{fund_to_path(fund, cfg)}_rep_{ts}_{i}_{j}",
                "title": row.get("title", ""),
                "url": row.get("url", ""),
                "date": row.get("date", ""),
                "source_type": row.get("source_type", ""),
                "subreddit": row.get("subreddit", ""),
                "rating": row.get("rating", ""),
                "precheck": row.get("precheck", ""),
                "precheck_weight": row.get("precheck_weight", 1.0),
                "quality_weight": row.get("quality_weight", 1.0),
                "source_base_weight": row.get("source_base_weight", 1.0),
                "rep_weight": row.get("rep_weight", 1.0),
                "text": ch,
                "chunk_length": len(ch.split()),
            })

    return pd.DataFrame(out)


# ============================================================
# MAIN
# ============================================================

def build_corpora(cfg: dict, selected_funds: Optional[list[str]] = None, timestamp: Optional[str] = None) -> None:
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M")

    brand_input_dir = Path(cfg["paths"]["brand_input_dir"])
    dorian_runs_dir = Path(cfg["paths"]["dorian_runs_dir"])
    qwass_roots = cfg["paths"]["qwass_roots"]
    scum_roots = cfg["paths"]["scum_roots"]
    werk_roots = cfg["paths"]["werk_roots"]

    funds = selected_funds if selected_funds else cfg["funds"]

    brand_files = discover_brand_files(brand_input_dir)
    brand_by_fund: dict[str, list[dict]] = defaultdict(list)

    for path in brand_files:
        fund, rows = load_brand_rows(path, cfg)
        if fund != "Unknown" and rows:
            brand_by_fund[fund].extend(rows)

    ultra_qwass = discover_ultra_qwass(qwass_roots)
    if ultra_qwass:
        print(f"[QWASS] Using workbook: {ultra_qwass}")
    else:
        print("[QWASS] No ULTRA.QWASS workbook found. News will be skipped.")

    for fund in funds:
        print(f"\n=== {fund} ===")
        run_dir = ensure_run_dir(dorian_runs_dir, fund, ts, cfg)
        tag = fund_to_path(fund, cfg)

        # BRAND
        brand_rows = brand_by_fund.get(fund, [])
        brand_df = build_brand_chunks(brand_rows, fund, ts, cfg)
        if not brand_df.empty:
            brand_csv = run_dir / f"{tag}_brand_chunks_{ts}.csv"
            brand_xlsx = run_dir / f"{tag}_brand_chunks_{ts}.xlsx"
            brand_df.to_csv(brand_csv, index=False)
            brand_df.to_excel(brand_xlsx, index=False)
            print(f"[OK] brand chunks: {len(brand_df)}")
        else:
            print("[INFO] no brand corpus found or no usable brand chunks.")

        # REPUTATION
        rep_rows = []
        if ultra_qwass:
            rep_rows.extend(load_news_rows_from_ultra(ultra_qwass, fund, cfg))
        rep_rows.extend(load_scum_rows(fund, cfg, scum_roots))
        rep_rows.extend(load_werk_rows(fund, cfg, werk_roots))

        rep_df = build_reputation_chunks(rep_rows, fund, ts, cfg)
        if not rep_df.empty:
            rep_csv = run_dir / f"{tag}_reputation_chunks_{ts}.csv"
            rep_xlsx = run_dir / f"{tag}_reputation_chunks_{ts}.xlsx"
            rep_df.to_csv(rep_csv, index=False)
            rep_df.to_excel(rep_xlsx, index=False)
            print(f"[OK] reputation chunks: {len(rep_df)}")
        else:
            print("[INFO] no usable reputation rows/chunks.")

    print("\nDone. Brand + reputation corpora built.")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build DORIAN brand + reputation corpora.")
    ap.add_argument(
        "--config",
        required=True,
        help="Path to dorian_config.yaml",
    )
    ap.add_argument(
        "--funds",
        nargs="*",
        default=None,
        help="Optional subset of funds to run",
    )
    ap.add_argument(
        "--timestamp",
        default=None,
        help="Optional fixed timestamp like 20260329_2315",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.config)
    build_corpora(config, selected_funds=args.funds, timestamp=args.timestamp)
