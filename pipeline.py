"""
pipeline.py — ML Stock Rank
============================
Ne télécharge RIEN de ce qui est déjà en DB.
Ne recalcule PAS les features si la table SQL est à jour.
Ne retente PAS les tickers qui n'ont pas de données sur Yahoo.

Les features sont stockées dans la table 'features' de chaque DB SQLite
(plus de fichiers .parquet).

Usage :
    from pipeline import run_all, status, read_features
    run_all()                              # tout (skip ce qui est fait)
    run_all("spx")                         # un seul index
    run_all(skip_fundamentals=True)        # sans fondamentaux
    run_all(force_features=True)           # force recalcul features

    status()                               # diagnostic de couverture

    # Lire les features dans le notebook :
    df_cac = read_features("cac40")
    df_spx = read_features("spx", start="2023-01-01")

    # Étape par étape :
    update_prices("all")
    update_fundamentals("all")             # résumable si rate-limit
    compute_features("all")

    # Reset si besoin de tout retélécharger :
    reset_fundamentals("all")              # vide financials + log
    reset_earnings("all")                  # vide earnings + log
"""

from __future__ import annotations

import argparse
import sqlite3
import time
import datetime as dt
import os
import urllib.request
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
START_DEFAULT = "2000-01-01"

INDEX_CONFIG = {
    "spx": {
        "label": "S&P 500",
        "wiki_url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "ticker_col": "Symbol",
        "proxy": "SPY",
        "db": "sp500_data.db",
        "alt_db": "spx_data.db",
        "market_ticker": "^GSPC",
        "dot_to_dash": True,
        "model_start": "2022-01-01",  # date de début modèle — _load_px charge à partir de model_start - 280j
        "macro_vix": "^VIX",           # CBOE VIX
        "macro_yield_10y": "^TNX",     # 10-Year Treasury Yield
        "macro_yield_3m": "^IRX",      # 13-Week T-Bill Yield
    },
    "cac40": {
        "label": "CAC 40",
        "wiki_url": "https://en.wikipedia.org/wiki/CAC_40",
        "ticker_col": "Ticker",
        "proxy": "^FCHI",
        "db": "cac40_data.db",
        "market_ticker": "^FCHI",
        "dot_to_dash": False,
        "model_start": "2022-01-01",
        "macro_vix": "^VIX",           # US VIX (proxy global — ^V2X cassé sur YF)
        "macro_yield_10y": "^TNX",     # US 10Y (proxy global)
        "macro_yield_3m": "^IRX",      # US 3M (proxy global)
    },
    "dax": {
        "label": "DAX",
        "wiki_url": "https://en.wikipedia.org/wiki/DAX",
        "ticker_col": "Ticker",
        "proxy": "^GDAXI",
        "db": "dax_data.db",
        "market_ticker": "^GDAXI",
        "dot_to_dash": False,
        "model_start": "2022-01-01",
        "macro_vix": "^VIX",           # US VIX (proxy global — ^V2X cassé sur YF)
        "macro_yield_10y": "^TNX",     # US 10Y (proxy global)
        "macro_yield_3m": "^IRX",      # US 3M (proxy global)
    },
}

FUNDAMENTAL_FEATURE_COLS = [
    "eps_surprise", "eps_surprise_abs", "eps_beat",
    "eps_growth_qoq", "eps_surprise_ma4", "consecutive_beats",
    "revenue_growth_qoq", "revenue_growth_yoy", "net_margin",
    "debt_to_equity", "roa", "ocf_to_revenue",
]

MACRO_FEATURE_COLS = [
    "macro_vix", "macro_vix_chg_20", "macro_term_spread",
]


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

TEMPORAL_CANDIDATES = ["mom_120", "rv_60", "dollar_volume", "beta_60", "slope_logp_60"]
TEMPORAL_WINDOWS    = (5, 10)

def _resolve(index: str) -> list[str]:
    if index == "all":
        return list(INDEX_CONFIG)
    if index not in INDEX_CONFIG:
        raise ValueError(f"Unknown: {index}. Use {list(INDEX_CONFIG)} or 'all'")
    return [index]


def _db_path(key: str) -> Path:
    cfg = INDEX_CONFIG[key]
    p = BASE_DIR / cfg["db"]
    if p.exists():
        return p
    alt = cfg.get("alt_db")
    if alt and (BASE_DIR / alt).exists():
        return BASE_DIR / alt
    return p


def _conn(path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(path)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def _strip_tz(ts):
    if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
        return ts.tz_localize(None) if hasattr(ts, 'tz_localize') else ts.replace(tzinfo=None)
    return ts


def _tbl_exists(path: Path, tbl: str) -> bool:
    c = sqlite3.connect(path)
    r = c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (tbl,)).fetchone()[0]
    c.close()
    return r > 0


def _count(path: Path, tbl: str) -> int:
    if not _tbl_exists(path, tbl): return 0
    c = sqlite3.connect(path)
    n = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    c.close()
    return n


def _tickers_in_db(path: Path, tbl: str) -> set[str]:
    """Tickers qui ont effectivement des données dans la table."""
    if not _tbl_exists(path, tbl): return set()
    c = sqlite3.connect(path)
    s = {r[0] for r in c.execute(f"SELECT DISTINCT Ticker FROM {tbl}").fetchall()}
    c.close()
    return s


def _tickers_attempted(path: Path, tbl: str) -> set[str]:
    """Tickers avec données OU déjà tentés (loggés même si 0 rows).
    Évite de re-fetcher les tickers européens sans données Yahoo."""
    have_data = _tickers_in_db(path, tbl)
    if not _tbl_exists(path, "fetch_log"):
        return have_data
    c = sqlite3.connect(path)
    logged = {r[0] for r in c.execute(
        "SELECT Ticker FROM fetch_log WHERE TableName=?", (tbl,)).fetchall()}
    c.close()
    return have_data | logged


def _log_fetch(db: Path, ticker: str, tbl: str, row_count: int,
               conn: sqlite3.Connection | None = None):
    """Enregistre qu'on a tenté ce ticker (même si 0 rows).
    Si conn est fournie, l'utilise directement (pas de commit ni close).
    Sinon ouvre une connexion temporaire.
    """
    own = conn is None
    if own:
        conn = sqlite3.connect(db)
    conn.execute("INSERT OR REPLACE INTO fetch_log VALUES (?,?,?,?)",
                 (ticker, tbl, dt.date.today().strftime("%Y-%m-%d"), row_count))
    if own:
        conn.commit()
        conn.close()


def _max_date(path: Path, tbl: str = "prices") -> dt.date | None:
    if not _tbl_exists(path, tbl): return None
    c = sqlite3.connect(path)
    r = c.execute(f"SELECT MAX(Date) FROM {tbl}").fetchone()
    c.close()
    if not r or not r[0]: return None
    return pd.to_datetime(r[0]).date()


def _db_mtime(path: Path) -> float:
    if not path.exists(): return 0.0
    return os.path.getmtime(path)



def _last_trading_day(proxy: str) -> dt.date:
    try:
        px = yf.download(proxy, period="14d", interval="1d",
                         auto_adjust=False, progress=False, threads=False)
        if px is not None and not px.empty:
            return pd.to_datetime(px.index.max()).date()
    except Exception:
        pass
    d = dt.date.today()
    wd = d.weekday()
    if wd == 5: return d - dt.timedelta(days=1)
    if wd == 6: return d - dt.timedelta(days=2)
    return d


# ═══════════════════════════════════════════════════════════════════════════════
# TICKERS (Wikipedia, cached per session)
# ═══════════════════════════════════════════════════════════════════════════════

_TCACHE: dict[str, list[str]] = {}


def get_tickers(key: str) -> list[str]:
    if key in _TCACHE:
        return _TCACHE[key]

    cfg = INDEX_CONFIG[key]
    req = urllib.request.Request(cfg["wiki_url"], headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        tables = pd.read_html(resp.read())

    tcol = cfg["ticker_col"]
    chosen = None
    for tbl in tables:
        cols = [str(c).strip() for c in tbl.columns]
        if tcol in cols:
            chosen = tbl; break
        for c in cols:
            if "ticker" in c.lower() or "symbol" in c.lower():
                tcol = c; chosen = tbl; break
        if chosen is not None:
            break
    if chosen is None:
        raise RuntimeError(f"No ticker column on {cfg['wiki_url']}")

    tks = [t.strip() for t in chosen[tcol].astype(str) if t.strip().lower() != "nan"]
    if cfg.get("dot_to_dash"):
        tks = [t.replace(".", "-") for t in tks]
    else:
        def _n(t):
            if "." not in t: return t
            l, r = t.rsplit(".", 1)
            return t if 2 <= len(r) <= 3 else f"{l}-{r}"
        tks = [_n(t) for t in tks]

    seen = set()
    out = [t for t in tks if not (t in seen or seen.add(t))]
    _TCACHE[key] = out
    print(f"  {len(out)} tickers ({cfg['label']})")
    return out


def _tickers_from_db(key: str) -> list[str] | None:
    """Tickers depuis la DB (évite l'appel Wikipedia)."""
    db = _db_path(key)
    if not db.exists(): return None
    tks = _tickers_in_db(db, "prices")
    cfg = INDEX_CONFIG[key]
    mkt = cfg.get("market_ticker", "")
    tks = {t for t in tks if t != mkt and not t.startswith("^")}
    if len(tks) < 10: return None
    return sorted(tks)


# ═══════════════════════════════════════════════════════════════════════════════
# INIT DB
# ═══════════════════════════════════════════════════════════════════════════════

def _init_db(path: Path):
    c = _conn(path)
    c.executescript("""
        CREATE TABLE IF NOT EXISTS prices (
            Date TEXT NOT NULL, Ticker TEXT NOT NULL,
            Open REAL, High REAL, Low REAL, Close REAL, AdjClose REAL, Volume REAL,
            PRIMARY KEY (Date, Ticker));
        CREATE INDEX IF NOT EXISTS ix_p ON prices (Ticker, Date);

        CREATE TABLE IF NOT EXISTS returns (
            Date TEXT NOT NULL, Ticker TEXT NOT NULL,
            AdjClose REAL, Return REAL, LogReturn REAL,
            PRIMARY KEY (Date, Ticker));
        CREATE INDEX IF NOT EXISTS ix_r ON returns (Ticker, Date);

        CREATE TABLE IF NOT EXISTS ratios (
            Ticker TEXT PRIMARY KEY,
            DailyMeanReturn REAL, DailyVol REAL,
            AnnualizedReturn REAL, AnnualizedVol REAL,
            Sharpe REAL, MaxDrawdown REAL, NbObs INTEGER);

        CREATE TABLE IF NOT EXISTS earnings_history (
            Ticker TEXT NOT NULL, ReportDate TEXT NOT NULL,
            FiscalEnd TEXT, EPS_actual REAL, EPS_estimate REAL,
            Surprise REAL, SurpriseAbs REAL,
            PRIMARY KEY (Ticker, ReportDate));
        CREATE INDEX IF NOT EXISTS ix_e ON earnings_history (Ticker, ReportDate);

        CREATE TABLE IF NOT EXISTS quarterly_financials (
            Ticker TEXT NOT NULL, ReportDate TEXT NOT NULL,
            FiscalEnd TEXT, TotalRevenue REAL, NetIncome REAL,
            TotalAssets REAL, TotalDebt REAL, BookValue REAL, OperatingCashFlow REAL,
            PRIMARY KEY (Ticker, ReportDate));
        CREATE INDEX IF NOT EXISTS ix_q ON quarterly_financials (Ticker, ReportDate);

        CREATE TABLE IF NOT EXISTS fetch_log (
            Ticker TEXT NOT NULL, TableName TEXT NOT NULL,
            FetchDate TEXT NOT NULL, RowCount INTEGER DEFAULT 0,
            PRIMARY KEY (Ticker, TableName));

        CREATE TABLE IF NOT EXISTS features_meta (
            Key TEXT PRIMARY KEY,
            Value TEXT);

        CREATE TABLE IF NOT EXISTS yf_prices (
            Date TEXT NOT NULL, Ticker TEXT NOT NULL,
            Open REAL, High REAL, Low REAL, Close REAL, Volume REAL,
            PRIMARY KEY (Date, Ticker));
        CREATE INDEX IF NOT EXISTS ix_yf ON yf_prices (Ticker, Date);

        CREATE TABLE IF NOT EXISTS macro_prices (
            Date TEXT NOT NULL, Ticker TEXT NOT NULL,
            Close REAL,
            PRIMARY KEY (Date, Ticker));
        CREATE INDEX IF NOT EXISTS ix_macro ON macro_prices (Date);
    """)
    c.commit()
    c.close()


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — PRICES (incrémental)
# ═══════════════════════════════════════════════════════════════════════════════

def _dl_prices(tickers, start: dt.date, end: dt.date) -> pd.DataFrame:
    if start > end:
        return pd.DataFrame()
    print(f"  ↓ {len(tickers)} tickers {start} → {end}")
    data = yf.download(
        tickers, start=start, end=(end + dt.timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1d", group_by="ticker", auto_adjust=False, threads=True)
    if data is None or data.empty:
        return pd.DataFrame()

    if isinstance(data.columns, pd.MultiIndex):
        # Détecter l'ordre des niveaux : si level 0 contient des noms OHLCV, swap
        lev0 = set(data.columns.get_level_values(0).unique())
        ohlcv = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
        if lev0 & ohlcv:
            data = data.swaplevel(axis=1)
        data = data.stack(level=0, future_stack=True).reset_index()
        data = data.rename(columns={"level_1": "Ticker"})
    else:
        data = data.reset_index()
        data["Ticker"] = tickers[0] if isinstance(tickers, list) else tickers

    want = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    return data[[c for c in want if c in data.columns]].sort_values(["Date", "Ticker"]).reset_index(drop=True)


def _save_prices(df: pd.DataFrame, path: Path):
    if df is None or df.empty: return
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df = df.rename(columns={"Adj Close": "AdjClose"})
    for c in ["Open", "High", "Low", "Close", "AdjClose", "Volume"]:
        if c not in df.columns: df[c] = np.nan
    c = _conn(path)
    c.executemany("INSERT OR REPLACE INTO prices VALUES (?,?,?,?,?,?,?,?)",
                  df[["Date","Ticker","Open","High","Low","Close","AdjClose","Volume"]]
                  .itertuples(index=False, name=None))
    c.commit(); c.close()


def _step_prices(key: str) -> tuple[pd.DataFrame, bool]:
    cfg = INDEX_CONFIG[key]
    db = _db_path(key)
    _init_db(db)
    tickers = get_tickers(key)

    # Inclure l'indice de marché dans les prix (nécessaire pour beta_60 / mkt_ret)
    mkt = cfg.get("market_ticker", "")
    if mkt and mkt not in tickers:
        tickers = tickers + [mkt]

    last = _max_date(db, "prices")
    today = _last_trading_day(cfg["proxy"])

    # Vérifier si l'indice de marché est absent de la DB → forcer le download
    mkt_missing = mkt and mkt not in _tickers_in_db(db, "prices")

    if last and last >= today and not mkt_missing:
        print(f"  Prix ✓ à jour ({last})")
        return pd.DataFrame(), False

    if mkt_missing and last and last >= today:
        # Prix à jour mais indice manquant → download complet de l'indice
        print(f"  Prix à jour ({last}) mais indice {mkt} absent → download")
        start = pd.to_datetime(START_DEFAULT).date()
        new = _dl_prices([mkt], start, today)
        _save_prices(new, db)
        return new, True

    start = (last + dt.timedelta(days=1)) if last else pd.to_datetime(START_DEFAULT).date()
    print(f"  Prix: {'DB vide → full' if not last else f'MAJ {last} → {today}'}")

    new = _dl_prices(tickers, start, today)
    _save_prices(new, db)
    return new, True


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — RETURNS
# ═══════════════════════════════════════════════════════════════════════════════

def _rebuild_returns(db: Path):
    c = _conn(db)
    p = pd.read_sql_query("SELECT Date,Ticker,AdjClose FROM prices ORDER BY Ticker,Date",
                          c, parse_dates=["Date"])
    if p.empty: c.close(); return
    p["Return"] = p.groupby("Ticker")["AdjClose"].pct_change(fill_method=None)
    with np.errstate(invalid="ignore", divide="ignore"):
        p["LogReturn"] = np.log1p(p["Return"])
    p["Date"] = p["Date"].dt.strftime("%Y-%m-%d")
    c.execute("DELETE FROM returns")
    c.executemany("INSERT INTO returns VALUES (?,?,?,?,?)",
                  p[["Date","Ticker","AdjClose","Return","LogReturn"]]
                  .itertuples(index=False, name=None))
    c.commit(); c.close()


def _step_returns(new_px: pd.DataFrame, had_new: bool, db: Path):
    if not had_new:
        print("  Returns ✓")
        return

    pc = _count(db, "prices")
    rc = _count(db, "returns")

    if pc > 0 and rc < max(1000, int(0.01 * pc)):
        print("  Returns: rebuild...")
        _rebuild_returns(db)
        return

    if new_px is None or new_px.empty: return

    df = new_px.copy()
    if "Adj Close" in df.columns: df = df.rename(columns={"Adj Close": "AdjClose"})
    df["Date"] = pd.to_datetime(df["Date"])
    start_str = df["Date"].min().strftime("%Y-%m-%d")

    c = _conn(db)
    prev = pd.read_sql_query(
        """SELECT p.Ticker, p.AdjClose FROM prices p
           JOIN (SELECT Ticker, MAX(Date) AS md FROM prices WHERE Date < ? GROUP BY Ticker) x
           ON p.Ticker=x.Ticker AND p.Date=x.md""", c, params=[start_str])
    c.close()
    prev["Date"] = pd.to_datetime(start_str) - pd.Timedelta(days=1)

    w = pd.concat([prev[["Date","Ticker","AdjClose"]], df[["Date","Ticker","AdjClose"]]],
                  ignore_index=True).sort_values(["Ticker","Date"])
    w["Return"] = w.groupby("Ticker")["AdjClose"].pct_change(fill_method=None)
    with np.errstate(invalid="ignore", divide="ignore"):
        w["LogReturn"] = np.log1p(w["Return"])

    out = w[w["Date"] >= pd.to_datetime(start_str)].copy()
    out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")

    c = _conn(db)
    c.executemany("INSERT OR REPLACE INTO returns VALUES (?,?,?,?,?)",
                  out[["Date","Ticker","AdjClose","Return","LogReturn"]]
                  .itertuples(index=False, name=None))
    c.commit(); c.close()
    print(f"  Returns: +{len(out)} rows")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — RATIOS (skip si pas de nouvelles données)
# ═══════════════════════════════════════════════════════════════════════════════

def _step_ratios(db: Path, had_new: bool):
    if not had_new:
        if _count(db, "ratios") > 0:
            print("  Ratios ✓")
            return
    print("  Ratios...")
    c = _conn(db)
    ret = pd.read_sql_query("SELECT Ticker,Return FROM returns ORDER BY Ticker,Date", c)
    px = pd.read_sql_query("SELECT Ticker,AdjClose FROM prices ORDER BY Ticker,Date", c)
    c.close()
    if ret.empty: return

    rows = []
    px_by = {t: g for t, g in px.groupby("Ticker")}
    for t, g in ret.groupby("Ticker"):
        r = g["Return"].dropna()
        if len(r) < 50: continue
        mu, vol = r.mean(), r.std()
        ar = (1+mu)**252-1; av = vol*np.sqrt(252)
        sh = ar/av if av > 0 else np.nan
        md = np.nan
        pg = px_by.get(t)
        if pg is not None:
            p = pg["AdjClose"].dropna()
            if not p.empty: md = float((p/p.cummax()-1).min())
        rows.append({"Ticker":t,"DailyMeanReturn":mu,"DailyVol":vol,
                     "AnnualizedReturn":ar,"AnnualizedVol":av,"Sharpe":sh,"MaxDrawdown":md,"NbObs":len(r)})

    if not rows: return
    df = pd.DataFrame(rows)
    c = _conn(db)
    c.executemany("INSERT OR REPLACE INTO ratios VALUES (?,?,?,?,?,?,?,?)",
                  df[["Ticker","DailyMeanReturn","DailyVol","AnnualizedReturn","AnnualizedVol",
                      "Sharpe","MaxDrawdown","NbObs"]].itertuples(index=False, name=None))
    c.commit(); c.close()


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4 — FUNDAMENTALS (résumable, throttled, tracked)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_earnings(tickers: list[str], db: Path):
    _init_db(db)
    done = _tickers_attempted(db, "earnings_history")
    todo = [t for t in tickers if t not in done]
    if not todo:
        n_data = len(_tickers_in_db(db, "earnings_history"))
        n_empty = len(done) - n_data
        print(f"  [Earnings] ✓ ({n_data} avec data, {n_empty} sans data Yahoo)")
        return
    print(f"  [Earnings] {len(todo)} à fetch ({len(done)} déjà traités)")

    c = sqlite3.connect(db, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    n, errs, t0 = 0, 0, time.time()

    try:
        for i, tk in enumerate(todo, 1):
            if i == 1 or i % 25 == 0:
                print(f"    [{i}/{len(todo)}] {tk} ({time.time()-t0:.0f}s)")
            time.sleep(0.5)
            if i % 50 == 0:
                c.commit()
                print(f"    ⏸ 30s")
                time.sleep(30)

            tk_rows = 0
            try:
                ed = yf.Ticker(tk).earnings_dates
                if ed is None or ed.empty:
                    _log_fetch(db, tk, "earnings_history", 0, conn=c)
                    continue
                ed = ed.reset_index().rename(columns={ed.reset_index().columns[0]: "RD"})
                if "Reported EPS" not in ed.columns:
                    _log_fetch(db, tk, "earnings_history", 0, conn=c)
                    continue
                ed = ed.dropna(subset=["Reported EPS"])
                if ed.empty:
                    _log_fetch(db, tk, "earnings_history", 0, conn=c)
                    continue

                for _, row in ed.iterrows():
                    rd = _strip_tz(pd.to_datetime(row["RD"]))
                    if pd.isna(rd): continue
                    act = row.get("Reported EPS")
                    est = row.get("EPS Estimate")
                    if pd.isna(act): continue
                    su = su_a = np.nan
                    if pd.notna(est) and abs(est) > 1e-8:
                        su = (act - est) / abs(est)
                        su_a = act - est
                    c.execute("INSERT OR REPLACE INTO earnings_history VALUES (?,?,?,?,?,?,?)",
                              (tk, rd.strftime("%Y-%m-%d"), None,
                               float(act), float(est) if pd.notna(est) else None,
                               float(su) if pd.notna(su) else None,
                               float(su_a) if pd.notna(su_a) else None))
                    tk_rows += 1; n += 1
            except Exception as e:
                errs += 1
                if errs <= 5: print(f"    ⚠ {tk}: {e}")

            _log_fetch(db, tk, "earnings_history", tk_rows, conn=c)

        c.commit()
    finally:
        c.close()
    print(f"  [Earnings] +{n} rows, {errs} errors ({time.time()-t0:.0f}s)")


# ── Robust field extraction ──

def _get_field(df, fiscal_end, *names):
    """Try multiple field names, return first match."""
    if df is None or df.empty or fiscal_end not in df.columns:
        return None
    for name in names:
        if name in df.index:
            v = df.loc[name, fiscal_end]
            if pd.notna(v):
                return float(v)
    # Fuzzy: case-insensitive, space-insensitive
    for name in names:
        nl = name.lower().replace(" ", "")
        for idx in df.index:
            if idx.lower().replace(" ", "") == nl:
                v = df.loc[idx, fiscal_end]
                if pd.notna(v):
                    return float(v)
    return None


def _fetch_financials(tickers: list[str], db: Path):
    _init_db(db)
    done = _tickers_attempted(db, "quarterly_financials")
    todo = [t for t in tickers if t not in done]
    if not todo:
        n_data = len(_tickers_in_db(db, "quarterly_financials"))
        n_empty = len(done) - n_data
        print(f"  [Financials] ✓ ({n_data} avec data, {n_empty} sans data Yahoo)")
        return
    print(f"  [Financials] {len(todo)} à fetch ({len(done)} déjà traités)")

    c = sqlite3.connect(db, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    n, errs, t0 = 0, 0, time.time()

    try:
        for i, tk in enumerate(todo, 1):
            if i == 1 or i % 25 == 0:
                print(f"    [{i}/{len(todo)}] {tk} ({time.time()-t0:.0f}s)")
            time.sleep(0.5)
            if i % 50 == 0:
                c.commit()
                print(f"    ⏸ 30s")
                time.sleep(30)

            tk_rows = 0
            try:
                obj = yf.Ticker(tk)
                inc = obj.quarterly_income_stmt
                if inc is None or inc.empty:
                    _log_fetch(db, tk, "quarterly_financials", 0, conn=c)
                    continue
                bs = obj.quarterly_balance_sheet
                cf = obj.quarterly_cashflow

                # Earnings dates for point-in-time
                ed = pd.DataFrame()
                try:
                    _ed = obj.earnings_dates
                    if _ed is not None and not _ed.empty:
                        ed = _ed.reset_index().rename(columns={_ed.reset_index().columns[0]: "ED"})
                        ed["ED"] = pd.to_datetime(ed["ED"]).dt.tz_localize(None)
                        if "Reported EPS" in ed.columns:
                            ed = ed.dropna(subset=["Reported EPS"])
                except Exception:
                    pass

                for fe in inc.columns:
                    fe_dt = _strip_tz(pd.to_datetime(fe))

                    rd = None
                    if not ed.empty:
                        fut = ed[ed["ED"] > fe_dt]["ED"]
                        if len(fut) > 0: rd = fut.min()
                    if rd is None:
                        rd = fe_dt + pd.Timedelta(days=45)
                    if rd > pd.Timestamp.today():
                        continue

                    rev = _get_field(inc, fe,
                        "Total Revenue", "TotalRevenue", "Revenue",
                        "Operating Revenue", "OperatingRevenue")
                    ni = _get_field(inc, fe,
                        "Net Income", "NetIncome",
                        "Net Income Common Stockholders", "NetIncomeCommonStockholders",
                        "Net Income From Continuing Operations", "NetIncomeContinuousOperations")
                    ta = _get_field(bs, fe,
                        "Total Assets", "TotalAssets")
                    td = _get_field(bs, fe,
                        "Total Debt", "TotalDebt",
                        "Net Debt", "NetDebt",
                        "Long Term Debt", "LongTermDebt",
                        "Total Liabilities Net Minority Interest", "TotalLiabilitiesNetMinorityInterest")
                    bv = _get_field(bs, fe,
                        "Stockholders Equity", "StockholdersEquity",
                        "Total Equity Gross Minority Interest", "TotalEquityGrossMinorityInterest",
                        "Common Stock Equity", "CommonStockEquity",
                        "Total Stockholders Equity",
                        "Ordinary Shares Number")
                    ocf = _get_field(cf, fe,
                        "Operating Cash Flow", "OperatingCashFlow",
                        "Total Cash From Operating Activities",
                        "Cash Flow From Continuing Operating Activities",
                        "CashFlowFromContinuingOperatingActivities",
                        "Free Cash Flow", "FreeCashFlow")

                    if rev is None and ni is None and bv is None:
                        continue

                    c.execute("INSERT OR REPLACE INTO quarterly_financials VALUES (?,?,?,?,?,?,?,?,?)",
                              (tk, rd.strftime("%Y-%m-%d"), fe_dt.strftime("%Y-%m-%d"),
                               rev, ni, ta, td, bv, ocf))
                    tk_rows += 1; n += 1
            except Exception as e:
                errs += 1
                if errs <= 5: print(f"    ⚠ {tk}: {e}")

            _log_fetch(db, tk, "quarterly_financials", tk_rows, conn=c)

        c.commit()
    finally:
        c.close()
    print(f"  [Financials] +{n} rows, {errs} errors ({time.time()-t0:.0f}s)")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5 — COMPUTE FUNDAMENTAL FEATURES (0 download)
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_earnings_feat(db: Path) -> pd.DataFrame:
    if _count(db, "earnings_history") == 0: return pd.DataFrame()
    c = sqlite3.connect(db)
    df = pd.read_sql_query(
        "SELECT Ticker,ReportDate,EPS_actual,EPS_estimate,Surprise,SurpriseAbs "
        "FROM earnings_history ORDER BY Ticker,ReportDate", c, parse_dates=["ReportDate"])
    c.close()
    if df.empty: return pd.DataFrame()

    df = df.sort_values(["Ticker","ReportDate"]).reset_index(drop=True)
    df["eps_surprise"] = df["Surprise"]
    df["eps_surprise_abs"] = df["SurpriseAbs"]
    df["eps_beat"] = (df["EPS_actual"] > df["EPS_estimate"]).astype(float)
    df.loc[df["EPS_estimate"].isna() | df["EPS_actual"].isna(), "eps_beat"] = np.nan
    df["eps_growth_qoq"] = df.groupby("Ticker")["EPS_actual"].pct_change(fill_method=None).clip(-5,5)
    df["eps_surprise_ma4"] = df.groupby("Ticker")["eps_surprise"].transform(
        lambda s: s.rolling(4, min_periods=2).mean())

    # consecutive_beats : reset si gap entre ReportDates > 100j calendaires
    df["_rd_gap"] = df.groupby("Ticker")["ReportDate"].diff().dt.days
    def _consec(grp):
        out, cnt = [], 0
        beats = grp["eps_beat"].values
        gaps  = grp["_rd_gap"].values
        for b, g in zip(beats, gaps):
            if pd.notna(g) and g > 100:
                cnt = 0  # reset après gap trop long (> ~1 quarter)
            cnt = cnt + 1 if b == 1.0 else 0
            out.append(cnt)
        return pd.Series(out, index=grp.index)
    df["consecutive_beats"] = df.groupby("Ticker", group_keys=False).apply(_consec)
    df = df.drop(columns=["_rd_gap"])

    cols = ["eps_surprise","eps_surprise_abs","eps_beat","eps_growth_qoq","eps_surprise_ma4","consecutive_beats"]
    return df[["Ticker","ReportDate"]+cols].rename(columns={"ReportDate":"Date"})


def _calc_fin_feat(db: Path) -> pd.DataFrame:
    if _count(db, "quarterly_financials") == 0: return pd.DataFrame()
    c = sqlite3.connect(db)
    df = pd.read_sql_query(
        "SELECT Ticker,ReportDate,FiscalEnd,TotalRevenue,NetIncome,TotalAssets,TotalDebt,BookValue,OperatingCashFlow "
        "FROM quarterly_financials ORDER BY Ticker,ReportDate", c,
        parse_dates=["ReportDate", "FiscalEnd"])
    c.close()
    if df.empty: return pd.DataFrame()

    df = df.sort_values(["Ticker","ReportDate"]).reset_index(drop=True)
    g = df.groupby("Ticker", sort=False)
    df["revenue_growth_qoq"] = g["TotalRevenue"].pct_change(1, fill_method=None).clip(-5,5)

    # YoY : pct_change(4) MAIS invalider si l'écart fiscal_end n'est pas ~365j (±60j)
    df["revenue_growth_yoy"] = g["TotalRevenue"].pct_change(4, fill_method=None).clip(-5,5)
    if df["FiscalEnd"].notna().any():
        fe_shift = g["FiscalEnd"].shift(4)
        fe_gap = (df["FiscalEnd"] - fe_shift).dt.days
        # Invalider les YoY où le gap n'est pas ~1 an (305-425j)
        bad_yoy = fe_gap.notna() & ((fe_gap < 305) | (fe_gap > 425))
        df.loc[bad_yoy, "revenue_growth_yoy"] = np.nan

    rev = df["TotalRevenue"].replace(0, np.nan)
    df["net_margin"] = (df["NetIncome"]/rev).clip(-2,2)
    bv = df["BookValue"].replace(0, np.nan)
    df["debt_to_equity"] = (df["TotalDebt"]/bv).clip(-10,10)
    ta = df["TotalAssets"].replace(0, np.nan)
    df["roa"] = (4*df["NetIncome"]/ta).clip(-2,2)
    df["ocf_to_revenue"] = (df["OperatingCashFlow"]/rev).clip(-5,5)

    cols = ["revenue_growth_qoq","revenue_growth_yoy","net_margin","debt_to_equity","roa","ocf_to_revenue"]
    return df[["Ticker","ReportDate"]+cols].rename(columns={"ReportDate":"Date"})


def _merge_pit(daily: pd.DataFrame, fund: pd.DataFrame) -> pd.DataFrame:
    if fund.empty: return daily
    daily = daily.copy(); fund = fund.copy()
    daily["Date"] = pd.to_datetime(daily["Date"])
    fund["Date"] = pd.to_datetime(fund["Date"])
    fund = fund.sort_values(["Ticker","Date"]).drop_duplicates(["Ticker","Date"], keep="last")

    fcols = [c for c in fund.columns if c not in ("Date","Ticker")]
    parts = []
    for tk, dtk in daily.groupby("Ticker", sort=False):
        ftk = fund[fund["Ticker"] == tk]
        if ftk.empty:
            for c in fcols: dtk[c] = np.nan
            parts.append(dtk)
        else:
            parts.append(pd.merge_asof(
                dtk.sort_values("Date"),
                ftk[["Date"]+fcols].sort_values("Date"),
                on="Date", direction="backward"))
    return pd.concat(parts, ignore_index=True).sort_values(["Ticker","Date"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — TECHNICAL FEATURES (0 download)
# ═══════════════════════════════════════════════════════════════════════════════


def _build_temporal_features(
    feat: pd.DataFrame,
    candidates: list = None,
    windows: tuple = TEMPORAL_WINDOWS,
) -> pd.DataFrame:
    """delta_{w}d_{col} et ma_{w}d_{col} pour chaque candidat présent."""
    if candidates is None:
        candidates = TEMPORAL_CANDIDATES
    feat = feat.sort_values(["Ticker","Date"]).copy()
    g = feat.groupby("Ticker", sort=False)
    new_cols = []
    for col in candidates:
        if col not in feat.columns: continue
        for w in windows:
            dn = f"delta_{w}d_{col}"
            feat[dn] = feat[col] - g[col].shift(w)
            new_cols.append(dn)
            mn = f"ma_{w}d_{col}"
            feat[mn] = g[col].transform(
                lambda s, _w=w: s.rolling(_w, min_periods=max(1, _w//2)).mean())
            new_cols.append(mn)
    base_present = [c for c in candidates if c in feat.columns]
    print(f"  Temporal: +{len(new_cols)} colonnes "
          f"({len(base_present)} base x {len(windows)} windows x 2)")
    return feat


def _build_interaction_features(feat: pd.DataFrame) -> pd.DataFrame:
    """mom120_div_rv60 et mom20_div_mom120."""
    interactions = [
        ("mom_120", "rv_60",   "mom120_div_rv60"),
        ("mom_20",  "mom_120", "mom20_div_mom120"),
    ]
    added = []
    for col_a, col_b, name in interactions:
        if col_a not in feat.columns or col_b not in feat.columns: continue
        if name in feat.columns: continue
        feat[name] = (feat[col_a] / feat[col_b].replace(0, np.nan)).clip(-5, 5)
        added.append(name)
    if added:
        print(f"  Interactions: +{len(added)} colonnes ({added})")
    return feat


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 8 — YAHOO HISTORY FEATURES
# ═══════════════════════════════════════════════════════════════════════════════
# NOTE: _update_yf_prices est conservée pour compatibilité mais n'est plus appelée.
# Les features yf_* sont désormais calculées directement depuis la table `prices`.
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_rsi(series: pd.Series, window: int) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int=14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([high-low, (high-prev_close).abs(), (low-prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(window, min_periods=window//2).mean()


def _update_yf_prices(db: Path, tickers: list[str], index_ticker: str, history_start: str):
    """
    Téléchargement incrémental des prix Yahoo dans yf_prices.
    Ne télécharge que les données postérieures à la dernière date en DB.
    Utilise auto_adjust=False (cohérent avec _dl_prices) et stocke AdjClose comme Close.
    """
    _init_db(db)
    last  = _max_date(db, "yf_prices")
    today = dt.date.today()

    if last and last >= today:
        print(f"  [YF prices] ✓ à jour ({last})")
        return

    start = (last + dt.timedelta(days=1)) if last else pd.to_datetime(history_start).date()
    all_tickers = list(set(tickers + [index_ticker]))
    end_str   = today.strftime("%Y-%m-%d")
    start_str = start.strftime("%Y-%m-%d")

    print(f"  [YF prices] {'full depuis '+history_start if not last else f'MAJ {last} → {today}'}"
          f" | {len(tickers)} tickers + index")

    raw = yf.download(all_tickers, start=start_str, end=end_str,
                      auto_adjust=False, progress=False, threads=True)
    if raw is None or raw.empty:
        print("  [YF prices] Aucune donnée retournée.")
        return

    if isinstance(raw.columns, pd.MultiIndex):
        # Détecter l'ordre des niveaux du MultiIndex (Ticker/Field ou Field/Ticker)
        lev0 = set(raw.columns.get_level_values(0).unique())
        ohlcv = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
        if lev0 & ohlcv:
            raw = raw.swaplevel(axis=1)
        price_df = raw.stack(level=0, future_stack=True).rename_axis(["Date","Ticker"]).reset_index()
    else:
        price_df = raw.reset_index()
        price_df["Ticker"] = all_tickers[0]

    price_df["Date"] = pd.to_datetime(price_df["Date"]).dt.strftime("%Y-%m-%d")
    price_df = price_df.rename(columns=str)

    # Utiliser Adj Close (cohérent avec la table prices qui utilise aussi auto_adjust=False)
    if "Adj Close" in price_df.columns:
        price_df["Close"] = price_df["Adj Close"].where(
            price_df["Adj Close"].notna(), price_df.get("Close"))

    for col in ["Open","High","Low","Close","Volume"]:
        if col not in price_df.columns:
            price_df[col] = np.nan

    rows = price_df[["Date","Ticker","Open","High","Low","Close","Volume"]].dropna(subset=["Date","Ticker"])

    c = _conn(db)
    try:
        c.executemany("INSERT OR REPLACE INTO yf_prices VALUES (?,?,?,?,?,?,?)",
                      rows.itertuples(index=False, name=None))
        c.commit()
    finally:
        c.close()
    print(f"  [YF prices] +{len(rows)} rows ({rows['Ticker'].nunique()} tickers)")


def _update_macro_prices(db: Path, cfg: dict):
    """
    Téléchargement incrémental des séries macro (VIX, yields) dans macro_prices.
    Tickers définis dans INDEX_CONFIG: macro_vix, macro_yield_10y, macro_yield_3m.
    Si un ticker configuré est absent de la table, force un téléchargement complet.
    """
    _init_db(db)
    macro_tickers = []
    for k in ("macro_vix", "macro_yield_10y", "macro_yield_3m"):
        t = cfg.get(k)
        if t:
            macro_tickers.append(t)
    macro_tickers = list(set(macro_tickers))
    if not macro_tickers:
        return

    last  = _max_date(db, "macro_prices")
    today = dt.date.today()

    # Vérifier si tous les tickers configurés sont présents dans la table
    force_full = False
    if last and _tbl_exists(db, "macro_prices"):
        c = sqlite3.connect(db)
        existing = {r[0] for r in c.execute(
            "SELECT DISTINCT Ticker FROM macro_prices").fetchall()}
        c.close()
        missing = set(macro_tickers) - existing
        if missing:
            print(f"  [Macro prices] Tickers manquants: {missing} — redownload complet")
            force_full = True

    if last and last >= today and not force_full:
        print(f"  [Macro prices] ✓ à jour ({last})")
        return

    if force_full:
        start = pd.to_datetime(
            cfg.get("model_start", START_DEFAULT)).date() - dt.timedelta(days=300)
    else:
        start = (last + dt.timedelta(days=1)) if last else pd.to_datetime(
            cfg.get("model_start", START_DEFAULT)).date() - dt.timedelta(days=300)
    start_str = start.strftime("%Y-%m-%d")
    end_str   = today.strftime("%Y-%m-%d")

    print(f"  [Macro prices] {'full redownload' if force_full else ('full' if not last else f'MAJ {last} → {today}')}"
          f" | tickers: {macro_tickers}")

    raw = yf.download(macro_tickers, start=start_str, end=end_str,
                      auto_adjust=True, progress=False, threads=True)
    if raw is None or raw.empty:
        print("  [Macro prices] Aucune donnée retournée.")
        return

    # Extraire Close uniquement
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw["Close"]
    else:
        close = raw[["Close"]]
        close.columns = [macro_tickers[0]]

    close = close.reset_index().melt(id_vars="Date", var_name="Ticker", value_name="Close")
    close = close.dropna(subset=["Close"])
    close["Date"] = pd.to_datetime(close["Date"]).dt.strftime("%Y-%m-%d")

    c = _conn(db)
    try:
        c.executemany("INSERT OR REPLACE INTO macro_prices VALUES (?,?,?)",
                      close[["Date","Ticker","Close"]].itertuples(index=False, name=None))
        c.commit()
    finally:
        c.close()
    print(f"  [Macro prices] +{len(close)} rows ({close['Ticker'].nunique()} tickers)")


def _build_macro_features(db: Path, feat: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Construit 3 features macro broadcast (même valeur pour tous les tickers à une date) :
      - macro_vix       : niveau VIX (ou VSTOXX)
      - macro_vix_chg_20: variation VIX sur 20j (%)
      - macro_term_spread: yield 10Y - yield 3M (en points)
    Merge sur Date dans feat. Forward-fill pour gérer les jours fériés.
    """
    if not _tbl_exists(db, "macro_prices") or _count(db, "macro_prices") == 0:
        print("  [Macro features] macro_prices vide — features macro ignorées.")
        return feat

    c = sqlite3.connect(db)
    macro_df = pd.read_sql_query(
        "SELECT Date, Ticker, Close FROM macro_prices ORDER BY Date",
        c, parse_dates=["Date"])
    c.close()

    if macro_df.empty:
        print("  [Macro features] Aucune donnée macro.")
        return feat

    # Pivot: colonnes = tickers macro, index = Date
    pivot = macro_df.pivot_table(index="Date", columns="Ticker", values="Close")
    pivot = pivot.sort_index().ffill()  # forward-fill weekends / holidays

    vix_tk = cfg.get("macro_vix")
    y10_tk = cfg.get("macro_yield_10y")
    y3m_tk = cfg.get("macro_yield_3m")

    macro = pd.DataFrame(index=pivot.index)

    # macro_vix — niveau
    if vix_tk and vix_tk in pivot.columns:
        macro["macro_vix"] = pivot[vix_tk]
        # macro_vix_chg_20 — variation 20j en %
        macro["macro_vix_chg_20"] = pivot[vix_tk].pct_change(20)

    # macro_term_spread — 10Y yield - 3M yield
    if y10_tk and y3m_tk and y10_tk in pivot.columns and y3m_tk in pivot.columns:
        macro["macro_term_spread"] = pivot[y10_tk] - pivot[y3m_tk]

    if macro.empty:
        print("  [Macro features] Aucune feature macro calculable.")
        return feat

    macro = macro.reset_index().rename(columns={"index": "Date"})
    macro["Date"] = pd.to_datetime(macro["Date"])

    n_before = len(feat.columns)
    feat = feat.copy()
    feat["Date"] = pd.to_datetime(feat["Date"])
    feat = feat.merge(macro, on="Date", how="left")

    n_macro = len(feat.columns) - n_before
    n_ok = feat[[c for c in macro.columns if c != "Date"]].notna().any(axis=0).sum()
    print(f"  [Macro features] +{n_macro} features ({n_ok} non-vides)")
    return feat


def _build_yf_features_from_db(
    db: Path,
    feat: pd.DataFrame,
    index_ticker: str,
    model_start: str | None = None,
) -> pd.DataFrame:
    """
    Calcule les 17 features yf_* depuis la table `prices` (plus besoin de yf_prices).
    Utilise AdjClose comme prix ajusté. Lit avec un buffer de 600j pour les lookbacks 252j.
    Merge sur (Date, Ticker) avec feat.
    """
    if not _tbl_exists(db, "prices") or _count(db, "prices") == 0:
        print("  [YF features] prices vide — features yf_* ignorées.")
        return feat

    # Buffer de 600j calendaires (~252 jours ouvrés) pour les rolling 252
    c = sqlite3.connect(db)
    if model_start:
        buf_start = (pd.to_datetime(model_start) - pd.Timedelta(days=600)).strftime("%Y-%m-%d")
        price_df = pd.read_sql_query(
            "SELECT Date,Ticker,Open,High,Low,AdjClose AS Close,Volume "
            "FROM prices WHERE Date >= ? ORDER BY Ticker,Date",
            c, params=[buf_start], parse_dates=["Date"])
    else:
        price_df = pd.read_sql_query(
            "SELECT Date,Ticker,Open,High,Low,AdjClose AS Close,Volume "
            "FROM prices ORDER BY Ticker,Date",
            c, parse_dates=["Date"])
    c.close()

    if price_df.empty:
        print("  [YF features] Aucune donnée prix.")
        return feat

    # Extraire l'indice
    idx_df    = price_df[price_df["Ticker"]==index_ticker][["Date","Close"]].set_index("Date")["Close"]
    index_ret = np.log(idx_df / idx_df.shift(1)).rename("idx_log_ret")

    # Tickers univers uniquement
    price_df = price_df[price_df["Ticker"] != index_ticker].copy()
    n_ok = price_df["Ticker"].nunique()
    print(f"  [YF features] Calcul sur {n_ok} tickers depuis prices...")

    results = []
    for ticker, grp in price_df.groupby("Ticker", sort=False):
        grp   = grp.set_index("Date").sort_index()
        close = grp["Close"].where(grp["Close"]>0)
        high  = grp["High"]   if "High"   in grp.columns else close
        low   = grp["Low"]    if "Low"    in grp.columns else close
        vol   = grp["Volume"] if "Volume" in grp.columns else pd.Series(np.nan, index=grp.index)

        log_ret = np.log(close / close.shift(1))

        roll_max = close.rolling(252, min_periods=126).max()
        roll_min = close.rolling(252, min_periods=126).min()

        vol_5   = log_ret.rolling(5,   min_periods=3).std()
        vol_20  = log_ret.rolling(20,  min_periods=10).std()
        vol_252 = log_ret.rolling(252, min_periods=126).std()
        v20_avg  = vol.rolling(20,  min_periods=10).mean()
        v252_avg = vol.rolling(252, min_periods=126).mean()
        sma50    = close.rolling(50,  min_periods=25).mean()
        sma200   = close.rolling(200, min_periods=100).mean()

        common = log_ret.index.intersection(index_ret.index)
        if len(common) >= 60:
            sa  = log_ret.reindex(common)
            ia  = index_ret.reindex(common)
            corr = sa.rolling(60, min_periods=30).corr(ia).reindex(close.index)
            cov  = sa.rolling(252, min_periods=126).cov(ia)
            var  = ia.rolling(252, min_periods=126).var().replace(0, np.nan)
            beta = (cov/var).reindex(close.index)
        else:
            corr = pd.Series(np.nan, index=close.index)
            beta = pd.Series(np.nan, index=close.index)

        atr = _compute_atr(high, low, close, 14)

        row = pd.DataFrame({
            "yf_dist_52w_high":     (close - roll_max) / roll_max,
            "yf_dist_52w_low":      (close - roll_min) / roll_min.replace(0, np.nan),
            "yf_rsi_14":            _compute_rsi(close, 14),
            "yf_rsi_60":            _compute_rsi(close, 60),
            "yf_vol_ratio":         vol_20  / vol_252.replace(0, np.nan),
            "yf_vol_ratio_short":   vol_5   / vol_20.replace(0, np.nan),
            "yf_volume_ratio":      v20_avg / v252_avg.replace(0, np.nan),
            "yf_dist_sma50":        (close - sma50)  / sma50.replace(0, np.nan),
            "yf_dist_sma200":       (close - sma200) / sma200.replace(0, np.nan),
            "yf_mom_3m":            log_ret.rolling(63,  min_periods=30).sum(),
            "yf_mom_6m":            log_ret.rolling(126, min_periods=60).sum(),
            "yf_mom_12m":           log_ret.rolling(252, min_periods=126).sum(),
            "yf_drawdown_252":      (close - close.rolling(252, min_periods=63).max()) /
                                     close.rolling(252, min_periods=63).max().replace(0, np.nan),
            "yf_trend_consistency": (log_ret>0).rolling(60, min_periods=30).mean(),
            "yf_corr_idx_60":       corr,
            "yf_beta_idx_252":      beta,
            "yf_atr_ratio":         atr / close.replace(0, np.nan),
        }, index=close.index)
        row.index.name = "Date"
        row["Ticker"] = ticker
        results.append(row.reset_index())

    if not results:
        print("  [YF features] Aucune feature calculée.")
        return feat

    yf_df = pd.concat(results, ignore_index=True)
    yf_df["Date"]   = pd.to_datetime(yf_df["Date"])
    yf_df["Ticker"] = yf_df["Ticker"].astype(str)

    yf_cols = [c for c in yf_df.columns if c.startswith("yf_")]
    pct = yf_df[yf_cols].notna().mean().mean() * 100
    print(f"  [YF features] {len(yf_cols)} features — {pct:.1f}% de lignes remplies.")

    feat["Date"]   = pd.to_datetime(feat["Date"])
    feat["Ticker"] = feat["Ticker"].astype(str)
    return feat.merge(yf_df, on=["Date","Ticker"], how="left")


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def _load_px(db: Path, start_date: str | None = None) -> pd.DataFrame:
    """Charge les prix depuis la DB.

    start_date : si fourni, ne charge que les données >= cette date
                 MOINS un buffer de 200 jours pour les lookbacks longs (mom_120, beta_60...).
    """
    conn = sqlite3.connect(db)
    if start_date:
        # Buffer de 200 jours ouvrés ~ 280 jours calendaires pour couvrir mom_120 + beta_60
        buf = (pd.to_datetime(start_date) - pd.Timedelta(days=280)).strftime("%Y-%m-%d")
        query = (
            "SELECT Date,Ticker,Open,High,Low,Close,AdjClose,Volume "
            "FROM prices WHERE Date >= ? ORDER BY Ticker,Date"
        )
        df = pd.read_sql_query(query, conn, params=[buf], parse_dates=["Date"])
    else:
        df = pd.read_sql_query(
            "SELECT Date,Ticker,Open,High,Low,Close,AdjClose,Volume FROM prices ORDER BY Ticker,Date",
            conn, parse_dates=["Date"])
    conn.close()

    df = df.dropna(subset=["Date","Ticker"]).copy()
    for col in ["Open","High","Low","Close","AdjClose","Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["AdjClose"] = df["AdjClose"].where(df["AdjClose"] > 0).fillna(df["Close"])
    df["Close"]    = df["Close"].where(df["Close"] > 0).fillna(df["AdjClose"])
    df["Open"]     = df["Open"].fillna(df["Close"])

    # numpy vectorisé — évite DataFrame.max(axis=1) qui est très lent sur gros volumes
    o, c_ = df["Open"].values, df["Close"].values
    df["High"] = df["High"].where(df["High"].notna(), np.fmax(o, c_))
    df["Low"]  = df["Low"].where(df["Low"].notna(),  np.fmin(o, c_))

    df["Volume"] = (
        df.groupby("Ticker", sort=False)["Volume"]
        .transform(lambda s: s.fillna(s.median()))
        .fillna(0)
    )
    df = (df.sort_values(["Ticker","Date"])
            .drop_duplicates(["Ticker","Date"], keep="last")
            .reset_index(drop=True))
    for col in ["AdjClose","Close"]:
        df[col] = df.groupby("Ticker", sort=False)[col].transform(lambda s: s.ffill().bfill())
    return df


def _slope(ac: pd.Series, w: int) -> pd.Series:
    """OLS slope sur log-prix, vectorisé via numpy stride tricks (50x vs polyfit+rolling.apply).

    Résultats identiques à polyfit (diff < 1e-15), sans aucun appel Python par fenêtre.
    Les fenêtres complètes (pas de NaN) sont traitées en bulk matriciel.
    Les fenêtres partielles (données manquantes) tombent dans un fallback scalaire rare.
    """
    mp = int(np.ceil(0.8 * w))
    lp_arr = np.log(
        pd.to_numeric(ac, errors="coerce").where(lambda x: x > 0).values.astype(float)
    )
    n = len(lp_arr)

    # Vecteur x centré — constant pour toutes les fenêtres
    x = np.arange(w, dtype=float)
    x_c = x - x.mean()
    x_var = (x_c ** 2).sum()

    result = np.full(n, np.nan)
    if n < w:
        return pd.Series(result, index=ac.index)

    # Vue stride tricks : (n-w+1, w) sans copie mémoire
    shape = (n - w + 1, w)
    strides = (lp_arr.strides[0], lp_arr.strides[0])
    windows = np.lib.stride_tricks.as_strided(lp_arr, shape=shape, strides=strides)

    n_valid = np.isfinite(windows).sum(axis=1)

    # ── Fenêtres complètes : calcul bulk vectorisé ──
    full_idx = np.where(n_valid == w)[0]
    if len(full_idx) > 0:
        y = windows[full_idx]
        y_c = y - y.mean(axis=1, keepdims=True)
        result[full_idx + w - 1] = (y_c * x_c).sum(axis=1) / x_var

    # ── Fenêtres partielles (NaN internes, assez de valeurs) : fallback scalaire ──
    for i in np.where((n_valid >= max(2, mp)) & (n_valid < w))[0]:
        y = windows[i]
        m = np.isfinite(y)
        xi = x[m]; yi = y[m]
        xi_c = xi - xi.mean()
        d = (xi_c ** 2).sum()
        if d > 0:
            result[i + w - 1] = float((xi_c * (yi - yi.mean())).sum() / d)

    return pd.Series(result, index=ac.index)


def _mkt_ret(prices: pd.DataFrame, mkt: Optional[str]) -> pd.Series:
    p = prices.copy(); p["Date"] = pd.to_datetime(p["Date"])
    if mkt and (p["Ticker"]==mkt).any():
        m = p.loc[p["Ticker"]==mkt, ["Date","AdjClose"]].sort_values("Date")
        m["mkt_ret"] = m["AdjClose"].pct_change(fill_method=None)
        return m.set_index("Date")["mkt_ret"].dropna()
    p["r"] = p.groupby("Ticker", sort=False)["AdjClose"].pct_change(fill_method=None)
    return p.groupby("Date")["r"].mean().ffill().fillna(0).rename("mkt_ret")


def _build_tech(prices: pd.DataFrame, mkt: Optional[str]=None) -> pd.DataFrame:
    df = prices.sort_values(["Ticker","Date"]).reset_index(drop=True)
    df["dollar_volume"] = df["Close"]*df["Volume"]
    df["range_hl_close"] = (df["High"]-df["Low"])/df["Close"]

    g = df.groupby("Ticker", sort=False)
    for n in (1,2,3): df[f"ret_{n}"] = g["AdjClose"].pct_change(n, fill_method=None)
    for w in (5,20,60,120): df[f"mom_{w}"] = g["AdjClose"].pct_change(w, fill_method=None)

    for w, mp in [(20,16),(60,48)]:
        sma = g["AdjClose"].rolling(w, min_periods=mp).mean().reset_index(level=0, drop=True)
        df[f"dist_sma{w}"] = df["AdjClose"]/sma - 1.0
    for w in (5,10):
        m = g["AdjClose"].rolling(w).mean().reset_index(level=0, drop=True)
        s = g["AdjClose"].rolling(w).std(ddof=0).reset_index(level=0, drop=True)
        df[f"z_price_{w}"] = (df["AdjClose"]-m)/s
    for w, mp in [(20,16),(60,48)]:
        df[f"rv_{w}"] = g["ret_1"].rolling(w, min_periods=mp).std(ddof=0).reset_index(level=0, drop=True)

    vm = g["Volume"].rolling(20, min_periods=16).mean().reset_index(level=0, drop=True)
    df["vol_chg_vs20"] = df["Volume"]/vm - 1.0
    df["slope_logp_20"] = g["AdjClose"].transform(lambda s: _slope(s,20))
    df["slope_logp_60"] = g["AdjClose"].transform(lambda s: _slope(s,60))

    mr = _mkt_ret(df, mkt)
    df = df.merge(mr.reset_index(), on="Date", how="left")

    # beta_60 vectorisé : rolling cov/var sans groupby.apply
    # Trier par Ticker+Date, calculer rolling sur toute la série, masquer les bords de groupe
    df_sorted = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    grp = df_sorted.groupby("Ticker", sort=False)

    roll_cov = grp.apply(
        lambda g: g["ret_1"].rolling(60, min_periods=48).cov(g["mkt_ret"]),
        include_groups=False
    ).reset_index(level=0, drop=True)
    roll_var = grp["mkt_ret"].transform(
        lambda s: s.rolling(60, min_periods=48).var(ddof=1)
    )
    df_sorted["beta_60"] = roll_cov / roll_var.values
    df = df_sorted

    keep = ["Date","Ticker","Open","High","Low","Close","AdjClose","Volume",
            "dollar_volume","range_hl_close","ret_1","ret_2","ret_3",
            "mom_5","mom_20","mom_60","mom_120","dist_sma20","dist_sma60",
            "slope_logp_20","slope_logp_60","z_price_5","z_price_10",
            "rv_20","rv_60","vol_chg_vs20","beta_60"]
    return df[keep].sort_values(["Date","Ticker"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════

def update_prices(index: str = "all"):
    """Prix + returns + ratios + macro prices. Skip si déjà à jour."""
    for key in _resolve(index):
        cfg = INDEX_CONFIG[key]
        print(f"\n{'='*50}\n  {cfg['label']} — PRICES\n{'='*50}")
        new, had_new = _step_prices(key)
        db = _db_path(key)
        _step_returns(new, had_new, db)
        _step_ratios(db, had_new)
        # Macro prices (VIX, yields) — incrémental
        _update_macro_prices(db, cfg)


def update_fundamentals(index: str = "all"):
    """Earnings + financials. Skip tickers déjà traités (data ou vide). Résumable."""
    for key in _resolve(index):
        cfg = INDEX_CONFIG[key]
        db = _db_path(key)
        _init_db(db)
        print(f"\n{'='*50}\n  {cfg['label']} — FUNDAMENTALS\n{'='*50}")
        tickers = _tickers_from_db(key) or get_tickers(key)
        _fetch_earnings(tickers, db)
        _fetch_financials(tickers, db)


def _features_up_to_date(db: Path) -> bool:
    """True si les features couvrent déjà la dernière date de prix disponible.

    Compare 'prices_max_date' stockée dans features_meta (date de prix au moment
    du dernier calcul) avec la date max actuelle des prix.
    Vérifie aussi que les colonnes macro sont présentes (pour migration).
    Évite le recalcul systématique causé par la mtime de la DB qui change
    à chaque update_prices().
    """
    if not _tbl_exists(db, "features"):
        return False
    if not _tbl_exists(db, "features_meta"):
        return False
    c = sqlite3.connect(db)
    row = c.execute(
        "SELECT Value FROM features_meta WHERE Key='prices_max_date'"
    ).fetchone()
    cols_row = c.execute(
        "SELECT Value FROM features_meta WHERE Key='columns'"
    ).fetchone()
    c.close()
    if not row:
        return False
    # Vérifier que les colonnes macro sont présentes (migration)
    if cols_row:
        existing_cols = set(cols_row[0].split(","))
        if not set(MACRO_FEATURE_COLS).issubset(existing_cols):
            return False
    feat_prices_max = row[0]
    current_prices_max = _max_date(db, "prices")
    if current_prices_max is None:
        return False
    return feat_prices_max == str(current_prices_max)


def _save_features(feat: pd.DataFrame, db: Path):
    """Écrit le DataFrame features dans la table SQL features (remplacement atomique)."""
    feat = feat.copy()
    feat["Date"] = pd.to_datetime(feat["Date"]).dt.strftime("%Y-%m-%d")

    # Construire le CREATE TABLE dynamiquement selon les colonnes présentes
    col_defs = []
    for col in feat.columns:
        if col == "Date":
            col_defs.append("Date TEXT NOT NULL")
        elif col == "Ticker":
            col_defs.append("Ticker TEXT NOT NULL")
        else:
            col_defs.append(f'"{col}" REAL')
    cols_sql = ",\n            ".join(col_defs)
    pk = "PRIMARY KEY (Date, Ticker)"

    c = _conn(db)

    # Écriture atomique : table temporaire puis rename
    c.execute("DROP TABLE IF EXISTS features_new")
    c.execute(f"""
        CREATE TABLE features_new (
            {cols_sql},
            {pk})
    """)
    c.execute(f"CREATE INDEX IF NOT EXISTS ix_feat_new ON features_new (Ticker, Date)")

    placeholders = ",".join(["?"] * len(feat.columns))
    c.executemany(
        f"INSERT OR REPLACE INTO features_new VALUES ({placeholders})",
        feat.itertuples(index=False, name=None),
    )

    # Swap atomique
    c.execute("DROP TABLE IF EXISTS features")
    c.execute("ALTER TABLE features_new RENAME TO features")
    # Re-créer l'index avec le bon nom (ALTER TABLE RENAME ne renomme pas les index)
    c.execute("DROP INDEX IF EXISTS ix_feat_new")
    c.execute("CREATE INDEX IF NOT EXISTS ix_feat ON features (Ticker, Date)")

    # Mettre à jour le timestamp de calcul
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("CREATE TABLE IF NOT EXISTS features_meta (Key TEXT PRIMARY KEY, Value TEXT)")
    c.execute("INSERT OR REPLACE INTO features_meta VALUES ('computed_at', ?)", (now,))
    c.execute("INSERT OR REPLACE INTO features_meta VALUES ('n_rows', ?)", (str(len(feat)),))
    c.execute("INSERT OR REPLACE INTO features_meta VALUES ('n_cols', ?)", (str(len(feat.columns)),))
    c.execute("INSERT OR REPLACE INTO features_meta VALUES ('columns', ?)",
              (",".join(feat.columns.tolist()),))
    # Stocker la date max des prix couverts → permet de détecter si un recalcul est nécessaire
    prices_max = feat["Date"].max() if "Date" in feat.columns else ""
    c.execute("INSERT OR REPLACE INTO features_meta VALUES ('prices_max_date', ?)", (str(prices_max),))
    c.commit()
    c.close()


def compute_features(index: str = "all", force: bool = False):
    """Features techniques + fondamentales → table SQL 'features'. Skip si déjà à jour."""
    for key in _resolve(index):
        cfg = INDEX_CONFIG[key]
        db = _db_path(key)

        if not force:
            # Diagnostic : afficher pourquoi on calcule ou non
            _feat_exists = _tbl_exists(db, "features")
            _meta_exists = _tbl_exists(db, "features_meta")
            if _feat_exists and _meta_exists:
                _conn2 = sqlite3.connect(db)
                _row = _conn2.execute(
                    "SELECT Value FROM features_meta WHERE Key='prices_max_date'"
                ).fetchone()
                _cols_row = _conn2.execute(
                    "SELECT Value FROM features_meta WHERE Key='columns'"
                ).fetchone()
                _conn2.close()
                _feat_max = _row[0] if _row else None
                _px_max = str(_max_date(db, "prices")) if _max_date(db, "prices") else None
                _up2date = _feat_max == _px_max
                # Vérifier que les colonnes macro sont présentes (migration)
                if _up2date and _cols_row:
                    _existing_cols = set(_cols_row[0].split(","))
                    if not set(MACRO_FEATURE_COLS).issubset(_existing_cols):
                        _up2date = False
                        print(f"\n  {cfg['label']} — FEATURES recalcul: colonnes macro manquantes")
                if _up2date:
                    _conn2 = sqlite3.connect(db)
                    n = _conn2.execute("SELECT COUNT(*) FROM features").fetchone()[0]
                    _conn2.close()
                    print(f"\n  {cfg['label']} — FEATURES ✓ ({n:,} rows, SQL à jour, prices_max={_feat_max})")
                    continue
                else:
                    print(f"\n  {cfg['label']} — FEATURES recalcul: feat_max={_feat_max!r} vs px_max={_px_max!r}")
            elif not _feat_exists:
                print(f"\n  {cfg['label']} — FEATURES recalcul: table features absente")
            elif not _meta_exists:
                print(f"\n  {cfg['label']} — FEATURES recalcul: table features_meta absente")

        print(f"\n{'='*50}\n  {cfg['label']} — FEATURES\n{'='*50}")

        # Charger uniquement depuis START_DEFAULT pour éviter de lire 25 ans de SPX inutilement
        # Le buffer de 280j est géré dans _load_px
        prices = _load_px(db, start_date=cfg.get("model_start", START_DEFAULT))
        feat = _build_tech(prices, mkt=cfg["market_ticker"])

        # Exclure l'indice de marché des features (il est dans prices pour le beta/mkt_ret,
        # mais ne doit pas apparaître comme ticker dans les features)
        mkt = cfg.get("market_ticker", "")
        if mkt:
            n_before = len(feat)
            feat = feat[feat["Ticker"] != mkt].copy()
            if len(feat) < n_before:
                print(f"  [Filter] Indice {mkt} exclu des features ({n_before - len(feat)} rows)")

        # Features Yahoo Finance (yf_*)
        feat = _build_yf_features_from_db(db, feat, cfg["market_ticker"],
                                          model_start=cfg.get("model_start"))

        # Features macro (macro_vix, macro_vix_chg_20, macro_term_spread)
        feat = _build_macro_features(db, feat, cfg)

        # Features temporelles (delta_*, ma_*)
        feat = _build_temporal_features(feat)

        # Features d'interaction
        feat = _build_interaction_features(feat)

        ef = _calc_earnings_feat(db)
        if not ef.empty:
            feat = _merge_pit(feat, ef)
            print(f"  Earnings: {feat['eps_surprise'].notna().sum():,}/{len(feat):,}")

        qf = _calc_fin_feat(db)
        if not qf.empty:
            feat = _merge_pit(feat, qf)
            print(f"  Financials: {feat['revenue_growth_yoy'].notna().sum():,}/{len(feat):,}")

        nf = len([c for c in FUNDAMENTAL_FEATURE_COLS if c in feat.columns])
        nm = len([c for c in MACRO_FEATURE_COLS if c in feat.columns])
        _save_features(feat, db)
        n_rows = len(feat)
        n_cols = len(feat.columns)
        print(f"  → {n_rows:,} rows × {n_cols} cols ({nf} fund, {nm} macro) → {cfg['db']} [features]")


def read_features(index: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """
    Lit la table 'features' depuis la DB SQLite et retourne un DataFrame.

    Usage dans le notebook :
        from pipeline import read_features
        df_cac = read_features("cac40")
        df_spx = read_features("spx", start="2023-01-01")
    """
    db = _db_path(index)
    if not db.exists():
        raise FileNotFoundError(f"DB introuvable : {db}")
    if not _tbl_exists(db, "features"):
        raise RuntimeError(
            f"Table 'features' absente de {db.name}. "
            "Lance compute_features('{index}') d'abord."
        )

    where_clauses, params = [], []
    if start:
        where_clauses.append("Date >= ?")
        params.append(start)
    if end:
        where_clauses.append("Date <= ?")
        params.append(end)

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT * FROM features {where} ORDER BY Ticker, Date"

    c = sqlite3.connect(db)
    df = pd.read_sql_query(query, c, params=params, parse_dates=["Date"])
    c.close()

    # Exclure l'indice de marché s'il est dans la table (safety net)
    mkt = INDEX_CONFIG[index].get("market_ticker", "")
    if mkt and mkt in df["Ticker"].values:
        df = df[df["Ticker"] != mkt].copy()

    # Ajouter la colonne Universe (compatible toutes versions pipeline)
    if "Universe" not in df.columns:
        df.insert(2, "Universe", index)

    label = INDEX_CONFIG[index]["label"]
    print(f"  [{label}] {len(df):,} rows × {len(df.columns)} cols "
          f"({df['Ticker'].nunique()} tickers, "
          f"{df['Date'].min().date()} → {df['Date'].max().date()})")
    return df


def run_all(index: str = "all", skip_fundamentals: bool = False, force_features: bool = False):
    """
    Pipeline complet. Ne retélécharge rien. Ne recalcule que si nécessaire.

        run_all()                          # tout
        run_all("spx")                     # un index
        run_all(skip_fundamentals=True)    # sans fondamentaux
        run_all(force_features=True)       # force recalcul features
    """
    update_prices(index)
    if not skip_fundamentals:
        update_fundamentals(index)
    compute_features(index, force=force_features)
    print("\n✓ DONE")


def status(index: str = "all"):
    """Diagnostic complet de couverture."""
    for key in _resolve(index):
        cfg = INDEX_CONFIG[key]
        db = _db_path(key)
        if not db.exists():
            print(f"\n  {cfg['label']}: DB inexistante")
            continue

        print(f"\n{'='*50}")
        print(f"  {cfg['label']} — STATUS")
        print(f"{'='*50}")

        n_px = _count(db, "prices")
        n_tk = len(_tickers_in_db(db, "prices"))
        last = _max_date(db, "prices")
        print(f"  Prices:     {n_px:>10,} rows, {n_tk:>4} tickers, last={last}")
        print(f"  Returns:    {_count(db, 'returns'):>10,} rows")
        print(f"  Ratios:     {_count(db, 'ratios'):>10,} tickers")

        # Earnings
        n_er = _count(db, "earnings_history")
        n_er_tk = len(_tickers_in_db(db, "earnings_history"))
        n_er_attempted = len(_tickers_attempted(db, "earnings_history"))
        n_er_empty = n_er_attempted - n_er_tk
        print(f"  Earnings:   {n_er:>10,} rows, {n_er_tk:>4} tickers ({n_er_empty} sans data)")

        # Financials
        n_fi = _count(db, "quarterly_financials")
        n_fi_tk = len(_tickers_in_db(db, "quarterly_financials"))
        n_fi_attempted = len(_tickers_attempted(db, "quarterly_financials"))
        n_fi_empty = n_fi_attempted - n_fi_tk
        print(f"  Financials: {n_fi:>10,} rows, {n_fi_tk:>4} tickers ({n_fi_empty} sans data)")

        if n_fi_tk > 0:
            c = sqlite3.connect(db)
            detail = pd.read_sql_query(
                "SELECT COUNT(*) as n, "
                "SUM(CASE WHEN TotalRevenue IS NOT NULL THEN 1 ELSE 0 END) as has_rev, "
                "SUM(CASE WHEN BookValue IS NOT NULL THEN 1 ELSE 0 END) as has_bv "
                "FROM quarterly_financials", c)
            c.close()
            print(f"            avg {n_fi/n_fi_tk:.1f} quarters/ticker, "
                  f"revenue: {int(detail['has_rev'].iloc[0])}/{n_fi}, "
                  f"book_value: {int(detail['has_bv'].iloc[0])}/{n_fi}")

        if _tbl_exists(db, "features"):
            c = sqlite3.connect(db)
            n_feat = c.execute("SELECT COUNT(*) FROM features").fetchone()[0]
            meta = {r[0]: r[1] for r in c.execute("SELECT Key, Value FROM features_meta").fetchall()}
            c.close()
            n_cols = meta.get("n_cols", "?")
            computed_at = meta.get("computed_at", "?")
            print(f"  Features:   {n_feat:>10,} rows × {n_cols} cols → {cfg['db']} [features] (calc: {computed_at})")
        else:
            print(f"  Features:   table 'features' absente — lance compute_features('{key}')")


def reset_financials(index: str = "all"):
    """Vide financials + log pour re-télécharger."""
    for key in _resolve(index):
        db = _db_path(key)
        if not db.exists(): continue
        c = sqlite3.connect(db)
        c.execute("DELETE FROM quarterly_financials")
        if _tbl_exists(db, "fetch_log"):
            c.execute("DELETE FROM fetch_log WHERE TableName='quarterly_financials'")
        c.commit(); c.close()
        print(f"  {INDEX_CONFIG[key]['label']}: quarterly_financials + log vidées")


def reset_earnings(index: str = "all"):
    """Vide earnings + log pour re-télécharger."""
    for key in _resolve(index):
        db = _db_path(key)
        if not db.exists(): continue
        c = sqlite3.connect(db)
        c.execute("DELETE FROM earnings_history")
        if _tbl_exists(db, "fetch_log"):
            c.execute("DELETE FROM fetch_log WHERE TableName='earnings_history'")
        c.commit(); c.close()
        print(f"  {INDEX_CONFIG[key]['label']}: earnings_history + log vidées")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ML Stock Rank — pipeline")
    ap.add_argument("--index", choices=["spx","cac40","dax","all"], default="all")
    ap.add_argument("--skip-fundamentals", action="store_true")
    ap.add_argument("--features-only", action="store_true")
    ap.add_argument("--force-features", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--reset-financials", action="store_true")
    ap.add_argument("--reset-earnings", action="store_true")
    args, _ = ap.parse_known_args()

    if args.status:
        status(args.index)
    elif args.reset_financials:
        reset_financials(args.index)
    elif args.reset_earnings:
        reset_earnings(args.index)
    elif args.features_only:
        compute_features(args.index, force=args.force_features)
    else:
        run_all(args.index, skip_fundamentals=args.skip_fundamentals,
                force_features=args.force_features)
