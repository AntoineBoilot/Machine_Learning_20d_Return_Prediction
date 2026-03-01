# ML Stock Rank

A machine learning stock ranking pipeline with HTML dashboards to visualize signals across three equity universes:

- CAC 40
- DAX
- S&P 500

The project combines:

- a data pipeline for collecting and updating market and fundamental data
- a feature engineering engine backed by SQLite
- a LightGBM modeling notebook with walk forward validation
- an interactive HTML dashboard generator

## What the project does

1. Updates prices, ratios, and macro variables for each universe
2. Fetches earnings and financial data when available
3. Builds technical, time series, macro, and fundamental features
4. Trains a ranking model in the notebook with walk forward validation
5. Produces a stock ranking for the latest available date
6. Generates an HTML dashboard with top picks, ranking KPIs, and fold level statistics

## Repository structure

### `pipeline.py`

Main project file.

It handles:

- ticker collection for `spx`, `cac40`, and `dax`
- incremental price updates through Yahoo Finance
- fundamental and earnings updates
- feature computation and storage in SQLite databases
- feature loading for the notebook
- diagnostic and reset commands

Main functions:

- `run_all(index="all")`: runs the full pipeline
- `update_prices(index="all")`: updates prices, returns, ratios, and macro data
- `update_fundamentals(index="all")`: updates earnings and financials
- `compute_features(index="all")`: recomputes features when needed
- `read_features(index, start=None, end=None)`: loads features into a DataFrame
- `status(index="all")`: prints table status and data coverage
- `reset_financials(index="all")`: clears financial tables
- `reset_earnings(index="all")`: clears earnings tables

The script avoids re downloading data already stored in the database and skips feature recomputation when the SQL table is already up to date.

### `ML_Stock_Rank.ipynb`

Main research and model execution notebook.

It includes:

- loading features from SQLite
- target construction
- cross sectional normalization
- ranking metrics such as Spearman IC and Pearson IC
- feature selection tools
- `LightGBM` model definition
- walk forward fold generation
- final training and ranking on the latest date
- retrieval of realized returns
- separate execution for CAC 40, DAX, and SPX
- calls to `generate_dashboard(...)` to produce the final dashboards

The notebook starts with `run_all()` to refresh the data before the modeling stage.

### `launch_dashboard.py`

HTML dashboard generation script.

It:

- converts the notebook `ranking` output into JSON injected into the template
- converts the `fold_report` into fold level performance data
- extracts the top important features
- automatically selects the right template based on the universe
- writes the final HTML file
- can open the dashboard in the browser

Main function:

- `generate_dashboard(ranking, fold_report, med_hat, universe=..., n_features=...)`

Templates used:

- `cac40_dashboard_template.html`
- `dax_dashboard_template.html`
- `spx_dashboard_template.html`

Generated outputs:

- `cac40_dashboard.html`
- `dax_dashboard.html`
- `spx_dashboard.html`

### `cac40_dashboard_template.html`

HTML template for the CAC 40 dashboard.

It displays:

- ranking KPIs
- the top 10% of stocks
- a full ranking table
- walk forward performance by fold
- several Chart.js charts

### `dax_dashboard_template.html`

HTML template for the DAX dashboard.

Same logic as the CAC 40 template, with styling adapted to the DAX universe.

### `spx_dashboard_template.html`

HTML template for the S&P 500 dashboard.

Same logic, with a broader universe. The header is designed for 503 tickers and a top 10% bucket of 50 stocks.

## Data and storage

The project stores data in local SQLite databases, one per universe.

Examples of expected files:

- `sp500_data.db` or `spx_data.db`
- `cac40_data.db`
- `dax_data.db`

Main tables include:

- `prices`
- `returns`
- `ratios`
- `earnings_history`
- `quarterly_financials`
- `features`
- `features_meta`
- `fetch_log`

## Installation

Create a Python environment, then install the main dependencies.

```bash
pip install pandas numpy yfinance scipy lightgbm notebook
```

Depending on your setup, you may also need:

```bash
pip install jupyterlab
```

## Running the project

### 1. Update data and recompute features

```bash
python pipeline.py
```

For a single universe:

```bash
python pipeline.py --index spx
python pipeline.py --index cac40
python pipeline.py --index dax
```

Without fundamentals:

```bash
python pipeline.py --skip-fundamentals
```

Feature recomputation only:

```bash
python pipeline.py --features-only
```

Force feature recomputation:

```bash
python pipeline.py --features-only --force-features
```

Check database status:

```bash
python pipeline.py --status
```

Reset fundamental tables:

```bash
python pipeline.py --reset-financials
python pipeline.py --reset-earnings
```

### 2. Run the notebook

Open `ML_Stock_Rank.ipynb` in Jupyter and run the cells.

```bash
jupyter notebook
```

The notebook:

- loads features from SQLite
- prepares the data
- trains the models
- generates the final ranking
- exports the dashboards

### 3. Generate an HTML dashboard

In the notebook, dashboard export is done through `generate_dashboard(...)`.

You can also test `launch_dashboard.py` as a standalone script:

```bash
python launch_dashboard.py
```

Without automatically opening the browser:

```bash
python launch_dashboard.py --no-browser
```

## Dashboard output

Each dashboard includes:

- a KPI summary such as average IC, long short spread, and `med_hat`
- the strongest long ideas
- a full ranking table
- walk forward validation statistics
- scatter, ranking, and top feature charts

The templates use `Chart.js` and dynamically inject:

- `RANKING`
- `FOLDS`
- `TOP_FEAT`
- `META`

## Recommended workflow

1. Run `pipeline.py`
2. Check `python pipeline.py --status`
3. Open `ML_Stock_Rank.ipynb`
4. Run the model for each universe
5. Generate the HTML dashboards
6. Share the HTML files or publish them through GitHub Pages

## Limitations

- Part of the data comes from Yahoo Finance, so coverage may vary across tickers
- Some fundamental data may be missing
- The dashboards are informational only and do not constitute investment advice
- Past performance does not guarantee future results

## Suggested improvements

- Add a `requirements.txt`
- Add automatic CSV export for model results
- Add unit tests for core functions
- Add a single script to run pipeline, model, and dashboard export end to end
- Add an automatic publishing mode for GitHub Pages

## Disclaimer

This project is a quantitative analysis and visualization tool. It is provided for informational purposes only.

Nothing in this repository constitutes investment advice.
