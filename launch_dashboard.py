"""
launch_dashboard.py
───────────────────
À coller à la fin de ton pipeline, après avoir calculé `ranking`,
`fold_report`, et `med_hat`.

Usage :
    python launch_dashboard.py          ← ouvre le browser auto
    python launch_dashboard.py --no-browser  ← génère le HTML sans ouvrir
"""

import json
import re
import webbrowser
import os
import sys
from datetime import datetime
from pathlib import Path

# ════════════════════════════════════════════════════════════════
# 1. NOMS DES SOCIÉTÉS
# ════════════════════════════════════════════════════════════════

TICKER_NAMES = {
    "ADS.DE": "Adidas", "AIR.DE": "Airbus", "ALV.DE": "Allianz",
    "BAS.DE": "BASF", "BAYN.DE": "Bayer", "BMW.DE": "BMW",
    "BNR.DE": "Brenntag", "CON.DE": "Continental", "1COV.DE": "Covestro",
    "DBK.DE": "Deutsche Bank", "CBK.DE": "Commerzbank",
    "DB1.DE": "Deutsche Börse", "DHL.DE": "DHL Group",
    "DTE.DE": "Deutsche Telekom", "DTG.DE": "Daimler Truck",
    "EOAN.DE": "E.ON", "FME.DE": "Fresenius Med.",
    "FRE.DE": "Fresenius", "G1A.DE": "GEA Group",
    "G24.DE": "Scout24", "HNR1.DE": "Hannover Rück",
    "HEI.DE": "HeidelbergMat.", "HEN3.DE": "Henkel",
    "IFX.DE": "Infineon", "MBG.DE": "Mercedes-Benz",
    "MRK.DE": "Merck", "MTX.DE": "MTU Aero",
    "MUV2.DE": "Munich Re", "PAH3.DE": "Porsche Hold.",
    "RHM.DE": "Rheinmetall", "RWE.DE": "RWE", "SAP.DE": "SAP",
    "SHL.DE": "Siemens Health.", "SIE.DE": "Siemens",
    "SRT3.DE": "Sartorius", "SY1.DE": "Symrise",
    "VNA.DE": "Vonovia", "VOW3.DE": "Volkswagen",
    "ZAL.DE": "Zalando", "P911.DE": "Porsche AG",
    "ENR.DE": "Siemens Energy", "QIA.DE": "Qiagen",
    "BEI.DE": "Beiersdorf", "AIR.PA": "Airbus (PA)",
    "EN.PA": "Bouygues", "GLE.PA": "Soc. Générale", "ACA.PA": "Crédit Agricole",
    "EL.PA": "EssilorLuxottica", "LR.PA": "Legrand", "BNP.PA": "BNP Paribas",
    "DG.PA": "Vinci", "MC.PA": "LVMH", "RMS.PA": "Hermès", "CS.PA": "AXA",
    "SAF.PA": "Safran", "AIR.PA": "Airbus", "ORA.PA": "Orange", "BN.PA": "Danone",
    "SGO.PA": "Saint-Gobain", "BVI.PA": "Bureau Veritas", "URW.PA": "Unibail",
    "AC.PA": "Accor", "AI.PA": "Air Liquide", "MT.AS": "ArcelorMittal",
    "CA.PA": "Carrefour", "SU.PA": "Schneider", "SAN.PA": "Sanofi",
    "ML.PA": "Michelin", "OR.PA": "L'Oréal", "RNO.PA": "Renault",
    "VIE.PA": "Veolia", "ENGI.PA": "Engie", "ERF.PA": "Eurofins",
    "KER.PA": "Kering", "TTE.PA": "TotalEnergies", "RI.PA": "Pernod Ricard",
    "PUB.PA": "Publicis", "EDEN.PA": "Edenred", "HO.PA": "Thales",
    "DSY.PA": "Dassault Systèmes", "CAP.PA": "Capgemini",
    "STMPA.PA": "STMicro", "STLAP.PA": "Stellantis", "TEP.PA": "Teleperformance",
    # ── Mega/Large caps souvent sans suffixe ──
    "AAPL": "Apple", "ABBV": "AbbVie", "ABT": "Abbott Labs",
    "ACN": "Accenture", "ADBE": "Adobe", "ADI": "Analog Devices",
    "AMAT": "Applied Materials", "AMD": "AMD", "AMGN": "Amgen",
    "AMT": "American Tower", "AMZN": "Amazon", "AVGO": "Broadcom",
    "AXP": "American Express", "BA": "Boeing", "BAC": "Bank of America",
    "BLK": "BlackRock", "C": "Citigroup", "CAT": "Caterpillar",
    "CMCSA": "Comcast", "COP": "ConocoPhillips", "COST": "Costco",
    "CRM": "Salesforce", "CSCO": "Cisco", "CVX": "Chevron",
    "DAL": "Delta Air Lines", "DHR": "Danaher", "DIS": "Disney",
    "DUK": "Duke Energy", "EL": "Estée Lauder", "EOG": "EOG Resources",
    "EQIX": "Equinix", "F": "Ford", "GE": "GE Aerospace",
    "GM": "General Motors", "GOOG": "Alphabet C", "GOOGL": "Alphabet A",
    "GS": "Goldman Sachs", "HD": "Home Depot", "HON": "Honeywell",
    "IBM": "IBM", "INTC": "Intel", "ISRG": "Intuitive Surgical",
    "JNJ": "Johnson & Johnson", "JPM": "JPMorgan", "KLAC": "KLA Corp",
    "KO": "Coca-Cola", "LIN": "Linde", "LLY": "Eli Lilly",
    "LMT": "Lockheed Martin", "LOW": "Lowe's", "LRCX": "Lam Research",
    "MA": "Mastercard", "MCD": "McDonald's", "MDLZ": "Mondelez",
    "META": "Meta", "MMM": "3M", "MRK": "Merck US",
    "MS": "Morgan Stanley", "MSFT": "Microsoft", "MU": "Micron",
    "NEE": "NextEra Energy", "NFLX": "Netflix", "NKE": "Nike",
    "NOC": "Northrop Grumman", "NVDA": "NVIDIA", "ORCL": "Oracle",
    "PANW": "Palo Alto Networks", "PEP": "PepsiCo", "PFE": "Pfizer",
    "PG": "Procter & Gamble", "PH": "Parker-Hannifin", "PLD": "Prologis",
    "PM": "Philip Morris", "PNC": "PNC Financial", "QCOM": "Qualcomm",
    "REGN": "Regeneron", "RTX": "RTX Corp", "SBUX": "Starbucks",
    "SLB": "Schlumberger", "SO": "Southern Co.", "SPGI": "S&P Global",
    "SYK": "Stryker", "T": "AT&T", "TGT": "Target",
    "TMO": "Thermo Fisher", "TMUS": "T-Mobile", "TSLA": "Tesla",
    "TXN": "Texas Instruments", "UAL": "United Airlines", "UBER": "Uber",
    "UNH": "UnitedHealth", "UPS": "UPS", "USB": "U.S. Bancorp",
    "V": "Visa", "VZ": "Verizon", "WFC": "Wells Fargo",
    "WMT": "Walmart", "XOM": "ExxonMobil",
    # ── Autres ──
    "COIN": "Coinbase", "SNDK": "SanDisk", "ARE": "Alexandria RE",
    "FIX": "Comfort Systems", "MRNA": "Moderna", "MOS": "Mosaic",
    "BAX": "Baxter", "HUM": "Humana", "HOOD": "Robinhood",
    "Q": "Quintiles", "PSKY": "Porch Group", "CIEN": "Ciena",
    "IVZ": "Invesco", "SWKS": "Skyworks", "CCL": "Carnival",
    "INCY": "Incyte", "EXPE": "Expedia", "BLDR": "Builders First",
    "ON": "ON Semiconductor", "CNC": "Centene", "DOW": "Dow Inc.",
    "NRG": "NRG Energy", "APO": "Apollo Global", "PTC": "PTC Inc.",
    "IP": "International Paper", "ALB": "Albemarle",
    "J": "Jacobs Solutions", "LUV": "Southwest Airlines",
    "CSGP": "CoStar Group", "PYPL": "PayPal", "APTV": "Aptiv",
    "GLW": "Corning", "ZBRA": "Zebra Tech", "SW": "Smurfit Westrock",
    "FISV": "Fiserv", "DLTR": "Dollar Tree", "DECK": "Deckers",
    "WDC": "Western Digital", "TER": "Teradyne", "WBD": "Warner Bros.",
    "TTD": "The Trade Desk", "IRM": "Iron Mountain", "FFIV": "F5 Inc.",
    "AXON": "Axon Enterprise", "PLTR": "Palantir", "APA": "APA Corp",
    "IBKR": "Interactive Brokers", "HPE": "HP Enterprise",
    "MCHP": "Microchip Tech", "EME": "EMCOR", "TPR": "Tapestry",
    "NCLH": "Norwegian Cruise", "SYF": "Synchrony", "FSLR": "First Solar",
    "GNRC": "Generac", "CARR": "Carrier Global", "XYZ": "Block Inc.",
    "TRMB": "Trimble", "RCL": "Royal Caribbean", "ELV": "Elevance",
    "AKAM": "Akamai", "NVR": "NVR Inc.", "COF": "Capital One",
    "JBHT": "J.B. Hunt", "DELL": "Dell", "DASH": "DoorDash",
    "GRMN": "Garmin", "MPWR": "Monolithic Power", "NWS": "News Corp B",
    "LII": "Lennox", "DG": "Dollar General", "CHTR": "Charter Comm.",
    "EQT": "EQT Corp", "PAYC": "Paycom", "NEM": "Newmont",
    "DDOG": "Datadog", "HPQ": "HP Inc.", "NUE": "Nucor",
    "FCX": "Freeport-McMoRan", "WST": "West Pharma",
    "CRL": "Charles River", "RL": "Ralph Lauren", "UHS": "Universal Health",
    "URI": "United Rentals", "VTRS": "Viatris", "BKNG": "Booking Holdings",
    "STLD": "Steel Dynamics", "POOL": "Pool Corp", "STX": "Seagate",
    "BX": "Blackstone", "NOW": "ServiceNow", "NTAP": "NetApp",
    "HAL": "Halliburton", "CMG": "Chipotle", "CMI": "Cummins",
    "TEL": "TE Connectivity", "KEYS": "Keysight", "ETN": "Eaton",
    "FDX": "FedEx", "CDW": "CDW Corp", "WSM": "Williams-Sonoma",
    "FICO": "Fair Isaac", "APH": "Amphenol", "SMCI": "Super Micro",
    "HST": "Host Hotels", "GPN": "Global Payments",
    "BG": "Bunge", "WDAY": "Workday", "NWSA": "News Corp A",
    "ZTS": "Zoetis", "DVA": "DaVita", "TECH": "Bio-Techne",
    "ALL": "Allstate", "EXE": "Expand Energy", "KKR": "KKR & Co.",
    "TPL": "Texas Pacific Land", "TROW": "T. Rowe Price",
    "AES": "AES Corp", "CEG": "Constellation Energy",
    "LULU": "Lululemon", "DD": "DuPont", "PWR": "Quanta Services",
    "APP": "Applovin", "HII": "Huntington Ingalls",
    "JBL": "Jabil", "HCA": "HCA Healthcare", "AOS": "A.O. Smith",
    "LYB": "LyondellBasell", "EXPD": "Expeditors", "CRWD": "CrowdStrike",
    "FTNT": "Fortinet", "MAS": "Masco", "CHRW": "C.H. Robinson",
    "TDY": "Teledyne", "MTB": "M&T Bank", "ULTA": "Ulta Beauty",
    "LVS": "Las Vegas Sands", "NXPI": "NXP Semi", "VST": "Vistra",
    "CDNS": "Cadence Design", "IR": "Ingersoll Rand",
    "HBAN": "Huntington Bancshares", "IDXX": "Idexx Labs",
    "CRH": "CRH plc", "HUBB": "Hubbell", "ANET": "Arista Networks",
    "MOH": "Molina Healthcare", "CPAY": "Corpay",
    "EPAM": "EPAM Systems", "DLR": "Digital Realty",
    "BXP": "Boston Properties", "CFG": "Citizens Financial",
    "SWK": "Stanley Black&Decker", "COO": "Cooper Companies",
    "IT": "Gartner", "ARES": "Ares Management", "PNR": "Pentair",
    "TKO": "TKO Group", "PKG": "Packaging Corp",
    "CBRE": "CBRE Group", "TTWO": "Take-Two", "EG": "Everest Group",
    "FITB": "Fifth Third", "DOC": "Healthpeak", "SOLV": "Solventum",
    "TFC": "Truist Financial", "EBAY": "eBay", "CINF": "Cincinnati Financial",
    "DXCM": "Dexcom", "LDOS": "Leidos", "ERIE": "Erie Indemnity",
    "KEY": "KeyCorp", "ALLE": "Allegion", "PHM": "PulteGroup",
    "EMR": "Emerson Electric", "DVN": "Devon Energy",
    "RJF": "Raymond James", "RF": "Regions Financial",
    "PODD": "Insulet", "ZBH": "Zimmer Biomet", "RVTY": "Revvity",
    "WYNN": "Wynn Resorts", "APD": "Air Products",
    "MTD": "Mettler-Toledo", "TRV": "Travelers", "CI": "Cigna",
    "VLO": "Valero Energy", "AVB": "AvalonBay", "INTU": "Intuit",
    "A": "Agilent", "OXY": "Occidental", "GEHC": "GE HealthCare",
    "NTRS": "Northern Trust", "MET": "MetLife", "STT": "State Street",
    "MTCH": "Match Group", "BKR": "Baker Hughes",
    "EFX": "Equifax", "AIG": "AIG", "GPC": "Genuine Parts",
    "AJG": "Arthur J. Gallagher", "PCAR": "PACCAR",
    "ESS": "Essex Property", "WY": "Weyerhaeuser",
    "ROST": "Ross Stores", "LYV": "Live Nation",
    "WAB": "Wabtec", "ALGN": "Align Technology",
    "KIM": "Kimco Realty", "TXT": "Textron", "BRO": "Brown & Brown",
    "MAR": "Marriott", "SYY": "Sysco", "CCI": "Crown Castle",
    "ODFL": "Old Dominion", "AFL": "Aflac", "DHI": "D.R. Horton",
    "ADSK": "Autodesk", "CF": "CF Industries", "LW": "Lamb Weston",
    "CBOE": "Cboe Global", "MCK": "McKesson", "DOV": "Dover Corp",
    "TYL": "Tyler Technologies", "HWM": "Howmet Aerospace",
    "AMP": "Ameriprise", "HSIC": "Henry Schein", "TT": "Trane Tech.",
    "KVUE": "Kenvue", "PPG": "PPG Industries", "GD": "General Dynamics",
    "SNPS": "Synopsys", "FAST": "Fastenal", "ES": "Eversource",
    "CVS": "CVS Health", "GEV": "GE Vernova", "ITW": "Illinois Tool",
    "IEX": "IDEX Corp", "BBY": "Best Buy", "GDDY": "GoDaddy",
    "WRB": "W.R. Berkley", "FDS": "FactSet", "EXR": "Extra Space",
    "DRI": "Darden Restaurants", "BF-B": "Brown-Forman",
    "FRT": "Federal Realty", "IQV": "IQVIA", "MGM": "MGM Resorts",
    "TSN": "Tyson Foods", "TSCO": "Tractor Supply",
    "CTSH": "Cognizant", "HAS": "Hasbro", "KMB": "Kimberly-Clark",
    "GEN": "Gen Digital", "WAT": "Waters Corp", "PCG": "PG&E",
    "JCI": "Johnson Controls", "ACGL": "Arch Capital",
    "EA": "Electronic Arts", "FTV": "Fortive", "VMC": "Vulcan Materials",
    "LHX": "L3Harris", "GILD": "Gilead", "L": "Loews Corp",
    "CHD": "Church & Dwight", "ROK": "Rockwell Automation",
    "PSX": "Phillips 66", "NDSN": "Nordson", "MLM": "Martin Marietta",
    "SRE": "Sempra", "BR": "Broadridge", "CPB": "Campbell Soup",
    "ECL": "Ecolab", "OMC": "Omnicom", "GWW": "W.W. Grainger",
    "HSY": "Hershey", "DPZ": "Domino's", "LH": "LabCorp",
    "IFF": "Intl Flavors", "STZ": "Constellation Brands",
    "HRL": "Hormel", "PFG": "Principal Financial",
    "UDR": "UDR Inc.", "PRU": "Prudential", "HLT": "Hilton",
    "CTAS": "Cintas", "FIS": "FIS Inc.", "O": "Realty Income",
    "BEN": "Franklin Templeton", "AME": "AMETEK",
    "CLX": "Clorox", "PAYX": "Paychex", "BDX": "Becton Dickinson",
    "EQR": "Equity Residential", "YUM": "Yum! Brands",
    "XYL": "Xylem", "RMD": "ResMed", "GIS": "General Mills",
    "SNA": "Snap-on", "VLTO": "Veralto", "PGR": "Progressive",
    "SPG": "Simon Property", "CTVA": "Corteva",
    "ADP": "Automatic Data", "VRSN": "VeriSign",
    "FANG": "Diamondback Energy", "ETR": "Entergy",
    "ABNB": "Airbnb", "FOXA": "Fox Corp A",
    "VRSK": "Verisk", "TRGP": "Targa Resources",
    "CPT": "Camden Property", "SJM": "J.M. Smucker",
    "KR": "Kroger", "ADM": "Archer-Daniels",
    "WMB": "Williams Companies", "VRTX": "Vertex Pharma",
    "MDT": "Medtronic", "NSC": "Norfolk Southern",
    "D": "Dominion Energy", "AWK": "American Water",
    "XEL": "Xcel Energy", "MSCI": "MSCI Inc.",
    "REG": "Regency Centers", "MRSH": "Marsh McLennan",
    "PSA": "Public Storage", "UNP": "Union Pacific",
    "HIG": "Hartford Financial", "KMI": "Kinder Morgan",
    "MO": "Altria", "CSX": "CSX Corp", "BALL": "Ball Corp",
    "BK": "Bank of New York", "TAP": "Molson Coors",
    "ROP": "Roper Technologies", "LNT": "Alliant Energy",
    "CAG": "Conagra", "VICI": "VICI Properties",
    "EVRG": "Evergy", "INVH": "Invitation Homes",
    "AON": "Aon plc", "AIZ": "Assurant",
    "GL": "Globe Life", "OTIS": "Otis Worldwide",
    "WM": "Waste Management", "ROL": "Rollins",
    "BIIB": "Biogen", "BSX": "Boston Scientific",
    "AVY": "Avery Dennison", "MCO": "Moody's",
    "CMS": "CMS Energy", "EW": "Edwards Lifesciences",
    "PEG": "PSEG", "CTRA": "Coterra Energy",
    "AZO": "AutoZone", "FE": "FirstEnergy",
    "SCHW": "Charles Schwab", "NDAQ": "Nasdaq Inc.",
    "CAH": "Cardinal Health", "JKHY": "Jack Henry",
    "NI": "NiSource", "MAA": "Mid-America Apt.",
    "KHC": "Kraft Heinz", "ATO": "Atmos Energy",
    "CL": "Colgate-Palmolive", "CPRT": "Copart",
    "MKC": "McCormick", "EIX": "Edison Intl.",
    "RSG": "Republic Services", "OKE": "ONEOK",
    "ICE": "Intercontinental Exch.", "DTE": "DTE Energy",
    "FOX": "Fox Corp B", "CVNA": "Carvana",
    "WTW": "WTW plc", "KDP": "Keurig Dr Pepper",
    "PNW": "Pinnacle West", "DGX": "Quest Diagnostics",
    "EXC": "Exelon", "MSI": "Motorola Solutions",
    "ORLY": "O'Reilly Auto", "SHW": "Sherwin-Williams",
    "TJX": "TJX Companies", "CNP": "CenterPoint",
    "VTR": "Ventas", "CME": "CME Group",
    "AEP": "American Electric Power", "WELL": "Welltower",
    "BRK-B": "Berkshire B", "AEE": "Ameren",
    "WEC": "WEC Energy", "BMY": "Bristol-Myers",
    "PPL": "PPL Corp", "DE": "Deere & Co.",
    "TDG": "TransDigm", "AMCR": "Amcor",
    "STE": "Steris", "SBAC": "SBA Communications",
    "ED": "Consolidated Edison", "CB": "Chubb",
    "MNST": "Monster Beverage", "MPC": "Marathon Petroleum",
    "LEN": "Lennar", "COR": "Cencora",
    "HOLX": "Hologic",
}

# ════════════════════════════════════════════════════════════════
# 2. ROUTING UNIVERS  — défini APRÈS les 3 dicts ci-dessus
# ════════════════════════════════════════════════════════════════

UNIVERSE_NAMES = {
    "cac40": TICKER_NAMES,      "CAC40": TICKER_NAMES,
    "dax":   TICKER_NAMES,  "DAX":   TICKER_NAMES,
    "spx":   TICKER_NAMES,  "SPX":   TICKER_NAMES,
    "sp500": TICKER_NAMES,
}

UNIVERSE_TEMPLATES = {
    "cac40": "cac40_dashboard_template.html", "CAC40": "cac40_dashboard_template.html",
    "dax":   "dax_dashboard_template.html",   "DAX":   "dax_dashboard_template.html",
    "spx":   "spx_dashboard_template.html",   "SPX":   "spx_dashboard_template.html",
    "sp500": "spx_dashboard_template.html",
}

UNIVERSE_OUTPUTS = {
    "cac40": "cac40_dashboard.html", "CAC40": "cac40_dashboard.html",
    "dax":   "dax_dashboard.html",   "DAX":   "dax_dashboard.html",
    "spx":   "spx_dashboard.html",   "SPX":   "spx_dashboard.html",
    "sp500": "spx_dashboard.html",
}

# ════════════════════════════════════════════════════════════════
# 3. EXTRACTION FEATURES
# ════════════════════════════════════════════════════════════════
def extract_top_features(fold_report_raw: str, top_n: int = 15) -> list[dict]:
    from collections import Counter
    counter = Counter()
    blocks = re.findall(
        r'\[Importance top 15\]:\s*(.*?)(?=\n\[|\Z)', fold_report_raw, re.DOTALL
    )
    for block in blocks:
        lines = block.strip().split('\n')
        for line in lines:
            m = re.match(r'\s*(\S+)\s+\d+', line)
            if m:
                counter[m.group(1)] += 1
    return [{"name": k, "count": v} for k, v in counter.most_common(top_n)]


def _top_features_from_imp(imp_df, top_n: int = 15) -> list[dict]:
    """Build top features list from walk-forward importance DataFrame.

    imp_df: rows = folds, columns = feature names, values = LightGBM importances.
    Counts how many folds each feature appears in the top 15 by importance.
    """
    from collections import Counter
    counter = Counter()
    for _, row in imp_df.iterrows():
        row_sorted = row.dropna().sort_values(ascending=False)
        for feat in row_sorted.head(15).index:
            counter[feat] += 1
    return [{"name": k, "count": v} for k, v in counter.most_common(top_n)]


# ════════════════════════════════════════════════════════════════
# 4. FONCTION PRINCIPALE
# ════════════════════════════════════════════════════════════════
def generate_dashboard(
    ranking,
    fold_report,
    med_hat: float,
    universe: str = "CAC40",
    n_features: int = 53,
    fold_report_raw: str = "",
    imp_df=None,
    template_path: str = None,
    output_path: str = None,
    open_browser: bool = True,
):
    # ── A. Ranking JSON ──
    top_pct = max(1, int(len(ranking) * 0.10))
    names_dict = UNIVERSE_NAMES.get(universe, TICKER_NAMES)
    ranking_data = []
    for i, row in ranking.iterrows():
        ticker = str(row["Ticker"])
        ranking_data.append({
            "ticker":   ticker,
            "name":     names_dict.get(ticker, ""),
            "pred_abs": round(float(row["pred_abs_%_hat"]), 4),
            "pred_exc": round(float(row["pred_excess_%_approx"]), 4),
        })

    # ── B. Folds JSON ──
    folds_data = []
    ok = fold_report[fold_report["status"] == "ok"].reset_index(drop=True)
    for i, row in ok.iterrows():
        ic_s_val = float(row["ic_spearman"])
        ic_p_val = float(row["ic_pearson"])
        ls_val   = float(row["ls_spread"])
        import math
        folds_data.append({
            "id":  int(row.get("fold_id", i)),
            "ts":  str(row["test_start"])[:10],
            "te":  str(row["test_end"])[:10],
            "ic_s": None if math.isnan(ic_s_val) else round(ic_s_val, 6),
            "ic_p": None if math.isnan(ic_p_val) else round(ic_p_val, 6),
            "ls":   None if math.isnan(ls_val)   else round(ls_val,   6),
            "bi":   int(row.get("best_iteration", 0)),
        })

    # ── C. Features (from imp_df or fold_report_raw) ──
    top_feat = []
    if imp_df is not None and len(imp_df) > 0:
        top_feat = _top_features_from_imp(imp_df)
    if not top_feat:
        top_feat = extract_top_features(fold_report_raw)

    # ── D. Métadonnées ──
    if "Date" in ranking.columns:
        signal_date = str(ranking["Date"].iloc[0])[:10]
    else:
        signal_date = datetime.today().strftime("%Y-%m-%d")

    meta = {
        "med_hat":     round(float(med_hat) * 100, 4),
        "n_top":       top_pct,
        "universe":    universe,
        "signal_date": signal_date,
    }

    # ── E. Résoudre les chemins ──
    if template_path is None:
        template_path = UNIVERSE_TEMPLATES.get(universe, "cac40_dashboard_template.html")
    if output_path is None:
        output_path = UNIVERSE_OUTPUTS.get(universe, f"{universe.lower()}_dashboard.html")

    template = Path(template_path).read_text(encoding="utf-8")

    # ── F. Métadonnées statiques ──
    template = template.replace("{{UNIVERSE}}",    universe)
    template = template.replace("{{N_TICKERS}}",   str(len(ranking)))
    template = template.replace("{{SIGNAL_DATE}}", signal_date)
    template = template.replace("{{N_FEATURES}}",  str(n_features))
    template = template.replace("{{N_FOLDS}}",     str(len(folds_data)))
    template = template.replace("{{N_TOP}}",       str(top_pct))
    template = template.replace("{{GEN_TIME}}",    datetime.now().strftime("%Y-%m-%d %H:%M"))

    # ── G. Injection JS ──
    inject_block = f"""/* INJECT_START */
const RANKING  = {json.dumps(ranking_data, ensure_ascii=False)};
const FOLDS    = {json.dumps(folds_data,   ensure_ascii=False)};
const TOP_FEAT = {json.dumps(top_feat,     ensure_ascii=False)};
const META     = {json.dumps(meta,         ensure_ascii=False)};
/* INJECT_END */"""

    template = re.sub(
        r'/\* INJECT_START \*/.*?/\* INJECT_END \*/',
        inject_block,
        template,
        flags=re.DOTALL
    )

    # ── H. Écriture ──
    Path(output_path).write_text(template, encoding="utf-8")
    print(f"✅ Dashboard généré → {os.path.abspath(output_path)}")

    if open_browser:
        webbrowser.open(f"file://{os.path.abspath(output_path)}")
        print("🌐 Dashboard ouvert dans le navigateur.")

    return os.path.abspath(output_path)


# ════════════════════════════════════════════════════════════════
# 5. MODE STANDALONE (test avec données exemples)
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import pandas as pd
    import numpy as np

    ranking_test = pd.DataFrame({
        "Ticker": ["EN.PA","GLE.PA","ACA.PA","EL.PA","LR.PA","BNP.PA","DG.PA","MC.PA",
                   "RMS.PA","CS.PA","SAF.PA","AIR.PA","ORA.PA","BN.PA","SGO.PA","BVI.PA",
                   "URW.PA","AC.PA","AI.PA","MT.AS","CA.PA","SU.PA","SAN.PA","ML.PA",
                   "OR.PA","RNO.PA","VIE.PA","ENGI.PA","ERF.PA","KER.PA","TTE.PA","RI.PA",
                   "PUB.PA","EDEN.PA","HO.PA","DSY.PA","CAP.PA","STMPA.PA","STLAP.PA","TEP.PA"],
        "pred_abs_%_hat": [3.24,3.16,2.86,1.83,1.81,1.79,1.39,1.39,1.26,1.12,0.99,0.74,
                           0.74,0.64,0.62,0.54,0.53,0.45,0.27,0.20,0.14,-0.01,-0.26,-0.26,
                           -0.36,-0.42,-0.50,-0.62,-0.73,-0.74,-0.96,-1.25,-1.35,-1.84,
                           -1.94,-2.14,-2.22,-2.70,-3.32,-3.67],
        "pred_excess_%_approx": [2.94,2.86,2.56,1.53,1.52,1.50,1.10,1.09,0.97,0.83,0.70,
                                  0.45,0.44,0.35,0.33,0.25,0.24,0.15,-0.02,-0.09,-0.15,-0.30,
                                  -0.55,-0.55,-0.64,-0.71,-0.79,-0.91,-1.02,-1.03,-1.25,-1.54,
                                  -1.64,-2.12,-2.22,-2.50,-2.98,-3.60,-3.95],
    })

    fold_report_test = pd.DataFrame({
        "fold_id":        range(12),
        "test_start":     ["2025-01-02","2025-02-03","2025-03-03","2025-04-01","2025-05-01",
                           "2025-06-02","2025-07-01","2025-08-01","2025-09-01","2025-10-01",
                           "2025-11-03","2025-12-01"],
        "test_end":       ["2025-07-01","2025-08-01","2025-09-01","2025-10-01","2025-10-31",
                           "2025-12-01","2025-12-31","2025-12-31","2025-12-31","2025-12-31",
                           "2025-12-31","2025-12-31"],
        "ic_spearman":    [0.0685,-0.0021,0.0139,0.0669,0.1098,0.0932,0.1031,0.0879,
                           0.1154,0.1555,0.2246,0.2605],
        "ic_pearson":     [0.0378,-0.0011,0.0262,0.0709,0.1034,0.0979,0.1343,0.0971,
                           0.1256,0.2205,0.3503,0.3674],
        "ls_spread":      [0.0083,0.0040,0.0027,0.0112,0.0221,0.0207,0.0171,0.0135,
                           0.0138,0.0195,0.0372,0.0363],
        "best_iteration": [138,16,7,24,2,68,185,394,314,152,12,20],
        "status":         ["ok"]*12,
    })

    open_browser = "--no-browser" not in sys.argv

    generate_dashboard(
        ranking       = ranking_test,
        fold_report   = fold_report_test,
        med_hat       = 0.0029,
        universe      = "cac40",
        n_features    = 53,
        template_path = "cac40_dashboard_template.html",
        output_path   = "cac40_dashboard.html",
        open_browser  = open_browser,
    )
