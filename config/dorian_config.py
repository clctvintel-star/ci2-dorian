from pathlib import Path
import os

# ============================================
# ROOT
# ============================================

CI2_ROOT = Path(os.environ.get("CI2_ROOT", "/content/drive/MyDrive/CI2"))

# ============================================
# DATA PATHS
# ============================================

PATHS = {
    "dorian_db": CI2_ROOT / "db" / "dorian2",
    "dorian_runs": CI2_ROOT / "db" / "dorian2" / "runs",
    "dorian_brand": CI2_ROOT / "db" / "dorian2" / "inputs" / "brand",

    "qwass": CI2_ROOT / "db" / "qwass2",
    "scum": CI2_ROOT / "db" / "scum2",
    "werk": CI2_ROOT / "db" / "werk2",

    # fallback (legacy)
    "legacy_qwass": CI2_ROOT / "QWASS",
    "legacy_scum": CI2_ROOT / "SCUM",
    "legacy_werk": CI2_ROOT / "WERK",
}

# ============================================
# FUNDS
# ============================================

CANON_FUNDS = [
    "Citadel",
    "Millennium",
    "Two Sigma",
    "D.E. Shaw",
    "Jane Street",
    "Hudson River Trading",
    "Point72",
    "Balyasny",
    "Schonfeld",
    "ExodusPoint",
    "Jump Trading",
]

FUND_PATH_FIX = {
    "Two Sigma": "TwoSigma",
    "D.E. Shaw": "D.E.Shaw",
    "Hudson River Trading": "HudsonRiverTrading",
    "Jane Street": "JaneStreet",
    "Jump Trading": "JumpTrading",
}

FUND_ALIASES = {
    "citadel": "Citadel",
    "millennium": "Millennium",
    "mlp": "Millennium",
    "two sigma": "Two Sigma",
    "twosigma": "Two Sigma",
    "two-sigma": "Two Sigma",
    "de shaw": "D.E. Shaw",
    "d.e. shaw": "D.E. Shaw",
    "deshaw": "D.E. Shaw",
    "jane street": "Jane Street",
    "janestreet": "Jane Street",
    "jane.street": "Jane Street",
    "hrt": "Hudson River Trading",
    "hudson river trading": "Hudson River Trading",
    "point72": "Point72",
    "balyasny": "Balyasny",
    "schonfeld": "Schonfeld",
    "exoduspoint": "ExodusPoint",
    "jump trading": "Jump Trading",
}

SCUM_PACKAGE_DIR = {
    "Citadel": "citadel.scum.package",
    "Millennium": "millennium.scum.package",
    "Two Sigma": "twosigma.scum.package",
    "D.E. Shaw": "deshaw.scum.package",
    "Hudson River Trading": "hudsonrivertrading.scum.package",
    "Jane Street": "janestreet.scum.package",
    "Point72": "point72.scum.package",
    "Balyasny": "balyasny.scum.package",
    "Schonfeld": "schonfeld.scum.package",
    "ExodusPoint": "exoduspoint.scum.package",
    "Jump Trading": "jumptrading.scum.package",
}

# ============================================
# PIPELINE SETTINGS
# ============================================

CHUNKING = {
    "max_words": 180,
    "overlap_words": 40,
    "min_chunk_words": 25,
}

FILTERS = {
    "reputation_months": 18,
    "news_quality_cutoff": 0.65,
}

WEIGHTS = {
    "source_base": {
        "news": 1.00,
        "reddit": 0.85,
        "glassdoor": 0.95,
    }
}
