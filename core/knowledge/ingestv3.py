import pandas as pd
import requests
import re
import os
import io
import unicodedata
from bs4 import BeautifulSoup

# -------------------------
# Cleaning helpers (keep your old behavior)
# -------------------------
def normalize_text(text):
    """Strips accents and normalizes whitespace for reliable matching."""
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text

def clean_wiki_artifacts(text):
    """Removes [1], [editar], etc; collapses whitespace; strips NBSP."""
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    text = re.sub(r"\[[^\]]*\]", "", text)  # remove bracketed refs/labels
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

# -------------------------
# Fetch full page via Wikipedia Parse API (more stable than view-source HTML)
# -------------------------
def fetch_wikipedia_html_via_api(page_title: str) -> str:
    api = "https://es.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": page_title,
        "prop": "text",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    headers = {"User-Agent": "Mozilla/5.0 (compatible; latam-localization/1.0)"}
    r = requests.get(api, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["parse"]["text"]

# -------------------------
# Find a reasonable "subtable label" by taking the nearest preceding heading
# (helps you map to "Utensilios y objetos cotidianos (20)" etc.)
# -------------------------
def nearest_heading_text(table_tag):
    for prev in table_tag.find_all_previous(["h2", "h3", "h4"], limit=30):
        headline = clean_wiki_artifacts(prev.get_text(" ", strip=True))
        if headline:
            return headline
    return ""

# -------------------------
# Parse all wikitables as individual tables (prevents partial parses)
# -------------------------
def parse_all_wikitables(html: str):
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", class_="wikitable")
    return tables

# -------------------------
# Country extraction logic tailored to your table shape
# -------------------------
TARGET_LOCALES = ["México", "España", "Puerto Rico", "Guatemala"]
TARGETS_NORM = {normalize_text(x) for x in TARGET_LOCALES}

def find_country_col(df):
    """
    Robustly locate the country column by checking which column
    contains the most target-country hits and generally looks like a country list.
    """
    best_col = None
    best_score = -1

    for col in df.columns:
        series = df[col].astype(str).map(normalize_text)

        # primary signal: does this column contain our target countries?
        hits = series.isin(TARGETS_NORM).sum()

        # secondary signal: the column is mostly short-ish tokens (country names)
        countryish = series.str.len().between(3, 30).sum()

        score = hits * 1000 + countryish
        if score > best_score:
            best_score = score
            best_col = col

    return best_col

def extract_four_countries_from_df(df, table_idx, table_heading):
    """
    Assumes the table is in the format you described:
    - first column is country (but header name can vary after read_html)
    - subsequent columns are "concept/article" columns
    - second row is "Artículo de Wikipedia" (metadata), which we remove
    """
    df = df.copy()

    # Flatten multiheaders
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join(map(str, c)).strip() for c in df.columns.values]

    # Clean columns + cells
    df.columns = [clean_wiki_artifacts(str(c)) for c in df.columns]
    df = df.applymap(clean_wiki_artifacts)

    country_col = find_country_col(df)
    if country_col is None:
        return None

    # Normalize the country column for filtering
    df["_country_norm"] = df[country_col].astype(str).map(normalize_text)

    # Drop the metadata row "Artículo de Wikipedia"
    df = df[df["_country_norm"] != normalize_text("Artículo de Wikipedia")]

    # Filter only target countries
    df = df[df["_country_norm"].isin(TARGETS_NORM)]
    if df.empty:
        return None

    # Melt the "concept" columns
    value_cols = [c for c in df.columns if c not in {country_col, "_country_norm"}]
    if not value_cols:
        return None

    melted = df.melt(
        id_vars=[country_col],
        value_vars=value_cols,
        var_name="Concept",
        value_name="LocalTerm"
    ).rename(columns={country_col: "Country"})

    melted["table_index"] = table_idx
    melted["table_heading"] = table_heading

    # Final cleanup
    melted["Country"] = melted["Country"].map(clean_wiki_artifacts)
    melted["Concept"] = melted["Concept"].map(clean_wiki_artifacts)
    melted["LocalTerm"] = melted["LocalTerm"].map(clean_wiki_artifacts)

    melted = melted.replace({"": pd.NA, "—": pd.NA, "-": pd.NA, "nan": pd.NA, "NaN": pd.NA})
    melted = melted.dropna(subset=["Country", "Concept", "LocalTerm"]).drop_duplicates()

    # Ensure only our targets remain (safety)
    melted = melted[melted["Country"].apply(lambda x: normalize_text(x) in TARGETS_NORM)]

    return melted

# -------------------------
# Main pipeline
# -------------------------
def ingest_full_page_four_countries():
    print("🧠 Initiating full-page ingestion (all tables, 4 countries)...")
    page_title = "Anexo:Diferencias de vocabulario estándar entre países hispanohablantes"

    html = fetch_wikipedia_html_via_api(page_title)
    soup = BeautifulSoup(html, "lxml")
    table_tags = soup.find_all("table", class_="wikitable")

    print(f"🔎 Found {len(table_tags)} wikitables.")

    master_records = []
    parsed_tables = 0
    extracted_tables = 0

    for table_idx, tbl in enumerate(table_tags):
        table_heading = nearest_heading_text(tbl)

        # Parse only this table
        try:
            df = pd.read_html(io.StringIO(str(tbl)), flavor="lxml")[0]
            parsed_tables += 1
        except Exception:
            continue

        extracted = extract_four_countries_from_df(df, table_idx, table_heading)
        if extracted is not None and not extracted.empty:
            master_records.append(extracted)
            extracted_tables += 1

    print(f"✅ Parsed tables: {parsed_tables}")
    print(f"✅ Tables producing 4-country output: {extracted_tables}")

    if not master_records:
        print("❌ No matching data extracted. (Unexpected for this page.)")
        return

    final_db = pd.concat(master_records, ignore_index=True)

    out_path = os.path.abspath("data/processed/regional_vocab_4countries_alltables.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    final_db.to_csv(out_path, index=False)

    print(f"✅ Saved CSV: {out_path}")
    print("Counts by Country:")
    print(final_db.groupby("Country").size().to_string())
    print("Unique tables represented:", final_db["table_index"].nunique())
    print("Unique headings represented:", final_db["table_heading"].nunique())

if __name__ == "__main__":
    ingest_full_page_four_countries()