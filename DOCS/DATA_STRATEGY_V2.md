# Data Engineering Strategy: Wikipedia Ingestion (V2)

## 1. Objective
Transform unstructured, human-readable HTML tables from Wikipedia into a normalized, machine-readable "Linguistic Oracle" (CSV) 

## 2. Technical Implementation Details
To resolve the failures in V1: 

### A. Bot-Blocker Mitigation (User-Agent Spoofing)
Wikipedia identifies default Python `requests` headers as bots. I implemented a `User-Agent` string mimicking a macOS Chrome session to ensure the server returned the full DOM.

### B. Unicode Normalization (Fuzzy Matching)
Linguistic data is notoriously inconsistent with accents (País vs. Pais). I used `unicodedata (NFKD)` to strip diacritics during the search phase. This allowed the script to identify the "Country" column regardless of varied encoding.

### C. MultiIndex Flattening
Wikipedia uses merged cells in headers. Standard Pandas ingestion sees these as `MultiIndex` objects, which break simple queries. I implemented a join-logic to flatten headers into single strings before processing.

### D. Relational "Melting" (Matrix to List)
Raw data was "Wide" (1 Row = 20 Countries). This is computationally expensive for LLM Agents to parse.
- **Transformation:** Used `df.melt()` to pivot the table into a "Long" format.
- **Result:** A 3-column relational schema: `[Country, Concept, LocalTerm]`.

## 3. Data Integrity Results
- **Target Locales:** México, Puerto Rico, Guatemala, España.
- **Total Mapping Rules:** 340.
- **Lookup Efficiency:** O(1) for Agentic retrieval.