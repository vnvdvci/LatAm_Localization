# Engineering Post-Mortem: V1 Ingestion Failure
**Date:** Day 1 of Sprint
**Module:** `core/knowledge/ingest.py`
**Version:** 1.0.0 (Baseline)

## 1. Symptom
Execution of the Wikipedia ingestion script resulted in:
- `ModuleNotFoundError: No module named 'pandas'`
- `ValueError: No objects to concatenate` (Internal logic failure after `pd.read_html` returned an empty list).

## 2. Root Cause Analysis (RCA)
### A. Environment Conflict
The macOS Homebrew Python environment was shadowed by the local Virtual Environment (`venv`). Standard `pip install` commands targeted the global site-packages rather than the project-isolated packages.
### B. Bot Mitigation (Header Rejection)
Wikipedia's server architecture identified the default `python-requests` User-Agent. Consequently, the server served a modified/empty DOM to the script, causing the `pd.read_html` function to find 0 tables, despite them being visible in a standard browser.

## 3. Data Schema Obstacle
The target Wikipedia page uses a "Conceptual Mapping" table (Concepts as Columns, Countries as Rows). The initial V1 logic attempted a standard column-wise scrape, which would have resulted in an unsearchable data format for the Cultural Critic Agent.

## 4. Mitigation Strategy (Planned for V2)
- Force absolute path execution via `./venv/bin/python3`.
- Implement `User-Agent` spoofing to mimic a Chrome/macOS browser session.
- Refactor the data pipeline to use a `melt` operation, transforming the matrix into a normalized `[Country, Concept, LocalTerm]` schema for O(1) agentic lookup.