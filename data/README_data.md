# Mock Data

This directory contains the pre-seeded SQLite database and its JSON export.
Both files are committed to the repository — no setup is required.

## Files

- `land_valuation.db` — SQLite database with all tables and seed data
- `mock_data.json` — Human-readable JSON export of all land transactions

## Database contents

| Table | Records | Description |
|---|---|---|
| land_transactions | 122 | Raw land transaction records |
| location_lookup | 15 | City/district hierarchy with centroids |
| zoning_lookup | 6 | Zoning codes and compatibility |
| factor_configs | 10 | Stage 2 scoring factor weights |
| district_price_trends | 2 | Rising trend markers for Diriyah and Al Hamra |

## Transaction breakdown

| Category | Count | Purpose |
|---|---|---|
| B1 controlled | 4 | Guarantee Benchmark 1 (Market Average) finds candidates |
| B2 controlled | 3 | Guarantee Benchmark 2 (Prime) finds candidates |
| B3 controlled | 3 | Guarantee Benchmark 3 (Secondary) finds candidates |
| B4 controlled | 3 | Guarantee Benchmark 4 (Large Dev) finds candidates |
| B5 controlled | 3 | Guarantee Benchmark 5 (Emerging) finds candidates |
| Random | 80 | Realistic diverse market data |
| Dirty (null price) | 8 | For testing null filtering |
| Dirty (zero area) | 8 | For testing zero-value filtering |
| Dirty (high outlier) | 5 | For testing top-5% outlier removal |
| Dirty (low outlier) | 5 | For testing bottom-5% outlier removal |

## Regenerate

If you need to regenerate the database:
```bash
python scripts/generate_db.py
```
