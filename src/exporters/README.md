# Fragebogen Distribution Exporter (Python)

This exporter is used only by:

- `POST /api/fragebogen/fragebogen/distribution-export.xlsx`

The Node route gathers and validates data, writes a temporary JSON payload, then calls:

- `src/exporters/fragebogen_distribution_export.py`

The Python script creates the Excel workbook with:

- `RawData` sheet as native Excel table + column filters
- Aggregation sheets (`ItemDistribution_Monthly`, `CustomerDistribution_Monthly`, `ADDistribution_Monthly`)
- Native Excel line chart in `Chart`
- Excel-side chain/question selector dropdowns in `Chart` that drive chart formulas

## Local setup

From `backend/`:

```bash
python -m pip install -r src/exporters/requirements.txt
```

If your machine uses `python3`:

```bash
python3 -m pip install -r src/exporters/requirements.txt
```

## Manual exporter test

```bash
python src/exporters/fragebogen_distribution_export.py .\tmp\input.json .\tmp\out.xlsx
```

Or:

```bash
python3 src/exporters/fragebogen_distribution_export.py ./tmp/input.json ./tmp/out.xlsx
```

## Runtime notes

- Node tries these Python binaries in order:
  1. `PYTHON_BIN` (if set)
  2. `py`
  3. `python3`
  4. `python`
- Exporter timeout is controlled by `FRAGEBOGEN_EXPORT_PY_TIMEOUT_MS` (default: `90000`).
- Railway deploys can use `backend/nixpacks.toml` to install Python + pip + `xlsxwriter`.
