---
title: Longevity Biotech Insights
emoji: 🧬
colorFrom: indigo
colorTo: yellow
sdk: docker
pinned: false
---

# Longevity Biotech Insights (LBI)

An interactive dashboard to explore the longevity and aging biotech landscape,
using data from agingbiotech.info.

## Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/f-neri/longevity_biotech_insights.git
cd longevity_biotech_insights
```
### 2. Create the Conda environment
```bash
conda env create -f environment.yml
conda activate lbi
```

### 3. Install the project (development mode)
```bash
pip install -e .
```
### 4. Run the dashboard
```bash
lbi
```
The app will be available at:
http://localhost:8050/

## Data Refresh Workflow

The repository includes a GitHub Actions workflow at `.github/workflows/weekly-data-update.yml`.

- It runs every Monday at 06:00 UTC.
- It can also be triggered manually from the GitHub Actions tab.
- It executes `lbi-update`, which now validates the generated dashboard artifacts before the workflow can commit them.
- It only commits and pushes when one of the tracked data artifacts changes:
	- `data/companies_raw.csv`
	- `data/companies_clean.csv`
	- `data/companies_clean.parquet`
	- `data/detail_lookups.json`

### Manual refresh locally

```bash
lbi-update
```

### Validate dashboard data locally

```bash
lbi-validate
```
