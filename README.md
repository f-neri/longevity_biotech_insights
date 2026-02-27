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
