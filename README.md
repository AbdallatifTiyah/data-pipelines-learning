# 01 - CSV Pipeline (Dagster)

![CSV Pipeline](../01-csv-pipeline.png)

A simple **Dagster** project to demonstrate basic asset-based pipelines:
- Load a CSV file.
- Clean and transform the data.
- Write the cleaned output to a new CSV file.

This project introduces Dagster concepts: **Assets**, **Dependencies**, and the **Dagit UI**.

---

## 🗂️ Project Structure

01-csv-pipeline/
├─ data/
│ └─ raw.csv # Sample input CSV
├─ src/
│ └─ my_dagster_csv/
│ ├─ init.py
│ └─ assets.py # Dagster assets code
└─ pyproject.toml


---

## 🚀 Quick Start

```bash
# Create and activate a virtual environment
python -m venv .venv
. .venv/Scripts/activate     # Windows
# source .venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -e .

# Launch Dagster web UI
dagster dev

Then open http://127.0.0.1:3000
 and Materialize write_clean_csv.
```
🧠 Key Learnings
  - Understand Dagster asset definitions and dependencies.
  - Build a reproducible pipeline with clear data flow.
  - Learn best practices for structuring Dagster projects.
