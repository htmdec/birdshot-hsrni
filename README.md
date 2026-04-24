# birdshot-hsrni

Dagster pipeline for automated nanoindentation analysis. Monitors a Girder folder for new test files, processes them, and uploads results back to Girder.

## How it works

1. The **indentation sensor** polls a Girder source folder every 60 seconds for new ZIP files
2. For each new ZIP, the pipeline:
   - Downloads the raw binary test data and the sample's CAG area measurement file
   - Extracts load, displacement, time, and strain rate signals
   - Computes hardness, contact area, and hc/h ratio
   - Uploads an Excel results file to a Girder destination folder

## Girder folder structure

All files for a sample live in a **single flat folder** (the source folder):

```
<source folder>/
  CBC06_CSR_2_Test001.zip
  CBC06_CSR_2_Test002.zip
  CBC06_IM_20mN_Test001.zip
  CBC06_IM_20mN_Test002.zip
  CBC06_area_measurements.cag     ← one CAG per sample
```

### File naming

| File | Pattern | Example |
|---|---|---|
| Test data | `{sample}_{type}_Test{N}.zip` | `CBC06_CSR_2_Test001.zip` |
| Area measurements | `{sample}_*.cag` | `CBC06_area_measurements.cag` |

The CAG file contains one measurement per indentation, labeled `{sample}_{type}_I{N:02d}`. The pipeline maps each ZIP to its CAG measurement automatically — `CBC06_CSR_2_Test001.zip` → `CBC06_CSR_2_I01`, `CBC06_IM_20mN_Test003.zip` → `CBC06_IM_20mN_I03`, etc.

## Running

### Setup

```bash
pip install -e ".[dev]"
```

### Start

```bash
GIRDER_API_KEY=<key> \
GIRDER_API_URL=<url> \
GIRDER_SRC_FOLDER_ID=<source_folder_id> \
GIRDER_DST_FOLDER_ID=<destination_folder_id> \
dagster dev
```

### In the Dagster UI (http://localhost:3000)

Go to **Automation** and start both sensors:
- **indentation_sensor** — watches for new ZIP files and triggers the analysis pipeline
- **default_automation_condition_sensor** — materializes instrument calibration parameters on startup

Results are uploaded to the destination Girder folder as `{zip_stem}.xlsx` (e.g. `CBC06_CSR_2_Test001.xlsx`).

## Scaling

The current pipeline assumes a **flat folder structure** — all ZIP and CAG files for a sample in one folder. A nested structure (e.g. organized by campaign, iteration, or sample) is planned but not yet implemented. The sensor and file-matching logic will need to be updated to traverse subfolders when that change is made.
