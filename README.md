# sentiment-analysis-bigdata

## Using `data_preprocessing.py`

This script cleans a Twitter training CSV and writes a filtered version to the `datasets/` folder.

### Prerequisites

- CSV must have these columns in this exact order:
   -group_id,topic_name,sentiment,text 

### What it does

- Removes rows containing `Neutral` or `Irrelevant`
- Replaces commas inside quoted text with spaces (to avoid breaking simple comma splitting)
- Drops the first two columns and keeps the remaining columns
- Writes the cleaned output to a new CSV in `datasets/`

### Run the script

From the project root:

```powershell
python data_preprocessing.py
```

### Inputs you will be prompted for

1. `Enter path for input csv :`
   - Example: `datasets\twitter_training.csv`
2. `Enter file size (approx) e.g. 100MB or 2GB:`
   - This is used only for the output filename label.
   - Example: `100MB`

### Output file

The script writes to:

```text
datasets/twitter_training_<file_size>_cleaned.csv
```

Example:

```text
datasets/twitter_training_100MB_cleaned.csv
```

### Notes

- Run it from the repository root so the `datasets/` output path resolves correctly.
