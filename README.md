# sentiment-analysis-bigdata

This repository now includes a Java Spark implementation that uses `JavaSparkContext` and RDDs only. It does not use Spark SQL or DataFrames.

## Files

- `data_preprocessing.py` cleans the source CSV and writes a filtered file to `datasets/`
- `src/main/java/ie/ucd/bigdata/SentimentWordCount.java` runs the Java Spark word count by sentiment
- `run_java_spark.ps1` selects a supported local JDK and runs the Spark job with Maven
  
### Prerequisites

1. CSV must have these columns in this exact order:
   - `group_id,topic_name,sentiment,text`

### What it does

## Java Spark setup

Spark 3.5 should be run with JDK 17 to 23. The machine this was fixed on had Java 24 as the default JDK, which is not a safe default for Spark 3.5. The launcher script prefers JDK 17, then falls back to 21, 20, or 23 if those are installed.

## Run the preprocessing step

```powershell
& 'C:\Users\kenep\AppData\Local\Programs\Python\Python312\python.exe' .\data_preprocessing.py
```

The script prompts for:

1. Input CSV path
2. Approximate file size label such as `100MB` or `1GB`

It writes the cleaned file to `datasets/twitter_training_<file_size>_cleaned.csv`.

## Run the Java Spark job

From the project root:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_java_spark.ps1
```

To run a specific cleaned dataset:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_java_spark.ps1 -Dataset "datasets/twitter_training_100MB_cleaned.csv"
```

To set both dataset and output path:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_java_spark.ps1 -Dataset "datasets/twitter_training_100MB_cleaned.csv" -Output "output/twitter_training_100MB_counts.csv"
```

## Output

The Spark job writes a CSV-style text file into `output/` and prints a small preview to the console.

## Build directly with Maven

```powershell
mvn -q -DskipTests compile
```
