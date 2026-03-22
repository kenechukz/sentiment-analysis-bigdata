# sentiment-analysis-bigdata

This project analyses a large Twitter dataset containing tweets for different topics and groups, labelled with fields such as sentiment, group ID, and topic name. The main goal is to compute the words that occur most frequently in positive and negative tweets.

The cleaned dataset was evaluated in two equivalent formats: `twitter_training_1GB_cleaned.csv` (`1,229,605,101` bytes, `13,074,651` lines including the header) and `twiiter_training_1GB_cleaned.txt` (`1,216,530,435` bytes, `13,074,650` lines). Each full run therefore processes roughly 1GB of text and more than 13 million sentiment-labelled records.

This repository includes a Java Spark implementation that uses `JavaSparkContext` and RDDs only and a Traditonal data processing implementation. It does not use Spark SQL or DataFrames.

## Files

- `data_preprocessing.py` cleans the source CSV and writes a filtered file to `datasets/`
- `src/main/java/ie/ucd/bigdata/SentimentWordCount.java` runs the Java Spark word count by sentiment
- `src/main/java/ie/ucd/bigdata/NonSparkSentimentWordCount.java` runs the same word count locally without Spark
- `run_java_spark.ps1` selects a supported local JDK and runs the Spark job with Maven
- `run_java_non_spark.ps1` runs the non-Spark Java implementation
- `word_sentiment_lookup.py` looks up how strongly a word is associated with positive vs negative tweets from the generated output file

## Prerequisites

1. CSV must have these columns in this exact order:
   - `group_id,topic_name,sentiment,text`

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

## Run the non-Spark Java job

```powershell
powershell -ExecutionPolicy Bypass -File .\run_java_non_spark.ps1 -Dataset "datasets/twitter_training_1GB_cleaned.csv"
```

This uses standard Java file I/O and in-memory maps instead of Spark.

## Look up a word

After generating `output/twitter_training_1GB_cleaned_benchmark_word_counts.csv`, you can check how often a word is associated with positive or negative tweets:

```powershell
python3 .\word_sentiment_lookup.py
```

Output:

```powershell

Enter a word: best
best
Positive: 99.94% (350355 occurences)
Negative: 0.06% (211 occurences)
```

The script prompts for a word, then prints the positive and negative percentage split for that word. If the word is not present in `output/twitter_training_1GB_cleaned_benchmark_word_counts.csv`, it prints `word not found`.

## Build directly with Maven

```powershell
mvn -q -DskipTests compile
```
