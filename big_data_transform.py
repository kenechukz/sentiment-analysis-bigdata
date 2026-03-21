import os
import re
import site
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
LOCAL_PACKAGES = PROJECT_ROOT / ".python_packages"

if LOCAL_PACKAGES.exists():
    site.addsitedir(str(LOCAL_PACKAGES))
    sys.path.insert(0, str(LOCAL_PACKAGES))

import nltk
from nltk.corpus import stopwords


DEFAULT_DATASET = "twitter_training_1GB_cleaned.csv"
FALLBACK_STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "ain", "all", "am",
    "an", "and", "any", "are", "aren", "aren't", "as", "at", "be", "because",
    "been", "before", "being", "below", "between", "both", "but", "by", "can",
    "couldn", "couldn't", "d", "did", "didn", "didn't", "do", "does", "doesn",
    "doesn't", "doing", "don", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't", "have",
    "haven", "haven't", "having", "he", "her", "here", "hers", "herself", "him",
    "himself", "his", "how", "i", "if", "in", "into", "is", "isn", "isn't", "it",
    "it's", "its", "itself", "just", "ll", "m", "ma", "me", "mightn", "mightn't",
    "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't", "no",
    "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other",
    "our", "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan",
    "shan't", "she", "she's", "should", "should've", "shouldn", "shouldn't", "so",
    "some", "such", "t", "than", "that", "that'll", "the", "their", "theirs",
    "them", "themselves", "then", "there", "these", "they", "this", "those",
    "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn",
    "wasn't", "we", "were", "weren", "weren't", "what", "when", "where", "which",
    "while", "who", "whom", "why", "will", "with", "won", "won't", "wouldn",
    "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your",
    "yours", "yourself", "yourselves",
}


def parse_java_major(version_output: str) -> int | None:
    match = re.search(r'version "(\d+)', version_output)
    if match:
        return int(match.group(1))
    return None


def configure_java_home() -> None:
    current_java_home = os.environ.get("JAVA_HOME")
    candidate_homes = []

    if current_java_home:
        candidate_homes.append(Path(current_java_home))

    java_root = Path(r"C:\Program Files\Java")
    if java_root.exists():
        candidate_homes.extend(sorted(java_root.glob("jdk-*"), reverse=True))

    for java_home in candidate_homes:
        java_exe = java_home / "bin" / "java.exe"
        if not java_exe.exists():
            continue

        try:
            version_result = subprocess.run(
                [str(java_exe), "-version"],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            continue

        java_major = parse_java_major(version_result.stderr or version_result.stdout)
        if java_major is None or java_major > 23:
            continue

        os.environ["JAVA_HOME"] = str(java_home)
        os.environ["PATH"] = f"{java_home / 'bin'}{os.pathsep}{os.environ['PATH']}"
        return

    raise RuntimeError(
        "Could not find a supported Java installation. Install or use JDK 23 or lower for PySpark."
    )


configure_java_home()

from pyspark import SparkConf, SparkContext


def resolve_dataset_path() -> Path:
    dataset_name = os.environ.get("SENTIMENT_DATASET", DEFAULT_DATASET)
    dataset_path = PROJECT_ROOT / "datasets" / dataset_name

    if dataset_path.exists():
        return dataset_path

    cleaned_datasets = sorted((PROJECT_ROOT / "datasets").glob("*_cleaned.csv"))
    if cleaned_datasets:
        return cleaned_datasets[0]

    raise FileNotFoundError(
        f"Could not find dataset '{dataset_name}' or any '*_cleaned.csv' file in datasets/."
    )


def create_spark_context() -> SparkContext:
    # Force loopback networking so Spark local mode does not use the Windows hostname.
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    python_paths = [str(PROJECT_ROOT)]
    if LOCAL_PACKAGES.exists():
        python_paths.append(str(LOCAL_PACKAGES))
    if os.environ.get("PYTHONPATH"):
        python_paths.append(os.environ["PYTHONPATH"])
    os.environ["PYTHONPATH"] = os.pathsep.join(python_paths)

    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("SentimentMapReduce")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .set("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .set("spark.pyspark.python", sys.executable)
        .set("spark.pyspark.driver.python", sys.executable)
        .set("spark.python.use.daemon", "false")
        .setExecutorEnv("PYTHONPATH", os.environ["PYTHONPATH"])
    )

    return SparkContext(conf=conf)


def map_function(line, stop_words):
    parts = line.split(",", 1)
    if len(parts) < 2:
        return []

    sentiment = parts[0]
    text = parts[1].lower()
    text = re.sub(r"[^a-zA-Z\s]", "", text)

    words = text.split()
    filtered_words = [word for word in words if word not in stop_words]
    return [((sentiment, word), 1) for word in filtered_words]


def load_stop_words() -> set[str]:
    try:
        nltk.download("stopwords", quiet=True)
    except Exception:
        pass

    try:
        return set(stopwords.words("english"))
    except LookupError:
        return set(FALLBACK_STOPWORDS)


def main():
    dataset_path = resolve_dataset_path()
    sc = create_spark_context()

    try:
        stop_words = load_stop_words()
        stop_words_bc = sc.broadcast(stop_words)
        rdd = sc.textFile(str(dataset_path))
        header = rdd.first()
        rdd = rdd.filter(lambda line: line != header)
        mapped_rdd = rdd.flatMap(lambda line: map_function(line, stop_words_bc.value))
        reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

        for result in reduced_rdd.collect():
            print(result)
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
