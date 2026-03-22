import csv
from pathlib import Path


DEFAULT_RESULTS_FILE = Path("output/twitter_training_1GB_cleaned_benchmark_word_counts.csv")


def load_word_counts(results_path: Path) -> dict[str, dict[str, int]]:
    word_counts: dict[str, dict[str, int]] = {}

    with results_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.reader(file_obj)
        for row in reader:
            if len(row) != 3:
                continue

            sentiment, word, count_text = row
            word = word.strip().lower()
            sentiment = sentiment.strip().lower()

            try:
                count = int(count_text)
            except ValueError:
                continue

            if word not in word_counts:
                word_counts[word] = {"positive": 0, "negative": 0}

            if sentiment in word_counts[word]:
                word_counts[word][sentiment] += count

    return word_counts


def main() -> None:
    if not DEFAULT_RESULTS_FILE.exists():
        print(f"Results file not found: {DEFAULT_RESULTS_FILE}")
        return

    word_counts = load_word_counts(DEFAULT_RESULTS_FILE)
    lookup_word = input("Enter a word: ").strip().lower()

    if not lookup_word or lookup_word not in word_counts:
        print("word not found")
        return

    positive_count = word_counts[lookup_word]["positive"]
    negative_count = word_counts[lookup_word]["negative"]
    total_count = positive_count + negative_count

    if total_count == 0:
        print("word not found")
        return

    positive_percentage = (positive_count / total_count) * 100
    negative_percentage = (negative_count / total_count) * 100

    print(f"{lookup_word}")
    print(f"Positive: {positive_percentage:.2f}% ({positive_count} occurences)")
    print(f"Negative: {negative_percentage:.2f}% ({negative_count} occurences)")


if __name__ == "__main__":
    main()
