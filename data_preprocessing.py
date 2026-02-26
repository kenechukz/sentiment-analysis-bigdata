import time


def fix_commas_inside_quotes(line):
    result = []
    in_quotes = False
    i = 0

    while i < len(line):
        char = line[i]

        if char == '"':
            # handle escaped quote ""
            if i + 1 < len(line) and line[i + 1] == '"':
                i += 2
                continue
            in_quotes = not in_quotes
            i += 1
            continue  # Do not add quotes at all

        elif char == "," and in_quotes:
            result.append(" ")
            i += 1
            continue

        result.append(char)
        i += 1

    return "".join(result)

def contains_unecessary_data(line):

    unecessary_data = {"Neutral", "Irrelevant"}
    for word in unecessary_data:

        if word in line:
            return True

def remove_columns_and_reorder(line):

    parts = line.split(",")
    if len(parts) < 4:
        return None
    
    return ",".join(parts[2:])

def main():
    in_path = str(input("Enter path for input csv : "))
    file_size = str(input("Enter file size (approx) e.g. 100MB or 2GB: "))
    out_path = f"datasets/twitter_training_{file_size}_cleaned.csv"
    start_time = time.perf_counter()

    try:
        with (
            open(in_path, "r", encoding="utf-8", newline="") as in_file,
            open(out_path, "w", encoding="utf-8", newline="") as out_file,
        ):
            for line in in_file:
                if not contains_unecessary_data(line):
                    line = fix_commas_inside_quotes(line)
                    extracted = remove_columns_and_reorder(line)

                    if extracted is None:
                        continue
                    
                    out_file.write(extracted)

        elapsed = time.perf_counter() - start_time
        print(f"Completed in {elapsed:.2f} seconds.")
    except FileNotFoundError:
        print(f"Error: {in_path} not found.")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
