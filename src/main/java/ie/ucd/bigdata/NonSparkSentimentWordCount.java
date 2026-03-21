package ie.ucd.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class NonSparkSentimentWordCount {
    private static final Pattern NON_LETTERS = Pattern.compile("[^a-zA-Z\\s]");
    private static final Set<String> STOP_WORDS = Set.copyOf(Arrays.asList(
        "a", "about", "above", "after", "again", "against", "ain", "all", "am",
        "an", "and", "any", "are", "aren", "as", "at", "be", "because", "been",
        "before", "being", "below", "between", "both", "but", "by", "can", "couldn",
        "d", "did", "didn", "do", "does", "doesn", "doing", "don", "down", "during",
        "each", "few", "for", "from", "further", "had", "hadn", "has", "hasn", "have",
        "haven", "having", "he", "her", "here", "hers", "herself", "him", "himself",
        "his", "how", "i", "if", "in", "into", "is", "isn", "it", "its", "itself",
        "just", "ll", "m", "ma", "me", "mightn", "more", "most", "mustn", "my",
        "myself", "needn", "no", "nor", "not", "now", "o", "of", "off", "on", "once",
        "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "re",
        "s", "same", "shan", "she", "should", "so", "some", "such", "t", "than", "that",
        "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they",
        "this", "those", "through", "to", "too", "under", "until", "up", "ve", "very",
        "was", "wasn", "we", "were", "weren", "what", "when", "where", "which", "while",
        "who", "whom", "why", "will", "with", "won", "wouldn", "y", "you", "your",
        "yours", "yourself", "yourselves"
    ));
    private static final String DEFAULT_DATASET = "datasets/twitter_training_1GB_cleaned.csv";

    private NonSparkSentimentWordCount() {
    }

    public static void main(String[] args) throws IOException {
        Path inputPath = resolveInputPath(args);
        Path outputPath = resolveOutputPath(args, inputPath);

        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }

        Map<WordKey, Integer> counts = countWords(inputPath);
        List<Map.Entry<WordKey, Integer>> results = new ArrayList<>(counts.entrySet());
        results.sort(Comparator
            .comparingInt((Map.Entry<WordKey, Integer> entry) -> sentimentOrder(entry.getKey().sentiment()))
            .thenComparing(entry -> entry.getKey().sentiment())
            .thenComparing(Map.Entry<WordKey, Integer>::getValue, Comparator.reverseOrder())
            .thenComparing(entry -> entry.getKey().word()));

        List<String> outputLines = new ArrayList<>(results.size());
        for (Map.Entry<WordKey, Integer> entry : results) {
            outputLines.add(entry.getKey().sentiment() + "," + entry.getKey().word() + "," + entry.getValue());
        }

        Files.write(outputPath, outputLines, StandardCharsets.UTF_8);

        System.out.println("Input: " + inputPath.toAbsolutePath());
        System.out.println("Output: " + outputPath.toAbsolutePath());
        System.out.println("Preview:");
        for (int i = 0; i < Math.min(20, results.size()); i++) {
            Map.Entry<WordKey, Integer> entry = results.get(i);
            System.out.println("((" + entry.getKey().sentiment() + "," + entry.getKey().word() + ")," + entry.getValue() + ")");
        }
    }

    private static Map<WordKey, Integer> countWords(Path inputPath) throws IOException {
        Map<WordKey, Integer> counts = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (isHeader(line)) {
                    continue;
                }

                ParsedLine parsedLine = parseLine(line);
                if (parsedLine == null) {
                    continue;
                }

                String cleanText = NON_LETTERS.matcher(parsedLine.text().toLowerCase(Locale.ROOT)).replaceAll(" ");
                String[] words = cleanText.trim().split("\\s+");
                for (String word : words) {
                    if (word.isBlank() || STOP_WORDS.contains(word)) {
                        continue;
                    }

                    WordKey key = new WordKey(parsedLine.sentiment(), word);
                    counts.merge(key, 1, Integer::sum);
                }
            }
        }

        return counts;
    }

    private static ParsedLine parseLine(String line) {
        if (line == null) {
            return null;
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        String[] csvParts = trimmed.split(",", 2);
        if (csvParts.length == 2 && isSentiment(csvParts[0])) {
            return new ParsedLine(csvParts[0].trim(), csvParts[1].trim());
        }

        String[] textParts = trimmed.split("\\s+", 2);
        if (textParts.length == 2 && isSentiment(textParts[0])) {
            return new ParsedLine(textParts[0].trim(), textParts[1].trim());
        }

        return null;
    }

    private static boolean isHeader(String line) {
        return "sentiment,text".equalsIgnoreCase(line.trim());
    }

    private static boolean isSentiment(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("positive") || normalized.equals("negative");
    }

    private static int sentimentOrder(String sentiment) {
        String normalized = sentiment.trim().toLowerCase(Locale.ROOT);
        if (normalized.equals("positive")) {
            return 0;
        }
        if (normalized.equals("negative")) {
            return 1;
        }
        return 2;
    }

    private static Path resolveInputPath(String[] args) {
        if (args.length > 0 && !args[0].isBlank()) {
            return Path.of(args[0]);
        }

        String fromEnv = System.getenv("SENTIMENT_DATASET");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Path.of(fromEnv);
        }

        return Path.of(DEFAULT_DATASET);
    }

    private static Path resolveOutputPath(String[] args, Path inputPath) {
        if (args.length > 1 && !args[1].isBlank()) {
            return Path.of(args[1]);
        }

        String fromEnv = System.getenv("SENTIMENT_OUTPUT");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Path.of(fromEnv);
        }

        String fileName = inputPath.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        String stem = dotIndex >= 0 ? fileName.substring(0, dotIndex) : fileName;
        return Path.of("output", stem + "_word_counts.csv");
    }

    private record ParsedLine(String sentiment, String text) {
    }

    private record WordKey(String sentiment, String word) {
    }
}
