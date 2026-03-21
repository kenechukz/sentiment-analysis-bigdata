package ie.ucd.bigdata;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public final class SentimentWordCount {
    private static final Pattern NON_LETTERS = Pattern.compile("[^a-zA-Z\\s]");
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
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

    private SentimentWordCount() {
    }

    public static void main(String[] args) throws IOException {
        Path inputPath = resolveInputPath(args);
        Path outputPath = resolveOutputPath(args, inputPath);

        deletePathIfExists(outputPath);
        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }

        SparkConf conf = new SparkConf()
            .setAppName("SentimentWordCount")
            .setIfMissing("spark.master", "local[*]")
            .set("spark.driver.host", "127.0.0.1")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.local.dir", "target/spark-temp");

        System.setProperty("spark.local.hostname", "127.0.0.1");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> lines = sc.textFile(inputPath.toString());

            JavaPairRDD<Tuple2<String, String>, Integer> counts = lines
                .filter(line -> !isHeader(line))
                .flatMapToPair(SentimentWordCount::mapLineToPairs)
                .reduceByKey(Integer::sum);

            List<Tuple2<Tuple2<String, String>, Integer>> results = new ArrayList<>(counts.collect());
            results.sort(Comparator
                .comparingInt((Tuple2<Tuple2<String, String>, Integer> entry) -> sentimentOrder(entry._1._1))
                .thenComparing(entry -> entry._1._1)
                .thenComparing((Tuple2<Tuple2<String, String>, Integer> entry) -> entry._2, Comparator.reverseOrder())
                .thenComparing(entry -> entry._1._2));

            List<String> outputLines = new ArrayList<>(results.size());
            for (Tuple2<Tuple2<String, String>, Integer> entry : results) {
                outputLines.add(entry._1._1 + "," + entry._1._2 + "," + entry._2);
            }

            Files.write(outputPath, outputLines);

            System.out.println("Input: " + inputPath.toAbsolutePath());
            System.out.println("Output: " + outputPath.toAbsolutePath());
            System.out.println("Preview:");
            for (Tuple2<Tuple2<String, String>, Integer> entry : results.subList(0, Math.min(20, results.size()))) {
                System.out.println(entry);
            }
        }
    }

    private static Iterator<Tuple2<Tuple2<String, String>, Integer>> mapLineToPairs(String line) {
        ParsedLine parsedLine = parseLine(line);
        if (parsedLine == null) {
            return List.<Tuple2<Tuple2<String, String>, Integer>>of().iterator();
        }

        String sentiment = parsedLine.sentiment();
        String cleanText = NON_LETTERS.matcher(parsedLine.text().toLowerCase(Locale.ROOT)).replaceAll(" ");
        String[] words = cleanText.trim().split("\\s+");

        List<Tuple2<Tuple2<String, String>, Integer>> pairs = new ArrayList<>();
        for (String word : words) {
            if (word.isBlank() || STOP_WORDS.contains(word)) {
                continue;
            }

            pairs.add(new Tuple2<>(new Tuple2<>(sentiment, word), 1));
        }
        return pairs.iterator();
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

    private static void deletePathIfExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }

        if (Files.isRegularFile(path)) {
            Files.delete(path);
            return;
        }

        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private record ParsedLine(String sentiment, String text) {
    }
}
