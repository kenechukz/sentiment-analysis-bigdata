package ie.ucd.bigdata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class ReportPdfGenerator {
    private static final float PAGE_WIDTH = 595f;
    private static final float PAGE_HEIGHT = 842f;
    private static final float MARGIN = 42f;
    private static final float FONT_SIZE = 10f;
    private static final float LINE_HEIGHT = 12f;
    private static final int MAX_CHARS_PER_LINE = 92;

    private ReportPdfGenerator() {
    }

    public static void main(String[] args) throws IOException {
        Path input = args.length > 0 ? Path.of(args[0]) : Path.of("Big Pharma - Report.docx.md");
        Path output = args.length > 1 ? Path.of(args[1]) : Path.of("Big Pharma - Report.pdf");

        List<String> sourceLines = Files.readAllLines(input, StandardCharsets.UTF_8);
        List<String> wrappedLines = prepareLines(sourceLines);
        List<List<String>> pages = paginate(wrappedLines);

        byte[] pdfBytes = buildPdf(pages);
        Files.write(output, pdfBytes);
    }

    private static List<String> prepareLines(List<String> sourceLines) {
        List<String> output = new ArrayList<>();
        for (String raw : sourceLines) {
            String line = simplifyMarkdown(raw);
            if (line.isBlank()) {
                output.add("");
                continue;
            }

            output.addAll(wrapLine(line, MAX_CHARS_PER_LINE));
        }
        return output;
    }

    private static String simplifyMarkdown(String raw) {
        String line = raw
            .replace("**", "")
            .replace("`", "")
            .replace("*", "")
            .replace("### ", "")
            .replace("## ", "")
            .replace("# ", "");

        if (line.startsWith("- ")) {
            return "• " + line.substring(2);
        }

        return line;
    }

    private static List<String> wrapLine(String line, int maxWidth) {
        List<String> lines = new ArrayList<>();
        String remaining = line.trim();

        while (remaining.length() > maxWidth) {
            int splitAt = remaining.lastIndexOf(' ', maxWidth);
            if (splitAt <= 0) {
                splitAt = maxWidth;
            }
            lines.add(remaining.substring(0, splitAt).trim());
            remaining = remaining.substring(splitAt).trim();
        }

        lines.add(remaining);
        return lines;
    }

    private static List<List<String>> paginate(List<String> lines) {
        int linesPerPage = (int) ((PAGE_HEIGHT - (2 * MARGIN)) / LINE_HEIGHT);
        List<List<String>> pages = new ArrayList<>();
        List<String> current = new ArrayList<>();

        for (String line : lines) {
            if (current.size() >= linesPerPage) {
                pages.add(current);
                current = new ArrayList<>();
            }
            current.add(line);
        }

        if (!current.isEmpty()) {
            pages.add(current);
        }

        return pages;
    }

    private static byte[] buildPdf(List<List<String>> pages) {
        List<String> objects = new ArrayList<>();
        objects.add("<< /Type /Catalog /Pages 2 0 R >>");

        StringBuilder kids = new StringBuilder();
        for (int i = 0; i < pages.size(); i++) {
            int pageObjectNumber = 4 + (i * 2);
            kids.append(pageObjectNumber).append(" 0 R ");
        }
        objects.add("<< /Type /Pages /Count " + pages.size() + " /Kids [" + kids + "] >>");
        objects.add("<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>");

        for (int i = 0; i < pages.size(); i++) {
            int contentObjectNumber = 5 + (i * 2);
            objects.add("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 " + PAGE_WIDTH + " " + PAGE_HEIGHT
                + "] /Resources << /Font << /F1 3 0 R >> >> /Contents " + contentObjectNumber + " 0 R >>");

            String stream = buildContentStream(pages.get(i));
            byte[] streamBytes = stream.getBytes(StandardCharsets.US_ASCII);
            objects.add("<< /Length " + streamBytes.length + " >>\nstream\n" + stream + "\nendstream");
        }

        StringBuilder pdf = new StringBuilder();
        pdf.append("%PDF-1.4\n");

        List<Integer> offsets = new ArrayList<>();
        offsets.add(0);

        for (int i = 0; i < objects.size(); i++) {
            offsets.add(pdf.length());
            pdf.append(i + 1).append(" 0 obj\n");
            pdf.append(objects.get(i)).append("\n");
            pdf.append("endobj\n");
        }

        int xrefOffset = pdf.length();
        pdf.append("xref\n");
        pdf.append("0 ").append(objects.size() + 1).append("\n");
        pdf.append("0000000000 65535 f \n");

        for (int i = 1; i < offsets.size(); i++) {
            pdf.append(String.format("%010d 00000 n \n", offsets.get(i)));
        }

        pdf.append("trailer\n");
        pdf.append("<< /Size ").append(objects.size() + 1).append(" /Root 1 0 R >>\n");
        pdf.append("startxref\n");
        pdf.append(xrefOffset).append("\n");
        pdf.append("%%EOF");

        return pdf.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static String buildContentStream(List<String> lines) {
        StringBuilder stream = new StringBuilder();
        float startY = PAGE_HEIGHT - MARGIN - FONT_SIZE;
        stream.append("BT\n");
        stream.append("/F1 ").append(FONT_SIZE).append(" Tf\n");
        stream.append(MARGIN).append(" ").append(startY).append(" Td\n");

        for (int i = 0; i < lines.size(); i++) {
            if (i > 0) {
                stream.append("0 -").append(LINE_HEIGHT).append(" Td\n");
            }
            stream.append("(").append(escapePdfText(lines.get(i))).append(") Tj\n");
        }

        stream.append("ET");
        return stream.toString();
    }

    private static String escapePdfText(String text) {
        return text
            .replace("\\", "\\\\")
            .replace("(", "\\(")
            .replace(")", "\\)");
    }
}
