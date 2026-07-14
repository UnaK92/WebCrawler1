import java.util.*;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SeqCrawler {

    private static final String startURL1 = "https://www.famnit.upr.si";
    private static final String startURL2 = "https://www.famnit.upr.si/";

    private static final String host1 = "www.famnit.upr.si";
    private static final String host2 = "famnit.upr.si";

    private static final Pattern hrefPattern = Pattern.compile("<a\\s+[^>]*href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please enter the starting URL: ");
        String inputURL = scanner.nextLine().trim();

        if (!inputURL.equals(startURL1) && !inputURL.equals(startURL2)) {
            System.out.println("Invalid URL.");
            scanner.close();
            return;
        }

        long startTime = System.currentTimeMillis();

        Set<String> visited = new HashSet<>();
        Set<String> seen = new HashSet<>();
        Set<String> workingLinks = new HashSet<>();
        Set<String> brokenLinks = new HashSet<>();
        Map<String, Set<String>> foundOnPage = new HashMap<>();
        Queue<String> queue = new ArrayDeque<>();
        queue.add(inputURL);
        seen.add(inputURL);

        int processedCount = 0;

        while (!queue.isEmpty()) {
            String currentURL = queue.poll();

            if (visited.contains(currentURL)) {
                continue;
            }

            visited.add(currentURL);
            processedCount++;

            if (processedCount == 100 || (processedCount > 100 && processedCount % 100 == 0)) {
                System.out.println("Processed " + processedCount + " pages.");
            }

            try {
                URL objURL = new URL(currentURL);
                HttpURLConnection conn = (HttpURLConnection) objURL.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                int code = conn.getResponseCode();
                HttpStatus status = HttpStatus.getStatusFromCode(code);

                if (code >= 200 && code < 400) {
                    workingLinks.add(currentURL);
                } else {
                    brokenLinks.add(currentURL + " (Response code: " + code + " - " + status.getDescription() + ")");
                    continue;
                }

                String type = conn.getContentType();
                if (type == null || !type.toLowerCase().contains("text/html")) {
                    continue;
                }

                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                StringBuilder html = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    html.append(line);
                }

                reader.close();

                Matcher matcher = hrefPattern.matcher(html.toString());

                while (matcher.find()) {
                    String href = matcher.group(1).trim();

                    if (href.isEmpty()) continue;
                    if (href.startsWith("#")) continue;

                    String lower = href.toLowerCase();
                    if (lower.startsWith("mailto:") || lower.startsWith("javascript:") || lower.startsWith("tel:")) {
                        continue;
                    }

                    URL absolute;
                    try {
                        absolute = new URL(objURL, href);
                    } catch (Exception e) {
                        continue;
                    }

                    String protocol = absolute.getProtocol();
                    if (!protocol.equals("http") && !protocol.equals("https")) {
                        continue;
                    }

                    String host = absolute.getHost();
                    boolean inDomain = host.equalsIgnoreCase(host1) || host.equalsIgnoreCase(host2);
                    if (!inDomain) {
                        continue;
                    }

                    String nextURL = absolute.toString();

                    if (nextURL.toLowerCase().contains("latest")) {
                        continue;
                    }

                    foundOnPage.computeIfAbsent(nextURL, k -> new HashSet<>()).add(currentURL);

                    if (!seen.contains(nextURL)) {
                        seen.add(nextURL);
                        queue.add(nextURL);
                    }
                }

            } catch (Exception e) {
                brokenLinks.add(currentURL + " (Error: " + e.getMessage() + ")");
            }
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        String outputPath = "WebCrawlerLogs/WebcrawlerSeq.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            writer.write("Execution time (ms): " + totalTime + "\n");
            writer.write("Pages visited: " + visited.size() + "\n");
            writer.write("Working links: " + workingLinks.size() + "\n");
            writer.write("Broken links: " + brokenLinks.size() + "\n\n");

            writer.write("Working Links:\n");
            for (String link : workingLinks) {
                writer.write(link + "\n");
            }

            writer.write("\nBroken Links:\n");
            for (String link : brokenLinks) {
                String urlOnly = link;
                int idx = link.indexOf(" (");
                if (idx != -1) {
                    urlOnly = link.substring(0, idx);
                }

                writer.write(link + "\n");

                Set<String> sources = foundOnPage.get(urlOnly);
                if (sources != null && !sources.isEmpty()) {
                    for (String src : sources) {
                        writer.write(" found on: " + src + "\n");
                    }
                } else {
                    writer.write(" found on: (unknown)\n");
                }
            }

            System.out.println("Results logged to WebcrawlerSeq.txt");
        } catch (Exception e) {
            System.out.println("Error writing the log " + e.getMessage());
        }

        scanner.close();
    }
}