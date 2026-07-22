import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParCrawler {

    private static final String startURL1 = "https://www.old.famnit.upr.si/sl";
    private static final String startURL2 = "https://www.old.famnit.upr.si/sl/";

    private static final String host1 = "www.old.famnit.upr.si";
    private static final String host2 = "old.famnit.upr.si";

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

        //Thread-safe collections + queue
        Set<String> visited = ConcurrentHashMap.newKeySet();
        Set<String> seen = ConcurrentHashMap.newKeySet();
        Set<String> workingLinks = ConcurrentHashMap.newKeySet();
        Set<String> brokenLinks = ConcurrentHashMap.newKeySet();
        Map<String, Set<String>> foundOnPage = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        AtomicInteger activeWorkers = new AtomicInteger(0); //Active worker counter
        AtomicInteger processedCount = new AtomicInteger(0);

        queue.add(inputURL);
        seen.add(inputURL);

        //Number of threads + array with references
        final int NUM_THREADS = 8;
        Thread[] workers = new Thread[NUM_THREADS];

        //Creation of worker threads
        for (int i = 0; i < NUM_THREADS; i++) {
            workers[i] = new Thread(() -> {

                while (true) {
                    String currentURL = queue.poll();

                    //If queue is empty & no worker is processing a page stop
                    if (currentURL == null) {
                        if (queue.isEmpty() && activeWorkers.get() == 0) {
                            break;
                        }

                        //If queue is empty, but another thread is working wait
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException ignored) {}
                        continue;
                    }

                    //Mark worker as active
                    activeWorkers.incrementAndGet();

                    //Automatically mark URLs as visited
                    if (!visited.add(currentURL)) {
                        activeWorkers.decrementAndGet();
                        continue;
                    }

                    //Print every 100 processed pages
                    int count = processedCount.incrementAndGet();
                    if (count % 100 == 0) {
                        System.out.println("Processed " + count + " pages");
                    }

                    //Convert string URL into java URL object, with no connection currently and -1 response code (no code received)
                    HttpURLConnection conn = null;
                    try {
                        URL objURL = new URL(currentURL);
                        int code = -1;

                        //Try to connect to each URL up to 3 times, URL & HTTP connection creation + request
                        for (int attempt = 1; attempt <= 3; attempt++) {
                            try {
                                conn = (HttpURLConnection) objURL.openConnection();
                                conn.setRequestMethod("GET");
                                conn.setConnectTimeout(20000);
                                conn.setReadTimeout(30000);

                                code = conn.getResponseCode();
                                break;
                            } catch (Exception ex) {
                                if (conn != null) {
                                    conn.disconnect();
                                    conn = null;
                                }

                                if (attempt == 3) {
                                    throw ex;
                                }

                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException ignored) {}
                            }
                        }

                        //Read HTTP response code & sorting into sets
                        HttpStatus status = HttpStatus.getStatusFromCode(code);

                        if (code >= 200 && code < 400) {
                            workingLinks.add(currentURL);
                        } else {
                            brokenLinks.add(currentURL + " (Response code: " + code + " - " + status.getDescription() + ")");
                            continue;
                        }

                        //Check if it's HTML, read it & combine it
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

                        //Search HTML for links & process them
                        Matcher matcher = hrefPattern.matcher(html.toString());
                        while (matcher.find()) {
                            String href = matcher.group(1).trim();

                            if (href.isEmpty()) continue;
                            if (href.startsWith("#")) continue;

                            String lower = href.toLowerCase();
                            if (lower.startsWith("mailto:") || lower.startsWith("javascript:") || lower.startsWith("tel:")) {
                                continue;
                            }

                            //Converting relative to absolute links
                            URL absolute;
                            try {
                                absolute = new URL(objURL, href);
                            } catch (Exception e) {
                                continue;
                            }

                            //Only allow HTTP & HTTPS
                            String protocol = absolute.getProtocol();
                            if (!protocol.equals("http") && !protocol.equals("https")) {
                                continue;
                            }

                            //Keep crawler inside the domain
                            String host = absolute.getHost();
                            boolean inDomain = host.equalsIgnoreCase(host1) || host.equalsIgnoreCase(host2);
                            if (!inDomain) continue;

                            //Convert URL object back to regular string + latest
                            String nextURL = absolute.toString();
                            if (nextURL.toLowerCase().contains("latest")) {
                                continue;
                            }

                            //Record on what page links were found & add new URLs to the queue
                            foundOnPage.computeIfAbsent(nextURL, k -> ConcurrentHashMap.newKeySet()).add(currentURL);

                            if (seen.add(nextURL)) {
                                queue.add(nextURL);
                            }
                        }

                    } catch (Exception e) {
                        brokenLinks.add(currentURL + " (Error: " + e.getMessage() + ")");
                    } finally {
                        if (conn != null) {
                            conn.disconnect();
                        }
                        activeWorkers.decrementAndGet(); //Always after done decrease active worker count
                    }
                }
            });

            workers[i].start(); //Starting each thread
        }

        //Wait for all workers to finish
        for (Thread t : workers) {
            try { t.join();
            } catch (InterruptedException ignored) {}
        }

        //Total time calculation + writing out the report text file
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        String outputPath = "WebCrawlerLogs/WebcrawlerPara.txt";

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
                        writer.write("  found on: " + src + "\n");
                    }
                } else {
                    writer.write("  found on: (unknown)\n");
                }
            }

            System.out.println("Results logged to " + outputPath);
        } catch (Exception e) {
            System.out.println("Error writing the log " + e.getMessage());
        }

        scanner.close();
    }
}