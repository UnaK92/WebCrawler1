import java.util.*;
import mpi.MPI;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
//import java.util.Scanner;

public class DistCrawler {

    private static final int tagControl = 1;
    private static final int tagURLLength = 2;
    private static final int tagURL = 3;
    private static final int tagStatusCode = 4;
    private static final int tagLinkCount = 5;
    private static final int tagLinksLength = 6;
    private static final int tagLinks = 7;

    private static final int work = 1;
    private static final int stop = 0;

   // private static final int maxPages = 20;

    private static final String startURL1 = "https://www.famnit.upr.si";
    private static final String startURL2 = "https://www.famnit.upr.si/";

    private static final String host1 = "www.famnit.upr.si";
    private static final String host2 = "famnit.upr.si";

    private static final Pattern hrefPattern = Pattern.compile("<a\\s+[^>]*href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) {

        args = MPI.Init(args); // start MPI

        int rank = MPI.COMM_WORLD.Rank(); // unique rank of current process
        int size = MPI.COMM_WORLD.Size(); // total number of processes

        if (size < 2) {
            if (rank == 0) {
                System.out.println("Program must be started with at least two processes.");
            }

            MPI.Finalize();
            return;
        }

        if (rank == 0) {
            runMaster(size, args);
        } else {
            runWorker(rank);
        }

        MPI.Finalize(); // finish MPI
    }

    private static void runMaster(int numOfProcesses, String[] args) {

        System.out.println("Running Master");
        System.out.println("Number of Workers: " + (numOfProcesses - 1));

        /*Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter the starting URL: ");
        String startURL = scanner.nextLine().trim();*/

        if (args.length == 0) {
            System.out.println("Please provide the starting URL.");
            for (int workerRank = 1; workerRank < numOfProcesses; workerRank++) {
                int[] control = {stop};
                MPI.COMM_WORLD.Send(
                        control, 0, 1, MPI.INT, workerRank, tagControl);
            }
            return;
        }

        String startURL = args[0].trim();

        if (!startURL.equals(startURL1) && !startURL.equals(startURL2)) {
            System.out.println("Invalid URL.");
            for (int workerRank = 1; workerRank < numOfProcesses; workerRank++) {
                int[] control = {stop};
                MPI.COMM_WORLD.Send(
                        control, 0, 1, MPI.INT, workerRank, tagControl
                );
            }

            //scanner.close();
            return;
        }

        long startTime = System.currentTimeMillis();

        Queue<String> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        Set<String> seen = new HashSet<>();

        Set<String> workingLinks = new HashSet<>();
        Set<String> brokenLinks = new HashSet<>();
        Map<String, Set<String>> foundOnPage = new HashMap<>();

        queue.add(startURL);
        seen.add(startURL);

        int processedPages = 0;
        int[] workerProcessedPages = new int[numOfProcesses];

        while (true) {
            int[] assignedWorkers = new int[numOfProcesses];
            String[] assignedURLs = new String[numOfProcesses];

            int workersUsed = 0;

            for (int workerRank = 1; workerRank < numOfProcesses; workerRank++) {
                if (!queue.isEmpty() /*&& processedPages + workersUsed < maxPages*/) {

                    String currentURL = queue.poll();

                    int[] control = {work};

                    MPI.COMM_WORLD.Send(
                            control, 0, 1, MPI.INT, workerRank, tagControl
                    );

                    sendString(currentURL, workerRank);

                    assignedWorkers[workerRank] = 1;
                    assignedURLs[workerRank] = currentURL;

                    workersUsed++;

                   // System.out.println("Sent " + currentURL + " to worker " + workerRank);
                    if (currentURL.equals(startURL)) {
                        System.out.println("Sent starting URL to worker " + workerRank);
                    }
                }
                else {
                    assignedWorkers[workerRank] = 0;
                }
            }

            if (workersUsed == 0) {
                break;
            }

            for (int workerRank = 1; workerRank < numOfProcesses; workerRank++) {
                if (assignedWorkers[workerRank] == 0) {
                    continue;
                }

                int[] statusCode = new int[1];
                int[] linkCount = new int[1];
                int[] linksLength = new int[1];

                MPI.COMM_WORLD.Recv(
                        statusCode, 0, 1, MPI.INT, workerRank, tagStatusCode
                );

                MPI.COMM_WORLD.Recv(
                        linkCount, 0, 1, MPI.INT, workerRank, tagLinkCount
                );

                MPI.COMM_WORLD.Recv(
                        linksLength, 0, 1, MPI.INT, workerRank, tagLinksLength
                );

                int[] linksCharacters = new int[linksLength[0]];

                if (linksLength[0] > 0) {
                    MPI.COMM_WORLD.Recv(
                            linksCharacters, 0, linksCharacters.length, MPI.INT, workerRank, tagLinks
                    );
                }

                StringBuilder linksBuilder = new StringBuilder();

                for (int character : linksCharacters) {
                    linksBuilder.append((char) character);
                }

                String receivedLinksString = linksBuilder.toString();
                Set<String> receivedLinks = new HashSet<>();

                if (!receivedLinksString.isEmpty()) {
                    String[] linksArray = receivedLinksString.split("\n");

                    for (String link : linksArray) {
                        if (!link.isEmpty()) {
                            receivedLinks.add(link);
                        }
                    }
                }

                String processedURL = assignedURLs[workerRank];

                visited.add(processedURL);
                processedPages++;

                if (statusCode[0] >= 200 && statusCode[0] < 400) {
                    workingLinks.add(processedURL);
                }
                else if (statusCode[0] == -1) {
                    brokenLinks.add(processedURL + " (Connection error)");
                }
                else {
                    HttpStatus status = HttpStatus.getStatusFromCode(statusCode[0]);

                    brokenLinks.add(processedURL + " (Response code: " + statusCode[0] + " - " + status.getDescription() + ")");
                }

                /*System.out.println("Worker " + workerRank + " processed: " + processedURL);
                System.out.println("Status code: " + statusCode[0]);
                System.out.println("Links found: " + linkCount[0]);*/

                workerProcessedPages[workerRank]++;

                if (workerProcessedPages[workerRank] % 100 == 0) {
                    System.out.println("Worker " + workerRank + " processed " + workerProcessedPages[workerRank] + " links");
                }

                for (String link : receivedLinks) {

                    foundOnPage.computeIfAbsent(link, key -> new HashSet<>()).add(processedURL);

                    if (seen.add(link)) {
                        queue.add(link);
                    }
                }

                //System.out.println("Pages processed: " + processedPages + ", URLs waiting: " + queue.size());
            }

            /*if (processedPages >= maxPages) {
                finished = true;
            }*/
        }

        for (int workerRank = 1; workerRank < numOfProcesses; workerRank++) {
            int[] control = {stop};

            MPI.COMM_WORLD.Send(
                    control, 0, 1, MPI.INT, workerRank, tagControl
            );
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        writeReport(totalTime, visited, workingLinks, brokenLinks, foundOnPage);

        System.out.println();
        System.out.println("Crawling finished");
        System.out.println("Execution time: " + totalTime + " ms");
        System.out.println("Pages visited: " + visited.size());
        System.out.println("Working links: " + workingLinks.size());
        System.out.println("Broken links: " + brokenLinks.size());

        //scanner.close();
    }

    private static void runWorker(int rank) {

        System.out.println("Running Worker " + rank);

        while (true) {
            int[] control = new int[1];

            MPI.COMM_WORLD.Recv(
                    control, 0, 1, MPI.INT, 0, tagControl
            );

            if (control[0] == stop) {
                System.out.println("Worker " + rank + " stopped");
                break;
            }

            String receivedURL = receiveString();

            //System.out.println("Worker " + rank + " received " + receivedURL);

            int[] statusCode = {-1};
            Set<String> foundLinks = new HashSet<>();

            try {
                URL url = new URL(receivedURL);
                HttpURLConnection connection = null;

                for (int attempt = 1; attempt <=3; attempt++) {
                    try {
                        connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestMethod("GET");
                        connection.setConnectTimeout(10000);
                        connection.setReadTimeout(15000);

                        statusCode[0] = connection.getResponseCode();
                        break;
                    } catch (Exception e) {
                        if (attempt == 3) {
                            throw e;
                        }

                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException ignored) {}
                    }
                }

                if (statusCode[0] >= 200 && statusCode[0] < 400) {

                    String contentType = connection.getContentType();

                    if (contentType != null && contentType.toLowerCase().contains("text/html")) {

                        BufferedReader reader =
                                new BufferedReader(
                                        new InputStreamReader(
                                                connection.getInputStream()
                                        )
                                );

                        StringBuilder html = new StringBuilder();
                        String line;

                        while ((line = reader.readLine()) != null) {
                            html.append(line);
                        }

                        reader.close();

                        Matcher matcher = hrefPattern.matcher(html.toString());

                        while (matcher.find()) {
                            String href = matcher.group(1).trim();

                            if (href.isEmpty()) {
                                continue;
                            }

                            if (href.startsWith("#")) {
                                continue;
                            }

                            String lower = href.toLowerCase();

                            if (lower.startsWith("mailto:") || lower.startsWith("tel:") || lower.startsWith("javascript:")) {
                                continue;
                            }

                            URL absoluteURL;

                            try {
                                absoluteURL = new URL(url, href);
                            } catch (Exception e) {
                                continue;
                            }

                            String protocol = absoluteURL.getProtocol();

                            if (!protocol.equals("http") && !protocol.equals("https")) {
                                continue;
                            }

                            String host = absoluteURL.getHost();

                            boolean inDomain =
                                    host.equalsIgnoreCase(host1) || host.equalsIgnoreCase(host2);

                            if (!inDomain) {
                                continue;
                            }

                            String nextURL = absoluteURL.toString();

                            if (nextURL.toLowerCase().contains("latest")) {
                                continue;
                            }

                            foundLinks.add(nextURL);
                        }
                    }
                }

                connection.disconnect();

            } catch (Exception e) {
                System.out.println("Worker " + rank + " couldn't access the URL: " + e.getMessage());
            }

            int[] linkCount = {foundLinks.size()};

            StringBuilder linksBuilder = new StringBuilder();

            for (String link : foundLinks) {
                linksBuilder.append(link);
                linksBuilder.append("\n");
            }

            String linksString = linksBuilder.toString();
            int[] linksCharacters = new int[linksString.length()];
            int[] linksLength = {linksCharacters.length};

            for (int i = 0; i < linksString.length(); i++) {
                linksCharacters[i] = linksString.charAt(i);
            }

            MPI.COMM_WORLD.Send(
                    statusCode, 0, 1, MPI.INT, 0, tagStatusCode
            );

            MPI.COMM_WORLD.Send(
                    linkCount, 0, 1, MPI.INT, 0, tagLinkCount
            );

            MPI.COMM_WORLD.Send(
                    linksLength, 0, 1, MPI.INT, 0, tagLinksLength
            );

            if (linksLength[0] > 0) {
                MPI.COMM_WORLD.Send(
                        linksCharacters, 0, linksCharacters.length, MPI.INT, 0, tagLinks
                );
            }
        }
    }

    private static void sendString(String text, int destination) {

        int[] textCharacters = new int[text.length()];
        int[] textLength = {textCharacters.length};

        for (int i = 0; i < text.length(); i++) {
            textCharacters[i] = text.charAt(i);
        }

        MPI.COMM_WORLD.Send(
                textLength, 0, 1, MPI.INT, destination, tagURLLength
        );

        MPI.COMM_WORLD.Send(
                textCharacters, 0, textCharacters.length, MPI.INT, destination, tagURL
        );
    }

    private static String receiveString() {

        int[] textLength = new int[1];

        MPI.COMM_WORLD.Recv(
                textLength, 0, textLength.length, MPI.INT, 0, tagURLLength
        );

        int[] textCharacters = new int[textLength[0]];

        MPI.COMM_WORLD.Recv(
                textCharacters, 0, textCharacters.length, MPI.INT, 0, tagURL
        );

        StringBuilder textBuilder = new StringBuilder();

        for (int character : textCharacters) {
            textBuilder.append((char) character);
        }

        return textBuilder.toString();
    }

    private static void writeReport(
            long totalTime,
            Set<String> visited,
            Set<String> workingLinks,
            Set<String> brokenLinks,
            Map<String, Set<String>> foundOnPage
    ) {

        File outputFolder = new File("WebCrawlerLogs");
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }
        String outputPath = "WebCrawlerLogs/WebcrawlerDist.txt";

        try (BufferedWriter writer =
                     new BufferedWriter(
                             new FileWriter(outputPath)
                     )) {

            writer.write("Execution time (ms): " + totalTime + "\n");
            writer.write("Pages visited: " + visited.size() + "\n");
            writer.write("Working links: " + workingLinks.size() + "\n");
            writer.write("Broken links: " + brokenLinks.size() + "\n\n");

            writer.write("Working Links:\n");

            for (String link : workingLinks) {
                writer.write(link + "\n");
            }

            writer.write("\nBroken Links:\n");

            for (String brokenLink : brokenLinks) {

                writer.write(brokenLink + "\n");

                String urlOnly = brokenLink;
                int index = brokenLink.indexOf(" (");

                if (index != -1) {
                    urlOnly = brokenLink.substring(0, index);
                }

                Set<String> sources = foundOnPage.get(urlOnly);

                if (sources != null && !sources.isEmpty()) {

                    for (String source : sources) {
                        writer.write(" found on: " + source + "\n");
                    }

                } else {
                    writer.write(" found on: starting URL\n");
                }
            }

            System.out.println("Report saved to: " + outputPath);

        } catch (IOException e) {
            System.out.println("Could not write the report: " + e.getMessage());
        }
    }
}