package pipeline.mgnify;


import data.mgnify.APICrawler;
import dbutil.Database;
import pipeline.mgnify.antismash.AntismashRunner;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AnalyzeDatasetsBinAC {

    public final static Database database = new Database(false);
    public static final int N_THREADS = 1;
    public static final int DOWNLOAD_THREADS = 1;

    public static void main(String[] args) {
        String assembliesFile = args[0];
        String analysisDir = args[1];
        String server = args[2];
        String dataset = args[3];

        new File(analysisDir + File.separatorChar + "datasets").mkdir();

//        analyzeIteratively(analysisDir + File.separatorChar + "datasets", server, dataset);
        analyzeDataset(analysisDir + File.separator + "datasets", server, dataset);

    }

    private static void analyzeDataset(String analysisDir, String server, String dataset) {
        ExecutorService executorAS = Executors.newFixedThreadPool(N_THREADS);
        String assemblyId = dataset;

        CountDownLatch latch = new CountDownLatch(1);
        AntismashRunner antismashRunner = runAntismash(analysisDir, assemblyId, latch, server);

        System.out.println("submitting jobs for " + assemblyId);
        executorAS.submit(antismashRunner);

        executorAS.shutdown();
        try {
            executorAS.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void analyzeIteratively(String analysisDir, String server, String dataset) {

        Map<String, Integer> labels = new HashMap<>();

        ExecutorService executorAS = Executors.newFixedThreadPool(N_THREADS);
        ExecutorService downloader = Executors.newFixedThreadPool(DOWNLOAD_THREADS);

        String[] nextInQueue = database.getNextInQueueRand();
        System.out.println("next in queue: " + Arrays.toString(nextInQueue));
        String assemblyId = nextInQueue[0];
        String link = nextInQueue[1];

        String runStatus = database.getRunStatus(assemblyId);
        String queueString = "in queue " + server;
        if(queueString.equals(runStatus)) {
            System.exit(0);
        }

        database.updateRunStatus(assemblyId, "in queue " + server);

        JsonObject assemblyObject = APICrawler.getJsonObjectFromURL(link);

        Map<String, List<String>> selfLinksMap = new HashMap<>();
        parsePage(labels, assemblyObject, selfLinksMap);

        CountDownLatch latch = new CountDownLatch(1);

        AssemblyDownloader assemblyDownloader = downloadLinks(selfLinksMap, analysisDir, assemblyId, latch);
        AntismashRunner antismashRunner = runAntismash(analysisDir, assemblyId, latch, server);

        System.out.println("submitting jobs for " + assemblyId);
        downloader.submit(assemblyDownloader);
        executorAS.submit(antismashRunner);

        downloader.shutdown();
        executorAS.shutdown();
        try {
            downloader.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            executorAS.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void transferResults(String analysisDir, String assemblyId, String server) {
//        System.out.println("Transferring results");
//        ProcessBuilder pb = new ProcessBuilder("scp", "-r", "-i", "/home/tu/tu_tu/tu_iijcb01/.ssh/id_rsa_denbi",
//                analysisDir + File.separator + assemblyId,
//                "ubuntu@193.196.20.94:/vol/atlas/mgnify/data/analysis/datasets/");
//        pb.inheritIO();
        try {
//            int i = pb.start().waitFor();
//            System.out.println("Done transferring results " + assemblyId);

//            database.updateDetails(assemblyId, "7.0.0.beta2", "0.0.1", server,
//                    analysisDir + File.separator + assemblyId + File.separator + "antismash", "transfer");

//            AntismashRunner.deleteDirectory(new File(analysisDir + File.separator + assemblyId));

//            System.out.println("Deleted results " + assemblyId);

//        } catch (InterruptedException e) {
//            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static AssemblyDownloader downloadLinks(Map<String, List<String>> selfLinksMap, String analysisDir,
                                                    String assemblyId, CountDownLatch latch) {
        AssemblyDownloader downloader = new AssemblyDownloader(selfLinksMap, analysisDir, assemblyId, latch);
        return downloader;
    }

    private static AntismashRunner runAntismash(String analysisDir, String assemblyId, CountDownLatch latch, String server) {
        AntismashRunner asRunner = new AntismashRunner(analysisDir, assemblyId, 1, latch, server);
        return asRunner;
    }


    private static void parsePage(Map<String, Integer> labels, JsonObject assemblyObject, Map<String,
            List<String>> selfLinksMap) {
        JsonArray dataArray = assemblyObject.getJsonArray("data");

        for (JsonValue datum : dataArray) {
            JsonObject attributes = ((JsonObject) datum).getJsonObject("attributes");
            JsonObject description = attributes.getJsonObject("description");
            String label = description.getString("label");

            labels.putIfAbsent(label, 0);
            labels.put(label, labels.get(label) + 1);

            JsonObject selfLinksObj = ((JsonObject) datum).getJsonObject("links");
            String self = selfLinksObj.getString("self");

            selfLinksMap.putIfAbsent(label, new ArrayList<>());
            selfLinksMap.get(label).add(self);
        }

        JsonObject links = assemblyObject.getJsonObject("links");
        JsonValue next = links.get("next");
        if(!next.toString().equals("null")) {
            JsonObject nextPage = APICrawler.getJsonObjectFromURL(next.toString().replaceAll("\"", ""));
            parsePage(labels, nextPage, selfLinksMap);
        }
    }

}
