package pipeline.mgnify;


import data.mgnify.APICrawler;
import dbutil.Database;
import pipeline.mgnify.antismash.AntismashRunner;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class AnalyzeDatasets {

//    public final static Database database = new Database();
    public final static Database database = new Database(false);
    public static int N_THREADS = 1;
    public static final int DOWNLOAD_THREADS = 1;
    private static String assembliesFile;
    private static String analysisDir;
    private static String server;
    private static String assembly;

    private static String condaEnv;

    private static String condaPath;

    public static void main(String[] args) {

        parseArgs(args);
//
//        String assembliesFile = args[0];
//        String analysisDir = args[1];
//        String server = args[2]; // denbi, binAC, azure

        System.out.println("args: " + Arrays.toString(args));

//        new File(analysisDir + File.separatorChar + "datasets").mkdir();
//
//        analyzeIteratively(analysisDir + File.separatorChar + "datasets", server);

        String assemblyLink = getAssemblyLink(assembliesFile, assembly);

//        System.out.println("assembly link: " + assemblyLink);

        analyzeDataset(assemblyLink);

        System.out.println("done");

    }

    private static String getAssemblyLink(String assembliesFile, String assembly) {
        try(BufferedReader br = new BufferedReader(new FileReader(assembliesFile))) {
            String line;
            while((line = br.readLine()) != null) {
                String[] split = line.split("\t");
                String assemblyId = split[0];
                String link = split[13];
                if(assembly.equals(assemblyId)) {
                    return link;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return "";
    }

    private static void analyzeDataset(String assemblyLink) {

        Map<String, Integer> labels = new HashMap<>();

        ExecutorService executorAS = Executors.newFixedThreadPool(N_THREADS);
        ExecutorService downloader = Executors.newFixedThreadPool(DOWNLOAD_THREADS);

        String assemblyId = assembly;

        JsonObject assemblyObject = APICrawler.getJsonObjectFromURL(assemblyLink);

//        System.out.println(assemblyObject.toString());

        Map<String, List<String>> selfLinksMap = new HashMap<>();
        parsePage(labels, assemblyObject, selfLinksMap);

        CountDownLatch latch = new CountDownLatch(1);

        AssemblyDownloader assemblyDownloader = downloadLinks(selfLinksMap, analysisDir, assemblyId, latch);
        AntismashRunner antismashRunner = runAntismash(analysisDir, assemblyId, latch, server, condaEnv, condaPath, N_THREADS);
//        AntismashRunner antismashRunner = runAntismash(analysisDir, assemblyId, latch, server);

        System.out.println("submitting jobs for " + assemblyId);
        downloader.submit(assemblyDownloader);
        executorAS.submit(antismashRunner);

//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
        downloader.shutdown();
        executorAS.shutdown();

        try {
            executorAS.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void parseArgs(String[] args) {

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-a":
                    assembliesFile = args[i + 1];
                    break;
                case "-d":
                    analysisDir = args[i + 1];
                    break;
                case "-s":
                    server = args[i + 1];
                    break;
                case "-i":
                    assembly = args[i + 1];
                    break;
                case "-e":
                    condaEnv = args[i + 1];
                    break;
                case "-c":
                    condaPath = args[i + 1];
                    break;
                case "-t":
                    N_THREADS = Integer.parseInt(args[i + 1]);
                    break;
                case "-h":
                    printHelp();
                    break;
                default:
                    break;
            }
        }

    }

    private static void printHelp() {
        System.out.println("Usage: java -jar AnalyzeDatasets.jar -a <assembliesFile> -d <analysisDir> -s <server> -i <assembly> -c <condaPath> -e <condaEnv> -t <threads>");
        System.out.println("Options:");
        System.out.println("-a <assembliesFile> : file with MGnify assembly data");
        System.out.println("-d <analysisDir> : directory to store analysis results");
        System.out.println("-s <server> : server to run analysis on (options: denbi, binAC, azure, smriti)");
        System.out.println("-i <assembly> : assembly to analyze");
        System.out.println("-c <condaPath> : path to conda installation. e.g. \"/beegfs/work/tu_iijcb01/software/miniforge3/\" ");
        System.out.println("-e <condaEnv> : antiSMASH 7 conda environment to use. e.g. \"antismash7\"");
        System.out.println("-t <threads> : number of threads to use for analysis. recommend 1. (needs to be adjusted in the job script as well)");
        System.exit(0);
    }

    private static void analyzeIteratively(String analysisDir, String server) {

        Map<String, Integer> labels = new HashMap<>();

        ExecutorService executorAS = Executors.newFixedThreadPool(N_THREADS);
        ExecutorService downloader = Executors.newFixedThreadPool(DOWNLOAD_THREADS);

//        ThreadPoolExecutor executorAS = new ThreadPoolExecutor(N_THREADS, N_THREADS, 0L, TimeUnit.MILLISECONDS,
//                new ArrayBlockingQueue<>(N_THREADS));

        while(true) {
            String[] nextInQueue = database.getNextInQueueRand();
            String assemblyId = nextInQueue[0];
            String link = nextInQueue[1];

            String runStatus = database.getRunStatus(assemblyId);
            String queueString = "in queue " + server;
            if(queueString.equals(runStatus)) {
                continue;
            }

            database.updateRunStatus(assemblyId, "in queue " + server);

            JsonObject assemblyObject = APICrawler.getJsonObjectFromURL(link);

            Map<String, List<String>> selfLinksMap = new HashMap<>();
            parsePage(labels, assemblyObject, selfLinksMap);

            CountDownLatch latch = new CountDownLatch(1);

            AssemblyDownloader assemblyDownloader = downloadLinks(selfLinksMap, analysisDir, assemblyId, latch);
            AntismashRunner antismashRunner = runAntismash(analysisDir, assemblyId, latch, server);

            System.out.println("submitting jobs for " + assemblyId );
            downloader.submit(assemblyDownloader);
            executorAS.submit(antismashRunner);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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

    private static AntismashRunner runAntismash(String analysisDir, String assemblyId, CountDownLatch latch, String server, String condaEnv, String condaPath, int nThreads) {
        AntismashRunner asRunner = new AntismashRunner(analysisDir, assemblyId, nThreads, latch, server, condaEnv, condaPath);
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
