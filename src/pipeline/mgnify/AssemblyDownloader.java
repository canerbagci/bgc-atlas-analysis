package pipeline.mgnify;

import data.mgnify.APICrawler;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static pipeline.mgnify.AnalyzeDatasets.database;

public class AssemblyDownloader implements Runnable{

    private final Map <String, List<String>> selfLinksMap;
    private final String analysisDir;
    private final String id;
    private final CountDownLatch latch;

    public AssemblyDownloader(Map<String, List<String>> selfLinksMap, String analysisDir, String id, CountDownLatch latch) {
        this.selfLinksMap = selfLinksMap;
        this.analysisDir = analysisDir;
        this.id = id;
        this.latch = latch;
    }

    @Override
    public void run() {
        this.downloadLinks();
    }

    private void downloadLinks() {
//        database.updateRunStatus(id, "downloading");
        System.out.println("Downloading " + id);

        new File(analysisDir + File.separatorChar + id).mkdir();

//        if(selfLinksMap.containsKey("Diamond annotation")) {
//            for(String url : selfLinksMap.get("Diamond annotation")) {
//                String filename = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));
//                APICrawler.downloadFile(url, analysisDir + File.separatorChar + id + File.separatorChar + filename);
//            }
//        }

        if(selfLinksMap.containsKey("antiSMASH summary")) {
            for(String url : selfLinksMap.get("antiSMASH summary")) {
                String filename = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));
                System.out.println("Downloading: " + filename);
                APICrawler.downloadFile(url, analysisDir + File.separatorChar + id + File.separatorChar + filename);
            }
        }

        if(selfLinksMap.containsKey("Processed contigs")) {
            for(String url : selfLinksMap.get("Processed contigs")) {
                String filename = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));
                System.out.println("Downloading: " + filename);
                APICrawler.downloadFile(url, analysisDir + File.separatorChar + id + File.separatorChar + filename);
                modifyFile(analysisDir + File.separatorChar + id + File.separatorChar + filename, id);
            }
        }

//        if(selfLinksMap.containsKey("antiSMASH annotation")) {
//            for(String url : selfLinksMap.get("antiSMASH annotation")) {
//                String filename = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));
//                APICrawler.downloadFile(url, analysisDir + File.separatorChar + id + File.separatorChar + filename);
//            }
//        }

//        database.updateRunStatus(id, "downloaded");

        latch.countDown();
    }

    private void modifyFile(String filePath, String dataset) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath))));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(filePath + "_mod"))));

            String line;
            while((line = br.readLine()) != null) {
                if(line.startsWith(">")) {
                    bw.write(">" + id + "_" + line.substring(1) + "\n");
                } else {
                    bw.write(line + "\n");
                }
            }

            br.close();
            bw.close();

            new File(filePath).delete();
            new File(filePath + "_mod").renameTo(new File(filePath));

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
