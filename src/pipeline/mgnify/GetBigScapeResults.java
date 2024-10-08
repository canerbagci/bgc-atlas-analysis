package pipeline.mgnify;

import dbutil.Database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GetBigScapeResults {

    static Database database = new Database();

    public static void main(String[] args) {
        String resultsDir = "/gcfs/bigscape_compl_out/network_files/2023-05-03_16-13-17_hybrids_auto/";
        String bgcsPath = "/gcfs/compl_clusters/temp.txt";
        Map<String, String> bgc2AsmMap = parseBGCPaths(bgcsPath);
        parseResults(resultsDir, bgc2AsmMap);
    }

    private static Map<String, String> parseBGCPaths(String bgcsPath) {
        Map<String, String> bgcToAsmMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(bgcsPath)))) {
            String line = "";
            while((line = br.readLine()) != null) {
                String[] split = line.split("/");
                bgcToAsmMap.put(split[9], split[7]);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return bgcToAsmMap;
    }

    private static void parseResults(String resultsDir, Map<String, String> bgc2AsmMap) {
        try {
            File directory = new File(resultsDir);
            File[] files = directory.listFiles();
            for(File file : files) {
                if(file.isDirectory()) {
                    String type = file.getName();
                    System.out.println(type + "\t" + file.getAbsolutePath());
                    parseType(type, file, bgc2AsmMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parseType(String type, File dir, Map<String, String> bgc2AsmMap) {
        try {
            File[] files = dir.listFiles();
            for(File f : files) {
                if(f.getName().contains("clustering")) {
                    String[] split = f.getName().split("_");
                    double clusteringThreshold = Double.parseDouble(split[split.length -1 ].
                            replace(".tsv", "").replaceAll("c", ""));
                    try(BufferedReader br = new BufferedReader(new FileReader(f))) {
                        String line = br.readLine();
                        while((line = br.readLine()) != null) {
                            String[] lineSplit = line.split("\t");
                            if(lineSplit[0].contains("region")) {
                                database.insertClustering(lineSplit[0], type, Integer.parseInt(lineSplit[1]), clusteringThreshold,
                                        bgc2AsmMap.get(lineSplit[0] + ".gbk"));
                            } else {
                                database.insertClustering(lineSplit[0], type, Integer.parseInt(lineSplit[1]), clusteringThreshold,
                                        "mibig2.1");
                            }
                        }
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                } else if (f.getName().contains(".network")) {
//                    String[] split = f.getName().split("_");
//                    double clusteringThreshold = Double.parseDouble(split[split.length -1 ].
//                            replace(".network", "").replaceAll("c", ""));
//                    try(BufferedReader br = new BufferedReader(new FileReader(f))) {
//                        String line = br.readLine();
//                        while((line = br.readLine()) != null) {
//                            String[] lineSplit = line.split("\t");
//                            String clustername1 = lineSplit[0];
//                            String clustername2 = lineSplit[1];
//                            double rawDistance = Double.parseDouble(lineSplit[2]);
//                            double squaredSimilarity = Double.parseDouble(lineSplit[3]);
//                            double jaccardIndex = Double.parseDouble(lineSplit[4]);
//                            double dssIndex = Double.parseDouble(lineSplit[5]);
//                            double adjacencyIndex = Double.parseDouble(lineSplit[6]);
//                            double rawDssNonAnchor = Double.parseDouble(lineSplit[7]);
//                            double rawDssAnchor = Double.parseDouble(lineSplit[8]);
//                            int nonAnchorDomains = Integer.parseInt(lineSplit[9]);
//                            int anchorDomains = Integer.parseInt(lineSplit[10]);
//                            String combinedGroup = lineSplit[11];
//                            String sharedGroup = "";
//                            if(lineSplit.length == 13) {
//                                sharedGroup = lineSplit[12];
//                            }
//
//                            database.insertNetworkEdge(clustername1, clustername2, rawDistance, squaredSimilarity, jaccardIndex,
//                                    dssIndex, adjacencyIndex, rawDssNonAnchor, rawDssAnchor, nonAnchorDomains, anchorDomains,
//                                    combinedGroup, sharedGroup, type, clusteringThreshold);
//                        }
//                    } catch (IOException ioe) {
//                        ioe.printStackTrace();
//                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
