package pipeline.mgnify;

import data.mgnify.APICrawler;
import data.mgnify.AnalysisPage;
import data.mgnify.Assembly;
import data.mgnify.Sample;
import dbutil.Database;

import javax.json.JsonObject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DownloadMGnifyAsmInfo {

//    private final static Database database = new Database();

    public static void main(String[] args) {

        String outDir = args[0];

        new File(outDir + File.separatorChar + "pages").mkdir();
        new File(outDir + File.separatorChar + "assemblies").mkdir();
        new File(outDir + File.separatorChar + "samples").mkdir();

        List<Assembly> assemblies = new ArrayList<>();

        int pageSize = 1000;

        JsonObject pagination = APICrawler.getPagination(pageSize);

        int firstPage = pagination.getInt("page");
        int numPages = pagination.getInt("pages");
        int count = pagination.getInt("count");

        System.out.println("Total objects: " + count);
        System.out.println("Total pages: " + numPages);

        firstPage = Integer.parseInt(args[1]);
        numPages = numPages; //TODO: debugging

        List<AnalysisPage> analysisPageList = new ArrayList<>();

        Map<String, Integer> metadataKeysCount = new HashMap<>();

        try(BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outDir + File.separatorChar + "assemblies.txt")))) {

            bw.write("assembly\tsampleAcc\tsubmittedSeqs\tenvMat\tlongitude\tlatitude\tenvBiome\tcollecDate\tspecies\tgeoLoc\tbiosample\thostTaxID\tenvFeat\tdownloadLink");
            bw.newLine();

            for (int i = firstPage; i <= numPages; i++) {
                JsonObject pageData = APICrawler.getPageData(i, pageSize);
                AnalysisPage analysisPage = new AnalysisPage(pageData, outDir, i);
                List<Assembly> pageAssemblies = analysisPage.parse(outDir);
                assemblies.addAll(pageAssemblies);
//                analysisPageList.add(analysisPage);

                for (Assembly assembly : pageAssemblies) {
                    Sample sample = assembly.getSample();
                    Map<String, String> attributes = sample.getAttributes();
                    bw.write(assembly.getId() + "\t" + sample.getId() + "\t" + assembly.getAnalysisSummary().get("Submitted nucleotide sequences") + "\t" +
                            attributes.get("environmentMaterial") + "\t" + attributes.get("longitude") + "\t" + attributes.get("latitude") + "\t" +
                            attributes.get("environmentBiome") + "\t" + attributes.get("collectionDate") + "\t" + attributes.get("species") + "\t" +
                            attributes.get("geoLocName") + "\t" + attributes.get("biosample") + "\t" + attributes.get("hostTaxID") + "\t" +
                            attributes.get("environmentFeature") + "\t" + assembly.getDownloadLink());
                    bw.newLine();

                    for (String key : sample.getMetadata().keySet()) {
                        metadataKeysCount.putIfAbsent(key, 0);
                        metadataKeysCount.put(key, metadataKeysCount.get(key) + 1);
                    }

                }

//                if(pageAssemblies.size() > 0)
//                    database.insertAssemblies(pageAssemblies);

                System.out.println("Total assemblies: " + assemblies.size());
            }

            BufferedWriter metadataKeys = new BufferedWriter(new FileWriter(new File(outDir + File.separatorChar +  "metadatakeys.txt")));
            for(String key : metadataKeysCount.keySet()) {
                metadataKeys.write(key + "\t" + metadataKeysCount.get(key));
                metadataKeys.newLine();
            }
            metadataKeys.close();

        }catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
