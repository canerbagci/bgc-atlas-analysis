package pipeline.mgnify;

import dbutil.Database;
import pipeline.mgnify.antismash.io.GenBank;

import java.io.File;
import java.util.List;

public class GetASResults {

    static Database database = new Database();

    public static void main(String[] args) {

        String resDir = args[0];

        analyzeResults(resDir);

    }

    private static void analyzeResults(String resDir) {
        List<String> finishedRuns = database.getNewFinishedRuns();
        System.out.println("Total finished runs: " + finishedRuns.size());
        for(String run : finishedRuns) {
            System.out.println(run);
            analyzeRun(resDir, run);
//            break;
        }

    }

    private static void analyzeRun(String resDir, String run) {
        try {
            File antismashDir = new File(resDir + File.separator + run + File.separator + "antismash");
            File[] files = antismashDir.listFiles();
            for(File f  : files) {
                if(f.getAbsolutePath().endsWith(".gbk") && f.getAbsolutePath().contains(".region")) {
                    String[] fSplit = f.getName().split("\\.");
                    String region = fSplit[fSplit.length - 2];
                    GenBank genbank = new GenBank(f);
                    genbank.read();
                    String accession = genbank.getAccession();
                    List<GenBank.ProtoCluster> protoClusters = genbank.getProtoClusters();
                    for(GenBank.ProtoCluster pc : protoClusters) {
//                        System.out.println(accession + "\t" + region + "\t" + pc.getNumber() + "\t" + pc.getCategory() +
//                                "\t" + pc.getProduct() +
//                                "\t" + pc.getContigEdge());
                        database.insertPC(run, accession, region, pc.getNumber(), pc.getCategory(), pc.getProduct(),
                                pc.getContigEdge(), f.getAbsolutePath());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
