package pipeline.mgnify;

import dbutil.Database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class CheckASResultsLocal {

    public final static Database database = new Database();
    public static void main(String[] args) {
        String analysisDir = args[0];
        crawlDir(analysisDir);
        database.closeConnection();
    }

    private static void crawlDir(String analysisDir) {
        File dir = new File(analysisDir);
        File[] datasets = dir.listFiles();
        for(File dataset : datasets) {
            if(!dataset.isDirectory())
                continue;
            File ASlog = new File(dataset.getAbsolutePath() + File.separator + "antismash/antismash_log.txt");
            try(BufferedReader br = new BufferedReader(new FileReader(ASlog))) {
                String line = "";
                String lastLine = "";
                String firstLine = br.readLine();
                while((line = br.readLine()) != null) {
                    lastLine = line;
                }
                if(lastLine.contains("antiSMASH status: SUCCESS")) {
                    database.insertRun(dataset.getName(), "7.0.0beta2", "0.1", "binAC_totransfer", dataset.getAbsolutePath() +
                            File.separator + "antismash", "success");

//                    database.updateRunStatus(dataset.getName(), "success");
                    System.out.println(dataset.getName() + "\t" + "success");
                } else {
                    System.out.println(dataset.getName() + "\t" + lastLine);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
