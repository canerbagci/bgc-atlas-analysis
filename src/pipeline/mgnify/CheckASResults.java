package pipeline.mgnify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class CheckASResults {
    public static void main(String[] args) {
        String analysisDir = args[0];
        crawlDir(analysisDir);
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
                    System.out.println(dataset.getName() + "\t" + "success");
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
