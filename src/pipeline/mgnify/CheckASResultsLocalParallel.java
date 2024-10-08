package pipeline.mgnify;

import dbutil.Database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CheckASResultsLocalParallel {

    public final static Database database = new Database();
    public static final int N_THREADS = 4;

    public static void main(String[] args) {
        String analysisDir = args[0];
        crawlDir(analysisDir);
//        database.closeConnection();
    }

    private static void crawlDir(String analysisDir) {

        List<String> allFinishedRuns = database.getAllFinishedRuns();

        ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);

        File dir = new File(analysisDir);
        File[] datasets = dir.listFiles();
        System.out.println(datasets.length);
        for(File dataset : datasets) {
            if(allFinishedRuns.contains(dataset)) {
                continue;
            }
            DatasetChecker checker = new DatasetChecker(dataset);
            executor.submit(checker);
        }

        executor.shutdown();
    }

    static class DatasetChecker implements Runnable {

        private final File dataset;

        public DatasetChecker(File dataset) {
            this.dataset = dataset;
        }

        @Override
        public void run() {
            if(!dataset.isDirectory())
                return;
            File ASlog = new File(dataset.getAbsolutePath() + File.separator + "antismash/antismash_log.txt");
            if(!ASlog.exists()) {
                database.insertRun(dataset.getName(), "7.0.0beta2", "0.1", "binAC", dataset.getAbsolutePath() +
                        File.separator + "antismash", "no file");
                return;
            }
            try(BufferedReader br = new BufferedReader(new FileReader(ASlog))) {
                String line = "";
                String lastLine = "";
                String firstLine = br.readLine();
                while((line = br.readLine()) != null) {
                    lastLine = line;
                }
                if(lastLine.contains("antiSMASH status: SUCCESS")) {
                    database.insertRun(dataset.getName(), "7.0.0beta2", "0.1", "binAC", dataset.getAbsolutePath() +
                            File.separator + "antismash", "success");
//                    System.out.println(dataset.getName() + "\t" + "success");
                } else if(lastLine.startsWith("INFO") || lastLine.startsWith("ERROR")) {
                    database.insertRun(dataset.getName(), "7.0.0beta2", "0.1", "binAC", dataset.getAbsolutePath() +
                            File.separator + "antismash", "incomplete");
                }
                else {
                    System.out.println(dataset + "\t" + lastLine);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
