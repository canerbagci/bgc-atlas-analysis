package pipeline.mgnify.antismash;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static pipeline.mgnify.AnalyzeDatasets.database;
import static pipeline.mgnify.AnalyzeDatasetsBinAC.transferResults;

public class AntismashRunner implements Runnable {

    private final String analysisDir;
    private final String assemblyId;
    private final int numCores;
    private final String server;
    private String condaEnv;
    private final String condaPath;
    private int exitCode = -2;
    private final CountDownLatch latch;


    public AntismashRunner(String analysisDir, String assemblyId, int numCores, CountDownLatch latch, String server) {
        this.analysisDir = analysisDir;
        this.assemblyId = assemblyId;
        this.numCores = numCores;
        this.latch = latch;
        this.server = server;
        this.condaEnv = "";
        this.condaPath = "";
    }

    public AntismashRunner(String analysisDir, String assemblyId, int nThreads, CountDownLatch latch, String server, String condaEnv, String condaPath) {
        this.analysisDir = analysisDir;
        this.assemblyId = assemblyId;
        this.numCores = nThreads;
        this.latch = latch;
        this.server = server;
        this.condaEnv = condaEnv;
        this.condaPath = condaPath;
    }

    @Override
    public void run() {

        try {
            latch.await();

//            database.updateRunStatus(assemblyId, "runningAS");

            System.out.println("Running antismash for " + assemblyId);

            File[] inputFiles = new File(analysisDir + File.separator + assemblyId).
            listFiles(pathname -> pathname.getName().endsWith("fasta.gz"));

            deleteDirectory(new File(analysisDir + File.separator + assemblyId + File.separator + "antismash"));

            for(File f : Objects.requireNonNull(inputFiles)) {

                String condaPath = "";
                if("denbi".equals(server)) {
                    condaPath = "/home/ubuntu/anaconda3/etc/profile.d/conda.sh";
                } else if ("binAC".equals(server)) {
                    condaPath = "/beegfs/work/tu_iijcb01/software/miniforge3/etc/profile.d/conda.sh";
//                    condaPath = "/home/tu/tu_tu/tu_iijcb01/miniconda3/etc/profile.d/conda.sh";
                } else if("azure".equals(server)){
                    condaPath = "...";
                } else if("smriti".equals(server)) {
                    condaPath = this.condaPath + "/etc/profile.d/conda.sh";
                }

                if("".equals(condaEnv)) {
                    condaEnv = "antismash7";
                }

                List<String> commandList = new ArrayList<>();
                commandList.add("bash");
                commandList.add("-c");
                commandList.add("source " + condaPath + " && conda activate " + condaEnv + " && " +
                        "antismash -c " + numCores + " --output-dir " + analysisDir + File.separator +
                        assemblyId + File.separator + "antismash" + " --output-basename " + assemblyId +
//                        " --fullhmmer --clusterhmmer --tigrfam --asf --cc-mibig --cb-general --cb-subclusters" +
                        " --clusterhmmer --tigrfam --asf --cc-mibig --cb-subclusters" +
                        " --cb-knownclusters --pfam2go --rre --tfbs" +
                        " --genefinding-tool prodigal-m " + "--allow-long-headers --logfile " + analysisDir +
                        File.separator + assemblyId + File.separator + "antismash" + File.separator + "antismash_log.txt " +
                        f.getAbsolutePath());

                ProcessBuilder pb = new ProcessBuilder(commandList);

                pb.inheritIO();

                Process process = pb.start();
                this.exitCode = process.waitFor();

                System.out.println("antismash run has finished with the exit code: " + exitCode);
            }

            if(exitCode == 0) {
                transferResults(analysisDir, assemblyId, this.server);

            } else {
//                database.updateRunStatus(assemblyId, "failed");
            }

            latch.countDown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getExitCode() {
        return exitCode;
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
