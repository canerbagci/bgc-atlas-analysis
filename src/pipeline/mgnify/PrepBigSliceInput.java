package pipeline.mgnify;

import dbutil.Database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class PrepBigSliceInput {

    static Database database = new Database();

    public static void main(String[] args) {
        String inputDir = args[0];

        prepDir(inputDir);
    }

    private static void prepDir(String inputDir) {
        try {
            BufferedWriter bwDatasets = new BufferedWriter(new FileWriter(new File(inputDir + File.separator + "datasets.tsv")));
            bwDatasets.write("#Dataset name\tPath to folder\tPath to taxonomy\tDescription");
            bwDatasets.newLine();
            new File(inputDir + File.separator + "taxonomy").mkdir();

            List<String> allFinishedRuns = database.getAllFinishedRuns();
            for(String run : allFinishedRuns) {
                if(!new File("/vol/atlas/mgnify/data/analysis/datasets/" + run).exists())
                    continue;

                bwDatasets.write(run + "\t" + "datasets/" + run + "\t"
                        + "taxonomy" + File.separator + "taxonomy_" + run + ".tsv" + "\t" +
                        run);
                bwDatasets.newLine();
                BufferedWriter bwTax = new BufferedWriter(new FileWriter(new File(inputDir + File.separator + "taxonomy" +
                        File.separator + "taxonomy_" + run + ".tsv")));
                bwTax.write("antismash\tBacteria\t \t \t \t \t \t" + run);
                bwTax.newLine();
                bwTax.close();
            }


            bwDatasets.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
