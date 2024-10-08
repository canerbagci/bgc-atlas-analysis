package pipeline.mgnify;

import dbutil.Database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Array;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class CreateGCFTable {


    static final Database database = new Database();

    public static void main(String[] args) {

        String gcfTable = "/vol/compl_bgcs_bigslice_def_t0.4_dec/gcf_table.tsv";

        writeGCFTable(gcfTable);
    }

    private static void writeGCFTable(String gcfTable) {
        try {

            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(gcfTable)));

            ResultSet gcfIDs = database.executeQuery("SELECT DISTINCT(gcf_id) FROM bigslice_gcf_membership");
            while(gcfIDs.next()) {
                int gcfId = gcfIDs.getInt(1);

                int regCount = 0;
                Map<String, Integer> productCount = new HashMap<>();
                Map<String, Integer> biomeCount = new HashMap<>();

                ResultSet regions = database.executeQuery("SELECT * FROM regions WHERE bigslice_gcf_id = " + gcfId);

                while(regions.next()) {
                    regCount++;
                    String assembly = regions.getString(2);
                    Array products = regions.getArray(11);

                    String[][] productArray = (String[][]) products.getArray();
                    for (String[] productRow : productArray) {
                        for (String product : productRow) {
                            productCount.putIfAbsent(product, 0);
                            productCount.put(product, productCount.get(product) + 1);
                        }
                    }

                    ResultSet longestBiome = database.executeQuery("SELECT longest_biome FROM assembly2longestbiome WHERE assembly = '" + assembly + "'");
                    if(longestBiome.next()) {
                        String longestBiomeString = longestBiome.getString(1);
                        biomeCount.putIfAbsent(longestBiomeString, 0);
                        biomeCount.put(longestBiomeString, biomeCount.get(longestBiomeString) + 1);
                    }
                }

                Map<String, Integer> pcSorted = sortMap(productCount);
                Map<String, Integer> bcSorted = sortMap(biomeCount);

                bw.write(gcfId + "\t" + regCount + "\t");

                String[] pcString = {""};
                pcSorted.forEach((k, v) -> pcString[0] += (k + " (" + v + "), "));
                bw.write(pcString[0].substring(0, pcString[0].length() - 2) + "\t");

                String[] bcString = {""};
                bcSorted.forEach((k, v) -> bcString[0] += (k + " (" + v + "), "));
                if(!bcString[0].isEmpty()) {
                    bw.write(bcString[0].substring(0, bcString[0].length() - 2));
                }
                bw.newLine();
            }

            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> sortMap(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());

        Collections.sort(list, (entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

        Map<String, Integer> sortedMap = list.stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        return sortedMap;
    }
}
