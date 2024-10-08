package pipeline.mgnify;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class CreateBigsliceGcfTable {

    static String regionsTable = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/regions.tab";
    static String outputGCFTable = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/gcfs.tab";
    static String outputGCFMembershipTable = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/gcf-membership.tab";

    public static void main(String[] args) {
        createTable(regionsTable, outputGCFTable, outputGCFMembershipTable);
    }

    private static void createTable(String regionsTable, String outputTable, String outputGCFMembershipTable) {
        //header: region_id       assembly        contig_name     contig_len      product_categories      anchor  start   end     contig_edge     type    products        region_num      bigslice_region_id      bigslice_gcf_id longest_biome   membership_value

        Map<Integer, GCF> gcfMap = new HashMap<>();

        int totalCount = 0;

        try(BufferedReader br = new BufferedReader(new FileReader(regionsTable))) {

            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputGCFMembershipTable)));

            String line = br.readLine();
            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                if(lineSplit.length < 13 || lineSplit[13].isEmpty())
                    continue;

                double membershipValue = Double.parseDouble(lineSplit[15]);
                if(membershipValue > 0.4)
                    continue;

                System.out.println(Arrays.toString(lineSplit));

                Integer gcfId = Integer.parseInt(lineSplit[13]);
                String biome = lineSplit[14];
                String product = lineSplit[9];

                int bigsliceBGCId = Integer.parseInt(lineSplit[12]);

                GCF gcf;
                if(gcfMap.containsKey(gcfId)) {
                    gcf = gcfMap.get(gcfId);
                } else {
                    gcf = new GCF();
                    gcf.gcfId = gcfId;
                    gcfMap.put(gcfId, gcf);
                }

                gcf.biomeCount.putIfAbsent(biome, 0);
                gcf.biomeCount.put(biome, gcf.biomeCount.get(biome) + 1);

                String[] productSplit = product.split(",");
                for (String s : productSplit) {
                    gcf.productCount.putIfAbsent(s, 0);
                    gcf.productCount.put(s, gcf.productCount.get(s) + 1);
                }

                gcf.regCount++;

                totalCount++;

                bw.write(gcfId + "\t" + bigsliceBGCId + "\t" + lineSplit[0] + "\t" + membershipValue + "\t" + "0.4");
                bw.newLine();
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("total count: " + totalCount);

        try(BufferedWriter bw  = new BufferedWriter(new FileWriter(new File(outputTable)))) {

            for(int gcf : gcfMap.keySet()) {
                GCF gcfObj = gcfMap.get(gcf);
                bw.write(gcfObj.gcfId + "\t" + gcfObj.regCount + "\t");

                // Use StringJoiner for product counts
                StringJoiner productJoiner = new StringJoiner(", ");
                for (Map.Entry<String, Integer> productEntry : gcfObj.productCount.entrySet()) {
                    productJoiner.add(productEntry.getKey() + " (" + productEntry.getValue() + ")");
                }
                bw.write(productJoiner.toString());

                bw.write("\t");

                // Use StringJoiner for biome counts
                StringJoiner biomeJoiner = new StringJoiner(", ");
                for (Map.Entry<String, Integer> biomeEntry : gcfObj.biomeCount.entrySet()) {
                    biomeJoiner.add(biomeEntry.getKey() + " (" + biomeEntry.getValue() + ")");
                }
                bw.write(biomeJoiner.toString());

                bw.newLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }


    static class GCF {
        int gcfId;
        Map<String, Integer> biomeCount = new HashMap<>();
        Map<String, Integer> productCount = new HashMap<>();
        int regCount = 0;
    }


}
