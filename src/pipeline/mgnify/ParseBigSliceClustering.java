package pipeline.mgnify;

import dbutil.Database;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ParseBigSliceClustering {

    static final Database database = new Database();

    public static void main(String[] args) {
        String gcfMembershipFile = "/ceph/ibmi/tgm/bgc-atlas/data/analysis/gcf-membership.txt"; //exported from bigslice sqlite
        String bgcInfoFile = "/ceph/ibmi/tgm/bgc-atlas/data/analysis/bgc-info.txt"; //exported from bigslice sqlite

        // \copy protoclusters TO 'protoclusters.tab' WITH (FORMAT CSV, DELIMITER E'\t', HEADER true)
        String protoclustersTable = "/ceph/ibmi/tgm/bgc-atlas/data/analysis/as_regions_export.txt"; //exported from bgc-atlas postgresql

        String newProtoclustersTable = "/ceph/ibmi/tgm/bgc-atlas/data/analysis/as_regions_5_new.txt";
        String gcfMembershipTable = "/ceph/ibmi/tgm/bgc-atlas/data/analysis/gcf_membership_new.tab";

//        parse(bgcInfoFile, gcfMembershipFile);

        /**
         * CREATE TABLE bgc (
         *     id INTEGER PRIMARY KEY AUTOINCREMENT,
         *     dataset_id INTEGER NOT NULL,
         *     name VARCHAR(250) NOT NULL,
         *     type VARCHAR(10) NOT NULL,
         *     on_contig_edge BOOLEAN,
         *     length_nt INTEGER NOT NULL,
         *     orig_folder VARCHAR(1500) NOT NULL,
         *     orig_filename VARCHAR(1500) NOT NULL,
         *     UNIQUE(orig_folder, orig_filename, dataset_id),
         *     FOREIGN KEY(dataset_id) REFERENCES dataset(id),
         *     FOREIGN KEY(type) REFERENCES enum_bgc_type(code)
         * );
         */


        parseAndWriteOut2(bgcInfoFile, gcfMembershipFile, protoclustersTable, gcfMembershipTable, newProtoclustersTable);


        /**
         * in psql: \copy regions FROM '/vol/compl_bgcs_bigslice_def_t0.4_dec/regions_new.tab' WITH (FORMAT 'text', DELIMITER E'\t', HEADER false, NULL '');
         *  \copy bigslice_gcf_membership FROM '/vol/compl_bgcs_bigslice_def_t0.4_dec/gcf_membership_new.tab' WITH (FORMAT 'text', DELIMITER E'\t', HEADER false);
         */
    }

    private static void parseAndWriteOut2(String bgcInfoFile, String gcfMembershipFile, String regionsTable, String gcfMembershipTable, String newRegionsTable) {
        try {

            BufferedReader br = new BufferedReader(new FileReader(new File(regionsTable)));
            String line = br.readLine();
            //regionID        assembly        contig_name     contig_len      product_categories      anchor  start   end     contig_edge     type    products        region_num

            Map<Integer, String> regionID2Row = new HashMap<>();
            Map<String, Integer> gbk2RegionID = new HashMap<>();
            Map<Integer, Integer> regionID2BGCID = new HashMap<>();
            Map<Integer, Integer> bgcID2regionID = new HashMap<>();
            Map<Integer, String> regionID2GCFLine = new HashMap<>();


            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                if(lineSplit[10].startsWith("\"")){
                    lineSplit[10] = lineSplit[10].substring(1, lineSplit[10].length() - 1);
                    lineSplit[10] = lineSplit[10].replaceAll("\"", "");
                    lineSplit[10] = lineSplit[10].replaceAll(" ", "");
//                    System.out.println(lineSplit[10]);
                }
                regionID2Row.put(Integer.parseInt(lineSplit[0]), String.join("\t", lineSplit));

                String formattedString3 = String.format("%03d", Integer.parseInt(lineSplit[11]));
//                String gbkFile = "complete-bgcs-20-march" + "/" + lineSplit[1] + "_" + lineSplit[2] + "_region" + formattedString3 + ".gbk";
                String gbkFile = lineSplit[1] + "_" + lineSplit[2] + ".region" + formattedString3 + ".gbk";
//                System.out.println(gbkFile);
                gbk2RegionID.put(gbkFile, Integer.parseInt(lineSplit[0]));

            }
            br.close();
            System.out.println("num regions: " + regionID2Row.size());

            br = new BufferedReader(new FileReader(new File(bgcInfoFile)));
            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split(",");
                Integer regionId = gbk2RegionID.get(lineSplit[7]);
//                System.out.println(regionId + "\t" + lineSplit[0] + "\t" + lineSplit[7]);

                regionID2BGCID.put(regionId, Integer.parseInt(lineSplit[0]));
                bgcID2regionID.put(Integer.parseInt(lineSplit[0]), regionId);
            }

            br.close();
            System.out.println("num bgcs: " + regionID2BGCID.size() + "\t" + bgcID2regionID.size());

            br = new BufferedReader(new FileReader(new File(gcfMembershipFile)));
            BufferedWriter bwR = new BufferedWriter(new FileWriter(new File(newRegionsTable)));
            BufferedWriter bwG = new BufferedWriter(new FileWriter(new File(gcfMembershipTable)));

            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split(",");
                Integer bgcID = Integer.parseInt(lineSplit[1]);
                Double membership = Double.parseDouble(lineSplit[2]);
                double threshold = 0.4;

                Integer regionID = bgcID2regionID.get(bgcID);
                regionID2GCFLine.put(regionID, line);

                bwG.write(lineSplit[0] + "\t" + bgcID + "\t" + regionID + "\t" + membership + "\t" + threshold);
                bwG.newLine();
            }

            bwG.close();

            for (int regionId : regionID2Row.keySet()) {
                String row = regionID2Row.get(regionId);
                bwR.write(row + "\t");
                Integer bgcID = regionID2BGCID.get(regionId);
                if(bgcID != null) {
                    bwR.write(bgcID + "\t" + regionID2GCFLine.get(regionId).split(",")[0]);
                } else {
                    bwR.write("\t");
                }
                bwR.newLine();
            }

            bwR.close();


        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }


    private static void parseAndWriteOut(String bgcInfoFile, String gcfMembershipFile, String protoclustersTable, String gcfMembershipTable, String newProtoclustersTable) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(protoclustersTable)));
            String line = br.readLine();

            Map<String, String> gbk2Row = new HashMap<>();
            Map<String, String> bgcId2gbk = new HashMap<>();
            Map<String, String> gbk2bgcId = new HashMap<>();
            Map<String, String> bgcId2gcf = new HashMap<>();

            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                String[] path = lineSplit[7].split("/");
                gbk2Row.put(path[path.length - 1], line);
            }

            br.close();

            br = new BufferedReader(new FileReader(new File(bgcInfoFile)));
            line = br.readLine();

            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split(",");
                String gbk = lineSplit[7];
                String id = lineSplit[0];
                bgcId2gbk.put(id, gbk);
                gbk2bgcId.put(gbk, id);
            }

            br = new BufferedReader(new FileReader(new File(gcfMembershipFile)));
            line = br.readLine();

            BufferedWriter bwP = new BufferedWriter(new FileWriter(new File(newProtoclustersTable)));

            bwP.write("assembly\tregion\tprotocluster_num\tcategory\tproduct\tcontig_edge\tcontig\tgbk_file\tid\t" +
                    "family_number\tbigslice_bgc_id\tbigslice_gcf_id");
            bwP.newLine();

            BufferedWriter bwG = new BufferedWriter(new FileWriter(new File(gcfMembershipTable)));

            bwG.write("gcf_id\tbgc_id\tmembership_value\tthreshold");
            bwG.newLine();

            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split(",");
                String gcfId = lineSplit[0];
                String bgcId = lineSplit[1];
                String membershipValue = lineSplit[2];

                bgcId2gcf.put(bgcId, gcfId);

                bwG.write(gcfId + "\t" + bgcId + "\t" + membershipValue + "\t" + 0.4);
                bwG.newLine();
            }
            bwG.close();

            System.out.println(bgcId2gbk.size());
            System.out.println(bgcId2gcf.size());
            System.out.println(gbk2Row.size());
            System.out.println(gbk2bgcId.size());

            br = new BufferedReader(new FileReader(new File(protoclustersTable)));
            line = br.readLine();

            while((line = br.readLine()) != null) {
                String[] rowSplit = line.split("\t");
                String[] path = rowSplit[7].split("/");
                String gbk = path[path.length - 1];

                String first10Col = String.join("\t", Arrays.copyOfRange(rowSplit, 0, Math.min(rowSplit.length, 9)));
                bwP.write(first10Col + "\t");

                String bgcId = gbk2bgcId.get(gbk);

                if(bgcId != null){
                    String gcfId = bgcId2gcf.get(bgcId);
                    bwP.write(bgcId + "\t" + gcfId);
                } else {
                    bwP.write("\t");
                }
//                if(rowSplit.length == 13) {
//                    bwP.write(rowSplit[12]);
//                } else {
//                    bwP.write("");
//                }
                bwP.newLine();

            }
            bwP.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void parse(String bgcInfoFile, String gcfMembershipFile) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(bgcInfoFile)));
            String line = br.readLine();

            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split(",");
                database.executeUpdate("UPDATE protoclusters SET bigslice_bgc_id = " + lineSplit[0] +
                        " WHERE gbk_file LIKE '%" + lineSplit[7] + "'");
            }

            br.close();

            br = new BufferedReader(new FileReader(new File(gcfMembershipFile)));
            line = br.readLine();

            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split(",");
                database.insertGCFMembership(lineSplit[0], lineSplit[1], lineSplit[2], 600);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
