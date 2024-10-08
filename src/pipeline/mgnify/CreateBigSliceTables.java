package pipeline.mgnify;


import java.io.*;
import java.util.*;

public class CreateBigSliceTables {

    /**
     * SQL COMMANDS:
     *
     CREATE TABLE regions_new (
     region_id SERIAL PRIMARY KEY,
     assembly VARCHAR(255),
     contig_name VARCHAR(255),
     contig_len INT,
     product_categories TEXT[],  -- Array of strings
     anchor VARCHAR(255),
     start INT,
     "end" INT,                  -- "end" is a reserved word in PostgreSQL, so it should be quoted
     contig_edge BOOLEAN,
     type VARCHAR(255),
     products TEXT[],            -- Array of strings
     region_num INT,
     bigslice_region_id INT,
     bigslice_gcf_id INT,
     longest_biome VARCHAR(255),
     membership_value DOUBLE PRECISION,  -- New column for membership value
     gcf_from_search BOOLEAN
     );
     *
     *
     \copy regions_new(region_id, assembly, contig_name, contig_len, product_categories, anchor, start, "end", contig_edge, type, products, region_num, bigslice_region_id, bigslice_gcf_id, longest_biome, membership_value, gcf_from_search) FROM '/ceph/ibmi/tgm/bgc-atlas/revision/new-tables/new/regions_out.tab' WITH (FORMAT csv, DELIMITER E'\t', NULL '', HEADER true, FORCE_NULL (membership_value));
     *
     CREATE TABLE bigslice_gcf_new (
     gcf_id INT,
     num_core_regions INT,
     core_products TEXT,
     core_biomes TEXT,
     num_all_regions INT,
     all_products TEXT,
     all_biomes TEXT
     );
     *
     \copy bigslice_gcf_new(gcf_id, num_core_regions, core_products, core_biomes, num_all_regions, all_products, all_biomes) FROM '/ceph/ibmi/tgm/bgc-atlas/revision/new-tables/new/bigslice_gcf_out.tab' WITH (FORMAT csv, DELIMITER E'\t', NULL '', HEADER true);
     *
     *
     CREATE TABLE bigslice_gcf_membership_new (
     gcf_id INT,
     bgc_id INT,
     region_id INT,
     membership_value DOUBLE PRECISION,
     threshold DOUBLE PRECISION,
     gcf_from_search BOOLEAN
     );
     *
     \copy bigslice_gcf_membership_new(gcf_id, bgc_id, region_id, membership_value, threshold, gcf_from_search) FROM '/ceph/ibmi/tgm/bgc-atlas/revision/new-tables/new/bigslice_gcf_membership_out.tab' WITH (FORMAT csv, DELIMITER E'\t', NULL '', HEADER true);
     *
     *
     **/



    static Map<String, Region> filename2reg = new HashMap<>();
    static Map<Integer, Region> atlasRegionId2reg = new HashMap<>();
    static Map<Integer, Region> bigsliceRegionId2reg = new HashMap<>();
    static Map<Integer, GCF> gcfId2gcf = new HashMap<>();

    public static void main(String[] args) {
        String asRegion = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/AS_regions_atlas_table.tab";
        String bigsliceBGCs= "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/bigslice_bgcs_table.tab";
        String bigsliceGCFs = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/bigslice_gcf_membership.tab";
        String bigsliceSearch = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/bigslice_search_results.tab";

        String regionsTable = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/regions_out.tab";
        String bigsliceGcfTable = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/bigslice_gcf_out.tab";
        String bigsliceGcfMembershipTable = "/Users/canerbagci/work/postdoc/global-map/revision/gcf-table/new/bigslice_gcf_membership_out.tab";


        getASRegions(asRegion);
        getBigSliceBGCs(bigsliceBGCs);

        getBigliceGCFs(bigsliceGCFs);
        addIncompleteBGCs(bigsliceSearch);

        writeTables(regionsTable, bigsliceGcfTable, bigsliceGcfMembershipTable);
    }

    private static void writeTables(String regionsTable, String bigsliceGcfTable, String bigsliceGcfMembershipTable) {
        try {
            BufferedWriter bwReg = new BufferedWriter(new FileWriter(new File(regionsTable)));
            BufferedWriter bigsliceGcfBw = new BufferedWriter(new FileWriter(new File(bigsliceGcfTable)));
            BufferedWriter bigsliceGcfMembershipBw = new BufferedWriter(new FileWriter(new File(bigsliceGcfMembershipTable)));

            bwReg.write("region_id\tassembly\tcontig_name\tcontig_len\tproduct_categories\tanchor\tstart\tend\t" +
                    "contig_edge\ttype\tproducts\tregion_num\tbigslice_region_id\tbigslice_gcf_id\tlongest_biome\t" +
                    "membership_value\tgcf_from_search");
            bwReg.newLine();
            for (Region reg : filename2reg.values()) {
                bwReg.write(reg.regionId + "\t" + reg.assembly + "\t" + reg.contigName + "\t" + reg.contigLen + "\t" +
                        reg.productCategories + "\t" + reg.anchor + "\t" + reg.start + "\t" + reg.end + "\t" +
                        reg.contigEdge + "\t" + reg.type + "\t" + reg.products + "\t" + reg.regionNum + "\t" +
                        reg.bigsliceRegionId + "\t" + reg.bigsliceGcfId + "\t" + reg.longestBiome + "\t" +
                        reg.membershipValue + "\t" + reg.gcfFromSearch);
                bwReg.newLine();
            }

            bwReg.close();

            bigsliceGcfBw.write("gcf_id\tnum_core_regions\tcore_products\tcore_biomes\t" +
                    "num_all_regions\tall_products\tall_biomes");
            bigsliceGcfBw.newLine();

            for (GCF gcf : gcfId2gcf.values()) {

                bigsliceGcfBw.write(gcf.gcfId + "\t" + gcf.getCount(true) + "\t" +
                        gcf.getProducts(true) + "\t" + gcf.getBiomes(true) + "\t" + gcf.getCount(false) +
                        "\t" + gcf.getProducts(false) + "\t" + gcf.getBiomes(false));
                bigsliceGcfBw.newLine();
            }

            bigsliceGcfBw.close();

            bigsliceGcfMembershipBw.write("gcf_id\tbgc_id\tregion_id\tmembership_value\tthreshold\tgcf_from_search");
            bigsliceGcfMembershipBw.newLine();

            for(Region r : filename2reg.values()) {
                if(r.bigsliceGcfId == -1) {
                    continue;
                }
                bigsliceGcfMembershipBw.write(r.bigsliceGcfId + "\t" + r.bigsliceRegionId + "\t" + r.regionId + "\t" +
                        r.membershipValue + "\t" + "0.4" + "\t" + r.gcfFromSearch);
                bigsliceGcfMembershipBw.newLine();
            }
            bigsliceGcfMembershipBw.close();

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void addIncompleteBGCs(String bigsliceSearch) {
        try(BufferedReader br = new BufferedReader(new FileReader(new File(bigsliceSearch)))) {
            String line = br.readLine();
            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                int gcfId = Integer.parseInt(lineSplit[0]);
                int bgcId = Integer.parseInt(lineSplit[1]);
                double membershipValue = Double.parseDouble(lineSplit[2]);
                int rank = Integer.parseInt(lineSplit[3]);
                String name = lineSplit[4];
                String type = lineSplit[5];
                int onContigEdge = Integer.parseInt(lineSplit[6]);
                int length = Integer.parseInt(lineSplit[7]);
                String origFolder = lineSplit[8];
                String origFilename = lineSplit[9];

                if (origFilename.endsWith(".gbk")) {
                    origFilename = origFilename.substring(0, origFilename.lastIndexOf(".gbk"));
                }

                if(filename2reg.containsKey(origFilename)) {
                    Region r = filename2reg.get(origFilename);
                    if(r.bigsliceGcfId == -1) {
                        r.bigsliceGcfId = gcfId;
                        r.membershipValue = membershipValue;
                        r.gcfFromSearch = true;
                        gcfId2gcf.get(gcfId).atlasBGCIds.add(r.regionId);
                    }
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void getBigliceGCFs(String bigsliceGCFs) {
        try(BufferedReader br  = new BufferedReader(new FileReader(new File(bigsliceGCFs)))) {
            String line = br.readLine();
            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                int gcfId = Integer.parseInt(lineSplit[0]);
                int bgcId = Integer.parseInt(lineSplit[1]);
                double membershipValue = Double.parseDouble(lineSplit[2]);
                int rank = Integer.parseInt(lineSplit[3]);

                if(gcfId2gcf.containsKey(gcfId)) {
                    GCF gcf = gcfId2gcf.get(gcfId);
                    gcf.bigsliceBGCIds.add(bgcId);
                    gcf.atlasBGCIds.add(bigsliceRegionId2reg.get(bgcId).regionId);
                } else {
                    GCF gcf = new GCF(gcfId);
                    gcf.bigsliceBGCIds.add(bgcId);
                    gcf.atlasBGCIds.add(bigsliceRegionId2reg.get(bgcId).regionId);
                    gcfId2gcf.put(gcfId, gcf);
                }

                bigsliceRegionId2reg.get(bgcId).membershipValue = membershipValue;
                bigsliceRegionId2reg.get(bgcId).bigsliceGcfId = gcfId;
                bigsliceRegionId2reg.get(bgcId).gcfFromSearch = false;

            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }


    private static void getASRegions(String asRegion) {
        try(BufferedReader br = new BufferedReader(new FileReader(new File(asRegion)))) {

            String line = br.readLine();
            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                int regionId = Integer.parseInt(lineSplit[0]);
                String assembly = lineSplit[1];
                String contigName = lineSplit[2];
                int contigLen = Integer.parseInt(lineSplit[3]);
                String productCategories = lineSplit[4];
                String anchor = lineSplit[5];
                int start = Integer.parseInt(lineSplit[6]);
                int end = Integer.parseInt(lineSplit[7]);
                String contigEdge = lineSplit[8];
                String type = lineSplit[9];
                String products = lineSplit[10];
                int regionNum = Integer.parseInt(lineSplit[11]);
                int bigsliceRegionId = -1;
                int bigsliceGcfId = -1;
                String longestBiome = "";
                if(lineSplit.length > 14) {
                    longestBiome = lineSplit[14];
                }
                double membershipValue = -1;

                Region r = new Region(regionId, assembly, contigName, contigLen, productCategories, anchor, start, end,
                        contigEdge, type, products, regionNum, bigsliceRegionId, bigsliceGcfId, longestBiome, membershipValue);

                filename2reg.put(assembly + "_" + contigName + ".region" + String.format("%03d", regionNum), r);
                atlasRegionId2reg.put(regionId, r);
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    private static void getBigSliceBGCs(String bigsliceBGCs) {
        try (BufferedReader br = new BufferedReader(new FileReader(new File(bigsliceBGCs)))) {
            String line = br.readLine();
            while((line = br.readLine()) != null) {
                String[] lineSplit = line.split("\t");

                int bigsliceRegionId = Integer.parseInt(lineSplit[0]);
                int datasetId = Integer.parseInt(lineSplit[1]);
                String name = lineSplit[2];
                String type = lineSplit[3];
                int onContigEdge = Integer.parseInt(lineSplit[4]);
                int length = Integer.parseInt(lineSplit[5]);
                String origFolder = lineSplit[6];
                String origFilename = lineSplit[7];

                if (origFilename.endsWith(".gbk")) {
                    origFilename = origFilename.substring(0, origFilename.lastIndexOf(".gbk"));
                }
                if(filename2reg.containsKey(origFilename)) {
                    Region r = filename2reg.get(origFilename);
                    r.bigsliceRegionId = bigsliceRegionId;
                    bigsliceRegionId2reg.put(bigsliceRegionId, r);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    static class Region {
        int regionId;
        String assembly;
        String contigName;
        int contigLen;
        String productCategories;
        String anchor;
        int start;
        int end;
        String contigEdge;
        String type;
        String products;
        int regionNum;
        int bigsliceRegionId;
        int bigsliceGcfId;
        String longestBiome;
        double membershipValue;
        public boolean gcfFromSearch;

        public Region(int regionId, String assembly, String contigName, int contigLen, String productCategories,
                      String anchor, int start, int end, String contigEdge, String type, String products, int regionNum,
                      int bigsliceRegionId, int bigsliceGcfId, String longestBiome, double membershipValue) {
            this.regionId = regionId;
            this.assembly = assembly;
            this.contigName = contigName;
            this.contigLen = contigLen;
            this.productCategories = productCategories;
            this.anchor = anchor;
            this.start = start;
            this.end = end;
            this.contigEdge = contigEdge;
            this.type = type;
            this.products = products;
            this.regionNum = regionNum;
            this.bigsliceRegionId = bigsliceRegionId;
            this.bigsliceGcfId = bigsliceGcfId;
            this.longestBiome = longestBiome;
            this.membershipValue = membershipValue;
        }
    }

    static class GCF {
        int gcfId;
        Set<Integer> bigsliceBGCIds = new HashSet<>();
        Set<Integer> atlasBGCIds = new HashSet<>();

        public GCF(int gcfId) {
            this.gcfId = gcfId;
        }

        public int getCount(boolean onlyCore) {
            int count = 0;
            for(int regId : atlasBGCIds) {
                Region r = atlasRegionId2reg.get(regId);
                if(onlyCore && r.gcfFromSearch)
                    continue;
                count++;
            }
            return count;
        }

        public String getProducts(boolean onlyCore) {
            Map<String, Integer> productCount = new HashMap<>();
            for (int regId : atlasBGCIds) {
                Region r = atlasRegionId2reg.get(regId);
                if (onlyCore && r.gcfFromSearch)
                    continue;

                String cleanedProducts = r.products.replace("{", "").replace("}", "");
                String[] productSplit = cleanedProducts.split(",");

                for (String s : productSplit) {
                    productCount.putIfAbsent(s, 0);
                    productCount.put(s, productCount.get(s) + 1);
                }
            }

            // Convert the map to a list of entries and sort by value (count) in decreasing order
            List<Map.Entry<String, Integer>> sortedProducts = new ArrayList<>(productCount.entrySet());
            sortedProducts.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue())); // Sort by count in descending order

            StringJoiner sj = new StringJoiner(",");
            for (Map.Entry<String, Integer> entry : sortedProducts) {
                sj.add(entry.getKey() + " (" + entry.getValue() + ")");
            }

            return sj.toString();
        }

        public String getBiomes(boolean onlyCore) {
            Map<String, Integer> biomeCount = new HashMap<>();
            for (int regId : atlasBGCIds) {
                Region r = atlasRegionId2reg.get(regId);
                if (onlyCore && r.gcfFromSearch)
                    continue;

                String cleanedBiome = r.longestBiome.replace("root:", "");

                biomeCount.putIfAbsent(cleanedBiome, 0);
                biomeCount.put(cleanedBiome, biomeCount.get(cleanedBiome) + 1);
            }

            // Convert the map to a list of entries and sort by value (count) in decreasing order
            List<Map.Entry<String, Integer>> sortedBiomes = new ArrayList<>(biomeCount.entrySet());
            sortedBiomes.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue())); // Sort by count in descending order

            StringJoiner sj = new StringJoiner(",");
            for (Map.Entry<String, Integer> entry : sortedBiomes) {
                sj.add(entry.getKey() + " (" + entry.getValue() + ")");
            }

            return sj.toString();
        }

    }

}
