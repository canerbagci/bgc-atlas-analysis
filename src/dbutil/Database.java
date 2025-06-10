package dbutil;

import data.mgnify.Assembly;
import data.mgnify.Sample;
import pipeline.mgnify.GetBiomeTypes;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Database {

    private String url;
    private String user;
    private String password;

    private final Connection connection;

    public Database() {
        loadConfiguration();
        this.connection = this.connect();
    }

    public Database(boolean connect) {
        loadConfiguration();
        if(!connect) {
            this.connection = null;
            return;
        } else {
            this.connection = this.connect();
        }
    }

    private void loadConfiguration() {
        Properties props = new Properties();
        try(InputStream in = new FileInputStream("db.properties")) {
            props.load(in);
        } catch (IOException ignored) {}

        Map<String, String> env = System.getenv();
        this.url = env.getOrDefault("DB_URL", props.getProperty("db.url", "jdbc:postgresql://localhost:5432/atlas"));
        this.user = env.getOrDefault("DB_USER", props.getProperty("db.user", "user"));
        this.password = env.getOrDefault("DB_PASSWORD", props.getProperty("db.password", "password"));
    }

    private Connection connect() {
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            System.err.println("Connected to the PostgreSQL server successfully!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void insertAssemblies(List<Assembly> assemblyList) {
        String assemblySQL = "INSERT INTO mgnify_asms(assembly, sampleacc, submittedseqs, envmat, longitude, latitude, envbiome, " +
                "collectdate, species, geoloc, biosample, hosttaxid, envfeat, downloadlink) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
                "ON CONFLICT(assembly) DO NOTHING";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try (PreparedStatement pstmt = connection.prepareStatement(assemblySQL, Statement.RETURN_GENERATED_KEYS)) {

            for(Assembly assembly : assemblyList) {
                System.out.println("Inserting " + assembly.getId());
                Sample sample = assembly.getSample();
                Map<String, String> attributes = sample.getAttributes();

                pstmt.setString(1, assembly.getId());
                pstmt.setString(2, sample.getId());
                String submittedNucleotideSequences = assembly.getAnalysisSummary().get("Submitted nucleotide sequences");
                int sns = submittedNucleotideSequences == null || submittedNucleotideSequences.equals("null") ? -1 : Integer.parseInt(submittedNucleotideSequences);
                pstmt.setInt(3, sns);
                pstmt.setString(4, attributes.get("environmentMaterial"));
                pstmt.setDouble(5, Double.parseDouble(attributes.get("longitude")));
                pstmt.setDouble(6, Double.parseDouble(attributes.get("latitude")));
                pstmt.setString(7, attributes.get("environmentBiome") );
                java.util.Date collectionDate = attributes.get("collectionDate").equals("null") || attributes.get("collectionDate").isEmpty() ? null : dateFormat.parse(attributes.get("collectionDate"));
                pstmt.setDate(8, collectionDate == null ? null : new Date(collectionDate.getTime()));
                pstmt.setString(9, attributes.get("species"));
                pstmt.setString(10, attributes.get("geoLocName"));
                pstmt.setString(11, attributes.get("biosample"));
                pstmt.setInt(12, Integer.parseInt(attributes.get("hostTaxID")));
                pstmt.setString(13, attributes.get("environmentFeature"));
                pstmt.setString(14, assembly.getDownloadLink());
                pstmt.addBatch();
            }

            int[] result = pstmt.executeBatch();
            System.out.println("Inserted " + result.length + " assemblies ");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        insertMetadata(assemblyList);

    }

    private void insertMetadata(List<Assembly> assemblyList) {
        String metadataSQL = "INSERT INTO sample_metadata(sample, meta_key, meta_value) VALUES (?,?,?) " +
                "ON CONFLICT(sample, meta_key, meta_value) DO NOTHING";

        try (PreparedStatement pstmt = connection.prepareStatement(metadataSQL, Statement.RETURN_GENERATED_KEYS)) {


            for (Assembly assembly : assemblyList) {
                Sample sample = assembly.getSample();
                Map<String, String> metadata = sample.getMetadata();
                for (String key : metadata.keySet()) {
                    pstmt.setString(1, sample.getId());
                    pstmt.setString(2, key);
                    pstmt.setString(3, metadata.get(key));
                    pstmt.addBatch();
                }
            }

            int[] result = pstmt.executeBatch();
            System.out.println("Inserted " + result.length + " metadata fields");
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String[] getNextInQueueRand() {
        String sql = "SELECT * FROM mgnify_asms WHERE assembly IN (SELECT assembly FROM antismash_runs WHERE status IS NULL) ORDER BY random()LIMIT 1";
//        String sql = "SELECT * FROM mgnify_asms WHERE assembly NOT IN (SELECT assembly FROM antismash_runs WHERE status != 'success') ORDER BY random()LIMIT 1";

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            boolean next = resultSet.next();
            String assemblyId = resultSet.getString(1);
            String link = resultSet.getString(14);
            statement.close();
            return new String[] {assemblyId, link};
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getRunStatus(String assemblyId) {
        String sql = "SELECT status FROM antismash_runs WHERE assembly = '" + assemblyId + "'";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            boolean next = resultSet.next();
            String status = resultSet.getString(1);
            statement.close();
            return status;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void updateRunStatus(String assemblyId, String status) {
        String sql = "UPDATE antismash_runs SET run_timestamp = CURRENT_TIMESTAMP, status = '" + status +
                "' WHERE assembly = '" + assemblyId + "'";
        System.out.println(sql);

        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateDetails(String assemblyId, String asVer, String pipeVer, String server, String resPath, String status) {
        String sql = "UPDATE antismash_runs SET antismash_version = '" + asVer + "', pipeline_version = '" + pipeVer + "', run_server = '"
                + server + "', res_path = '" +  resPath + "', status = '" + status + "', run_timestamp = CURRENT_TIMESTAMP WHERE assembly = '"
                + assemblyId + "'";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertRun(String assemblyId, String ASver, String pipeVer, String server, String path, String status) {
        String sql = "INSERT INTO antismash_runs (assembly, antismash_version, pipeline_version, run_timestamp, " +
                "run_server, res_path, status) VALUES ('" + assemblyId + "', '" + ASver + "', '" + pipeVer +
                "', CURRENT_TIMESTAMP, '" + server + "', '" + path + "', '" + status + "') " +
                "ON CONFLICT(assembly) DO NOTHING";
//        System.out.println(sql);

        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String> getAllAssemblyAccessions() {
        String sql = "SELECT assembly FROM mgnify_asms";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> assemblyAccessions = new ArrayList<>();
            while(resultSet.next()) {
                assemblyAccessions.add(resultSet.getString(1));
            }
            return assemblyAccessions;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> getNewFinishedRuns() {
//        String sql = "SELECT assembly FROM antismash_runs WHERE status = 'success'";
        String sql = "SELECT ar.assembly\n" +
                "FROM antismash_runs AS ar\n" +
                "         LEFT JOIN protoclusters AS pc ON ar.assembly = pc.assembly\n" +
                "WHERE ar.status = 'success' AND pc.assembly IS NULL AND ar.res_path NOT LIKE '/vol/%';";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> assemblyAccessions = new ArrayList<>();
            while(resultSet.next()) {
                assemblyAccessions.add(resultSet.getString(1));
            }
            return assemblyAccessions;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> getAllFinishedRuns() {
                String sql = "SELECT assembly FROM antismash_runs WHERE status = 'success'";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> assemblyAccessions = new ArrayList<>();
            while(resultSet.next()) {
                assemblyAccessions.add(resultSet.getString(1));
            }
            return assemblyAccessions;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void closeConnection() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertPC(String assembly, String contig, String region, int number, String category, String product,
                         String contigEdge, String gbkFile) {
        String sql = "INSERT INTO protoclusters (assembly, contig, region, protocluster_num, " +
                "category, product, contig_edge, gbk_file) VALUES ('" + assembly + "', '" + contig + "', '" + region +
                "', '"  + number + "', '"  + category + "', '" + product + "', '" + contigEdge + "', '" + gbkFile + "')";
//        System.out.println(sql);

        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertClustering(String bgc, String bgcType, int familyNumber, double clusteringThreshold, String assembly) {
        String sql = "INSERT INTO bigscape_clustering (bgc_name, bgc_type, family_number, clustering_threshold, assembly) VALUES ('" +
                bgc + "', '" + bgcType + "', '" + familyNumber + "', '" + clusteringThreshold + "', '" + assembly + "')";
        try{
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            System.out.println(sql);
            e.printStackTrace();
        }
    }
    public void insertNetworkEdge(String clustername1, String clustername2, double rawDistance, double squaredSimilarity,
                                  double jaccardIndex, double dssIndex, double adjacencyIndex, double rawDSSnonAnchor,
                                  double rawDSSAnchor, int nonAnchorDomains, int anchorDoamins, String combinedGroup,
                                  String sharedGroup, String bgcType, double clusteringThreshold) {
        String sql = "INSERT INTO bigscape_networks (clustername1, clustername2, raw_distance, squared_similarity, jaccard_index, " +
                "dss_index, adjacency_index, dss_non_anchor, raw_dss_anchor, non_anchor_domains, anchor_domains, " +
                "combined_group, shared_group, bgc_type, clustering_threshold) VALUES ('" + clustername1 + "', '" +
                clustername2 + "', '" + rawDistance + "', '" + squaredSimilarity + "', '" + jaccardIndex + "', '" +
                dssIndex + "', '" + adjacencyIndex + "', '" + rawDSSnonAnchor + "', '" + rawDSSAnchor + "', '" +
                nonAnchorDomains + "', '" + anchorDoamins + "', '" + combinedGroup + "', '" + sharedGroup + "', '" +
                bgcType + "', '" + clusteringThreshold + "')";
        try{
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            System.err.println(sql);
            e.printStackTrace();
        }
    }

    public List<String> getGCFFamily(String bgcName) {
        List<String> bgcs = new ArrayList<>();
        String sql = "SELECT *\n" +
                "FROM bigscape_clustering\n" +
                "WHERE clustering_threshold = 0.3\n" +
                "  AND family_number = (\n" +
                "    SELECT family_number\n" +
                "    FROM bigscape_clustering\n" +
                "    WHERE bgc_name = '" + bgcName + "'\n" +
                "      AND clustering_threshold = 0.3\n" +
                "    LIMIT 1\n" +
                ");";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()) {
                String bgcNameFam = resultSet.getString(1);
                bgcs.add(bgcNameFam);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return bgcs;
    }

    public String getGCFFamilyName(String bgcName) {
        String sql = "SELECT family_number FROM bigscape_clustering " +
                "WHERE bgc_name = '" + bgcName + "' " +
                "AND clustering_threshold = 0.3;";
        try{
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()) {
                return resultSet.getString(1);
            }
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    public void addBiome(GetBiomeTypes.Biome biome) {
        String sql = "INSERT INTO biomes (id, samplescount, biomename, lineage, sampleslink, genomeslink, childrenlink, studieslink)" +
                "VALUES ('" + biome.getId() + "', '" + biome.getSamplesCount() + "', '" + biome.getBiomeName() + "', '" +  biome.getLineage()
                + "', '" + biome.getSamplesRelated()+ "', '" + biome.getGenomesRelated() + "', '" + biome.getChildrenRelated()
                + "', '" + biome.getStudiesRelated()  + "')" + "ON CONFLICT(id) DO NOTHING";
        System.out.println(sql);
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet executeQuery(String sql) {
        try{
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            return resultSet;
        } catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    public void executeUpdate(String sql) {
        try{
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertGCFMembership(String gcfId, String bgcId, String membershipValue, int threshold) {
        String sql = "INSERT INTO bigslice_gcf_membership (gcf_id, bgc_id, membership_value, threshold) VALUES (" + gcfId + ", " +
                bgcId + ", " + membershipValue + ", " + threshold + ")" + " ON CONFLICT DO NOTHING";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.executeUpdate("UPDATE protoclusters SET bigslice_gcf_id = " + gcfId + " WHERE bigslice_bgc_id = " + bgcId);
    }

//    public void insertRegion(String run, String contigName, int recordId, String products, String contigEdge, String regionNumber, String location) {
//        String sql = "INSERT INTO regions (assembly, contigName, products, contig_edge, contigNumber, regionNumber, location) VALUES (" +
//                run + ", " + "'" + contigName + "'" + ", " + "ARRAY"  + products.replaceAll("\"", "'") + ", " + contigEdge + ", " +
//                recordId + ", " + regionNumber + ", " + location + ")" + " ON CONFLICT DO NOTHING";
//        System.out.println(sql);
//        try {
//            PreparedStatement statement = connection.prepareStatement(sql);
//            int i = statement.executeUpdate();
//            statement.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }

    public void insertRegion(String run, String contigName, int recordId, String products, String contigEdge, String regionNumber, String location) {
        String sql = "INSERT INTO regions (assembly, contig_name, products, contig_edge, contig_number, region_number, location) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, run);
            statement.setString(2, contigName);
            String[] productArray = products
                    .replaceAll("[\\[\\]\"]", "")
                    .split(",");
            statement.setArray(3, connection.createArrayOf("text", productArray));
            statement.setBoolean(4, Boolean.parseBoolean(contigEdge)); // Assuming contig_edge is a boolean
            statement.setInt(5, recordId);
            statement.setInt(6, Integer.parseInt(regionNumber)); // Assuming regionNumber is an integer
            statement.setString(7, location);

            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertRegion(String run, String recordName, int length, String productCategories, String anchor, int start, int end, boolean isContigEdge, String type, String products, int regionNum) {
        String sql = "INSERT INTO regions (assembly, contig_name, contig_len, product_categories, anchor, start, \"end\", contig_edge, type, products, region_num) VALUES (?, ?, ?, ARRAY[?], ?, ?, ?, ?, ?, ARRAY[?], ?) ON CONFLICT DO NOTHING";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, run);
            statement.setString(2, recordName);
            statement.setInt(3, length);

            String[] productArray = productCategories
                    .replaceAll("[\\[\\]\"]", "")
                    .split(",");
            statement.setArray(4, connection.createArrayOf("text", productArray));

            statement.setString(5, anchor);
            statement.setInt(6, start);
            statement.setInt(7, end);
            statement.setBoolean(8, isContigEdge);
            statement.setString(9, type);

            String[] productArray2 = products
                    .replaceAll("[\\[\\]\"]", "")
                    .split(",");
            statement.setArray(10, connection.createArrayOf("text", productArray2));

            statement.setInt(11, regionNum);


            int i = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
