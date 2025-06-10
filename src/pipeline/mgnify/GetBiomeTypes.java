package pipeline.mgnify;

import data.mgnify.APICrawler;
import dbutil.Database;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GetBiomeTypes {

    static String BASE_URL = "https://www.ebi.ac.uk/metagenomics/api/v1/biomes?format=json&page=";

    static List<Biome> biomes = new ArrayList<>();

    static Database database = new Database();
    public static void main(String[] args) {
        crawlBiomePages();
    }

    private static void crawlBiomePages() {
        JsonObject biomesPage = APICrawler.getJsonObjectFromURL("https://www.ebi.ac.uk/metagenomics/api/v1/biomes?format=json");

        JsonObject meta = biomesPage.getJsonObject("meta");
        JsonObject pagination = meta.getJsonObject("pagination");
        int totalPages = pagination.getInt("pages");
        int totalBiomes = pagination.getInt("count");

        System.out.println("total pages: " + totalPages);
        System.out.println("total biomes: "+ totalBiomes);

        for (int i = 1; i <= totalPages; i++) {
            getPage(i);
        }

        for(Biome biome : biomes) {
            database.addBiome(biome);
            parseStudies(biome);
        }
    }

    private static void parseStudies(Biome biome) {
        String studiesBaseURL = biome.getStudiesRelated();
        System.out.println(studiesBaseURL);

        JsonObject studiesBase = APICrawler.getJsonObjectFromURL(studiesBaseURL);
        JsonObject meta = studiesBase.getJsonObject("meta");
        JsonObject pagination = meta.getJsonObject("pagination");
        int studiesTotalPages = pagination.getInt("pages");

        for (int i = 1; i <= studiesTotalPages; i++) {
            System.out.println("\t" + studiesBaseURL + "&page=" + i);
            JsonObject studiesPage = APICrawler.getJsonObjectFromURL(studiesBaseURL + "&page=" + i);
            JsonArray data = studiesPage.getJsonArray("data");
            for(JsonValue datum : data) {
                JsonObject studyObj = datum.asJsonObject();
                String studyID = studyObj.getString("id");
                JsonObject relationships = studyObj.getJsonObject("relationships");
                JsonObject analyses = relationships.getJsonObject("analyses");
                JsonObject links = analyses.getJsonObject("links");
                String analysesLink = links.getString("related");

                parseAnalysis(analysesLink, biome);
            }
        }
    }

    private static void parseAnalysis(String analysesLink, Biome biome) {
        JsonObject analysesBase = APICrawler.getJsonObjectFromURL(analysesLink);
        JsonObject meta = analysesBase.getJsonObject("meta");
        JsonObject pagination = meta.getJsonObject("pagination");
        int pages = pagination.getInt("pages");

        for (int i = 1; i <= pages; i++) {
            System.out.println("\t\t" + analysesLink + "&page=" + i);
            JsonObject analysesPage = APICrawler.getJsonObjectFromURL(analysesLink + "&page=" + i);
            JsonArray data = analysesPage.getJsonArray("data");
            for(JsonValue datum : data) {
                JsonObject analysisObj = datum.asJsonObject();
                String analysisID = analysisObj.getString("id");
                boolean exists = Boolean.TRUE.equals(
                        database.executeQuery(
                                "SELECT 1 FROM mgnify_asms WHERE assembly = '" + analysisID + "'",
                                rs -> rs.next()));
                if (exists) {
                    database.executeUpdate("INSERT INTO assembly2biome (assembly, biome) VALUES ('" + analysisID + "', '" +
                            biome.getId() + "')");
                }
            }
        }
    }

    private static void getPage(int i) {
        JsonObject page = APICrawler.getJsonObjectFromURL(BASE_URL + i);

        JsonArray data = page.getJsonArray("data");
        for (JsonValue datum : data) {
            JsonObject biomObj = datum.asJsonObject();
            String id = biomObj.getString("id");
            JsonObject attributes = biomObj.getJsonObject("attributes");
            int samplesCount = attributes.getInt("samples-count");
            String biomeName = attributes.getString("biome-name");
            String lineage = attributes.getString("lineage");

            JsonObject relationships = biomObj.getJsonObject("relationships");
            JsonObject samples = relationships.getJsonObject("samples");
            JsonObject links = samples.getJsonObject("links");
            String samplesRelated = links.getString("related");

            JsonObject genomes = relationships.getJsonObject("genomes");
            JsonObject genomesLinks = genomes.getJsonObject("links");
            String genomesRelated = genomesLinks.getString("related");

            JsonObject children = relationships.getJsonObject("children");
            JsonObject childrenLinks = children.getJsonObject("links");
            String childrenRelated = childrenLinks.getString("related");

            JsonObject studies = relationships.getJsonObject("studies");
            JsonObject studiesLinks = studies.getJsonObject("links");
            String studiesRelated = studiesLinks.getString("related");

            Biome biome = new Biome(id, samplesCount, biomeName, lineage, samplesRelated, genomesRelated, childrenRelated, studiesRelated);
            biomes.add(biome);
        }

    }

    public static class Biome {
        String id;
        int samplesCount;
        String biomeName;
        String lineage;

        String samplesRelated;
        String genomesRelated;
        String childrenRelated;
        String studiesRelated;

        public Biome(String id, int samplesCount, String biomeName, String lineage, String samplesRelated, String genomesRelated, String childrenRelated, String studiesRelated) {
            this.id = id;
            this.samplesCount = samplesCount;
            this.biomeName = biomeName;
            this.lineage = lineage;
            this.samplesRelated = samplesRelated;
            this.genomesRelated = genomesRelated;
            this.childrenRelated = childrenRelated;
            this.studiesRelated = studiesRelated;
        }

        public String getId() {
            return id;
        }

        public int getSamplesCount() {
            return samplesCount;
        }

        public String getBiomeName() {
            return biomeName;
        }

        public String getLineage() {
            return lineage;
        }

        public String getSamplesRelated() {
            return samplesRelated;
        }

        public String getGenomesRelated() {
            return genomesRelated;
        }

        public String getChildrenRelated() {
            return childrenRelated;
        }

        public String getStudiesRelated() {
            return studiesRelated;
        }
    }

}
