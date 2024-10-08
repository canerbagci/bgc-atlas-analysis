package data.mgnify;

import jsonutil.JsonUtil;

import javax.json.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class Sample {

    private final String link;
    private final String id;
    private JsonObject sampleData;
    private final Map<String, String> attributes = new HashMap<>();
    private final Map<String, String> metadata = new HashMap<>();

    public Sample(String id, String link, String outDir) {
        this.id = id;
        this.link = link;
        parse(outDir);
    }

    public Sample(String id, String link, File file) {
        this.id = id;
        this.link = link;
        parse(file);
    }

    private void parse(File file) {
        FileReader reader = null;
        try {
//            if(!file.exists()) {
//                parse(file.getParentFile().getAbsolutePath());
//                return;
//            }

            reader = new FileReader(file);

            JsonReader jsonReader = Json.createReader(reader);
            this.sampleData = jsonReader.readObject();

            JsonObject data = sampleData.getJsonObject("data");
            JsonObject attributesData = data.getJsonObject("attributes");

            parseAttributes(attributesData);

            JsonArray sampleMetadata = attributesData.getJsonArray("sample-metadata");
            parseMetadata(sampleMetadata);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void parse(String outDir) {
        JsonObject sampleData = APICrawler.getJsonObjectFromURL(link);
        if(sampleData == null) {
            System.err.println("sample data is null for " + id);
            return;
        }
        this.sampleData = sampleData;

        JsonUtil.writeJSON2File(sampleData, outDir + File.separatorChar + "samples" + File.separatorChar + id + ".json");

        JsonObject data = sampleData.getJsonObject("data");
        JsonObject attributesData = data.getJsonObject("attributes");

        parseAttributes(attributesData);

        JsonArray sampleMetadata = attributesData.getJsonArray("sample-metadata");
        parseMetadata(sampleMetadata);

    }

    private void parseMetadata(JsonArray sampleMetadata) {
        for (JsonValue sampleMetadatum : sampleMetadata) {
            try {
                if(sampleMetadatum.asJsonObject().get("value").getValueType().equals(JsonValue.ValueType.NULL))
                    continue;
                metadata.put(sampleMetadatum.asJsonObject().getString("key"), sampleMetadatum.asJsonObject().getString("value"));
            } catch (Exception e) {
                System.out.println(sampleMetadatum.asJsonObject().get("value").getValueType());
                System.out.println(sampleMetadatum.asJsonObject().get("value").getClass());
                System.out.println(sampleMetadatum.asJsonObject().get("value"));
                System.out.println(sampleMetadatum.asJsonObject().get("value") == null);
                System.err.println(this.id + "\t" + sampleMetadatum);
                e.printStackTrace();
            }
        }
    }

    private void parseAttributes(JsonObject attributesData) {
        String environmentMaterial = JsonUtil.getStringOrDefault(attributesData, "environment-material").replaceAll("&quot;", "");
        String environmentFeatures = JsonUtil.getStringOrDefault(attributesData, "environment-feature");
        double latitude = JsonUtil.getDoubleOrDefault(attributesData, "latitude", Double.NaN);
        String accession = JsonUtil.getStringOrDefault(attributesData, "accession");
        String environmentBiome = JsonUtil.getStringOrDefault(attributesData, "environment-biome");
        String sampleDesc = JsonUtil.getStringOrDefault(attributesData, "sample-desc");
        String lastUpdate = JsonUtil.getStringOrDefault(attributesData, "last-update");
        String collectionDate = JsonUtil.getStringOrDefault(attributesData, "collection-date");
        String sampleName = JsonUtil.getStringOrDefault(attributesData, "sample-name");
        String species = JsonUtil.getStringOrDefault(attributesData, "species");
        String analysisCompleted = JsonUtil.getStringOrDefault(attributesData, "analysis-completed");
        String geoLocName = JsonUtil.getStringOrDefault(attributesData, "geo-loc-name");
        String biosample = JsonUtil.getStringOrDefault(attributesData, "biosample");
        int hostTaxID = JsonUtil.getDoubleOrDefault(attributesData, "host-tax-id", Double.NaN).intValue();
        double longitude = JsonUtil.getDoubleOrDefault(attributesData, "longitude", Double.NaN);
        String sampleAlias = JsonUtil.getStringOrDefault(attributesData, "sample-alias");

        attributes.put("environmentMaterial", environmentMaterial);
        attributes.put("environmentFeatures", environmentFeatures);
        attributes.put("latitude", String.valueOf(latitude));
        attributes.put("accession", accession);
        attributes.put("environmentBiome", environmentBiome);
        attributes.put("sampleDesc", sampleDesc);
        attributes.put("lastUpdate", lastUpdate);
        attributes.put("collectionDate", collectionDate);
        attributes.put("sampleName", sampleName);
        attributes.put("species", species);
        attributes.put("analysisCompleted", analysisCompleted);
        attributes.put("geoLocName", geoLocName);
        attributes.put("biosample", biosample);
        attributes.put("hostTaxID", String.valueOf(hostTaxID));
        attributes.put("longitude", String.valueOf(longitude));
        attributes.put("sampleAlias", sampleAlias);
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getLink() {
        return link;
    }

    public JsonObject getSampleData() {
        return sampleData;
    }
}
