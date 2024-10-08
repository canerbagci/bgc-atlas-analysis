package data.mgnify;

import jsonutil.JsonUtil;

import javax.json.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class Assembly {
    private JsonObject assemblyData;
    private Sample sample;
    private String id;
    private final Map<String, String> attributes = new HashMap<>();
    private final Map<String, String> analysisSummary = new HashMap<>();
    private String downloadLink;

    public Assembly(JsonObject assemblyData, String outDir) {
        this.assemblyData = assemblyData;
        parse(outDir);
    }

    public Assembly(File file) {
        parse(file);
    }

    public void parse(File file) {
        try {
            FileReader reader = new FileReader(file);
            JsonReader jsonReader = Json.createReader(reader);
            this.assemblyData = jsonReader.readObject();

            final String id = assemblyData.getString("id");
            this.id = id;

            parseAttributes();

            JsonObject relationships = assemblyData.getJsonObject("relationships");
            JsonObject sample = relationships.getJsonObject("sample");
            JsonObject sampleData = sample.getJsonObject("data");
            final String samplesId = sampleData.getString("id");
            JsonObject sampleLinks = sample.getJsonObject("links");
            final String sampleLinksRelated = sampleLinks.getString("related");
            JsonObject downloads = relationships.getJsonObject("downloads");
            JsonObject downloadsLinks = downloads.getJsonObject("links");
            final String downloadsLinksRelated = downloadsLinks.getString("related");
            JsonObject study = relationships.getJsonObject("study");
            JsonObject studyData = study.getJsonObject("data");
            final String studyID = studyData.getString("id");
            JsonObject studyLinks = study.getJsonObject("links");
            final String studyLinksRelated = studyLinks.getString("related");
            JsonObject assembly = relationships.getJsonObject("assembly");
            JsonObject assemblyData1 = assembly.getJsonObject("data");
            final String assemblyId = assemblyData1.getString("id");
            JsonObject assemblyLinks = assembly.getJsonObject("links");
            final String assemblyLinksRelated = assemblyLinks.getString("related");
            JsonObject taxonomy = relationships.getJsonObject("taxonomy");
            JsonObject taxonomyLinks = taxonomy.getJsonObject("links");
            final String taxonomyLinksRelated = taxonomyLinks.getString("related");
            JsonObject antismashGC = relationships.getJsonObject("antismash-gene-clusters");
            JsonObject antismashGCLinks = antismashGC.getJsonObject("links");
            final String antismashGCLinksRelated = antismashGCLinks.getString("related");

            this.downloadLink = downloadsLinksRelated;


            Sample sampleModel = new Sample(samplesId, sampleLinksRelated, new File(file.getParentFile().getParentFile().getAbsolutePath()
                    + File.separator + "samples" + File.separator + samplesId + ".json"));
            this.sample = sampleModel;

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public void parse(String outDir) {
        this.assemblyData = assemblyData;

        final String id = assemblyData.getString("id");
        this.id = id;

        JsonUtil.writeJSON2File(this.assemblyData, outDir + File.separatorChar + "assemblies" + File.separatorChar + id + ".json");

        System.out.println(id);
        parseAttributes();

        JsonObject relationships = assemblyData.getJsonObject("relationships");
        JsonObject sample = relationships.getJsonObject("sample");
        JsonObject sampleData = sample.getJsonObject("data");
        final String samplesId = sampleData.getString("id");
        JsonObject sampleLinks = sample.getJsonObject("links");
        final String sampleLinksRelated = sampleLinks.getString("related");
        JsonObject downloads = relationships.getJsonObject("downloads");
        JsonObject downloadsLinks = downloads.getJsonObject("links");
        final String downloadsLinksRelated = downloadsLinks.getString("related");
        JsonObject study = relationships.getJsonObject("study");
        JsonObject studyData = study.getJsonObject("data");
        final String studyID = studyData.getString("id");
        JsonObject studyLinks = study.getJsonObject("links");
        final String studyLinksRelated = studyLinks.getString("related");
        JsonObject assembly = relationships.getJsonObject("assembly");
        JsonObject assemblyData1 = assembly.getJsonObject("data");
        final String assemblyId = assemblyData1.getString("id");
        JsonObject assemblyLinks = assembly.getJsonObject("links");
        final String assemblyLinksRelated = assemblyLinks.getString("related");
        JsonObject taxonomy = relationships.getJsonObject("taxonomy");
        JsonObject taxonomyLinks = taxonomy.getJsonObject("links");
        final String taxonomyLinksRelated = taxonomyLinks.getString("related");
        JsonObject antismashGC = relationships.getJsonObject("antismash-gene-clusters");
        JsonObject antismashGCLinks = antismashGC.getJsonObject("links");
        final String antismashGCLinksRelated = antismashGCLinks.getString("related");

        this.downloadLink = downloadsLinksRelated;

        Sample sampleModel = new Sample(samplesId, sampleLinksRelated, outDir);
        this.sample = sampleModel;
    }

    private void parseAttributes() {
        JsonObject attributes = assemblyData.getJsonObject("attributes");
        final String pipelineVersion = JsonUtil.getStringOrDefault(attributes, "pipeline-version");
        final String experimentType = JsonUtil.getStringOrDefault(attributes, "experiment-type");
//        final JsonArray analysisSummary = attributes.getJsonArray("analysis-summary");

        JsonArray analysisSummary = null;
        if (attributes.containsKey("analysis-summary") && attributes.get("analysis-summary").getValueType() == JsonValue.ValueType.ARRAY) {
            analysisSummary = attributes.getJsonArray("analysis-summary");
            System.out.println("analysis-summary: " + analysisSummary);
        } else {
            System.out.println("analysis-summary type: " + attributes.get("analysis-summary").getValueType());
        }

        final String analysisStatus = JsonUtil.getStringOrDefault(attributes, "analysis-status");
        final String accession = JsonUtil.getStringOrDefault(attributes, "accession");
        final JsonValue isPrivate = attributes.get("is-private");
        final String completeTime = JsonUtil.getStringOrDefault(attributes, "complete-time");
        final String instrumentPlatform = JsonUtil.getStringOrDefault(attributes, "instrument-platform");
        final String instrumentModel = JsonUtil.getStringOrDefault(attributes, "instrument-model");

        this.attributes.put("pipelineVersion", pipelineVersion);
        this.attributes.put("experimentType", experimentType);
        if(analysisSummary != null) {
            this.attributes.put("analysisSummary", analysisSummary.toString());
        }
        this.attributes.put("analysisStatus", analysisStatus);
        this.attributes.put("accession", accession);
        this.attributes.put("isPrivate", isPrivate.toString());
        this.attributes.put("completeTime", completeTime);
        this.attributes.put("instrumentPlatform", instrumentPlatform);
        this.attributes.put("instrumentModel", instrumentModel);

        if(analysisSummary != null) {
            parseAnalysisSummary(analysisSummary);
        }
    }

    private void parseAnalysisSummary(JsonArray analysisSummaryArr) {
        analysisSummaryArr.forEach(val -> {
                analysisSummary.put(val.asJsonObject().getString("key"), val.asJsonObject().getString("value"));
        });
    }

    public String getId() {
        return id;
    }

    public Sample getSample() {
        return sample;
    }

    public Map<String, String> getAnalysisSummary() {
        return analysisSummary;
    }

    public String getDownloadLink() {
        return downloadLink;
    }

    public JsonObject getAssemblyData() {
        return assemblyData;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
