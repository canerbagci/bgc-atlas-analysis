package org.bgcatlas.analysis.api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of ApiClient for the MGnify repository.
 * MGnify is a resource for analysis and archiving of microbial genomics data.
 */
public class MgnifyApiClient implements ApiClient {

    private static final Logger logger = LoggerFactory.getLogger(MgnifyApiClient.class);
    private static final String BASE_URL = "https://www.ebi.ac.uk/metagenomics/api/v1";

    private final HttpClient httpClient;

    /**
     * Creates a new MGnify API client with default HTTP client.
     */
    public MgnifyApiClient() {
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Creates a new MGnify API client with a custom HTTP client.
     *
     * @param httpClient The HTTP client to use for API calls
     */
    public MgnifyApiClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public ApiResponse fetchDatasets(Map<String, String> parameters) throws ApiException {
        // For backward compatibility, we'll just call fetchStudies
        return fetchStudies(parameters);
    }

    @Override
    public ApiResponse fetchStudies(Map<String, String> parameters) throws ApiException {
        try {
            StringBuilder urlBuilder = new StringBuilder(BASE_URL + "/studies");

            if (parameters != null && !parameters.isEmpty()) {
                urlBuilder.append("?");
                boolean first = true;
                for (Map.Entry<String, String> entry : parameters.entrySet()) {
                    if (!first) {
                        urlBuilder.append("&");
                    }
                    urlBuilder.append(entry.getKey()).append("=").append(entry.getValue());
                    first = false;
                }
            }

            HttpGet request = new HttpGet(urlBuilder.toString());
            return executeRequest(request);
        } catch (Exception e) {
            logger.error("Error fetching studies from MGnify", e);
            throw new ApiException("Failed to fetch studies from MGnify", e);
        }
    }

    @Override
    public void fetchAllStudiesAndSave(Map<String, String> parameters, String outputDir) throws ApiException, IOException {
        logger.info("Fetching all studies from MGnify and saving to {}", outputDir);

        // Create the output directory if it doesn't exist
        Path dirPath = Paths.get(outputDir);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            logger.info("Created directory: {}", outputDir);
        }

        String nextUrl = null;
        int pageNumber = 1;
        int maxPages = 2; // For testing purposes, stop at page 2 as requested
        int studyCount = 0;

        do {
            // Fetch the current page
            ApiResponse response;
            if (nextUrl == null) {
                // First page
                response = fetchStudies(parameters);
            } else {
                // Subsequent pages
                HttpGet request = new HttpGet(nextUrl);
                response = executeRequest(request);
            }

            if (!response.isSuccessful()) {
                throw new ApiException("Failed to fetch studies page " + pageNumber, response.getStatusCode());
            }

            // Process each study in the response
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(response.getBody());

                if (json.has("data") && json.get("data").isArray()) {
                    for (JsonNode study : json.get("data")) {
                        studyCount++;

                        // Extract study ID
                        String studyId = study.has("id") ? study.get("id").asText() : "unknown-" + studyCount;
                        logger.info("Processing study: {}", studyId);

                        // Create directory for this study
                        Path studyDir = dirPath.resolve(studyId);
                        if (!Files.exists(studyDir)) {
                            Files.createDirectories(studyDir);
                        }

                        // Save the study as its own JSON file
                        Path studyFilePath = studyDir.resolve("study.json");
                        Files.writeString(studyFilePath, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(study));
                        logger.info("Saved study {} to {}", studyId, studyFilePath);

                        // Process relationships
                        if (study.has("relationships")) {
                            JsonNode relationships = study.get("relationships");

                            // List of relationship types to follow
                            String[] relationshipTypes = {"biomes", "geocoordinates", "samples", "publications", "downloads", "analyses"};

                            for (String relType : relationshipTypes) {
                                if (relationships.has(relType) && 
                                    relationships.get(relType).has("links") && 
                                    relationships.get(relType).get("links").has("related")) {

                                    String relatedUrl = relationships.get(relType).get("links").get("related").asText();
                                    fetchAndSaveRelationship(relatedUrl, studyDir, relType);
                                }
                            }
                        }

                        // Add a small delay to avoid overwhelming the API
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new ApiException("Thread interrupted while processing study", e);
                        }
                    }
                }

                // Extract the next URL from the response
                nextUrl = json.has("links") && json.get("links").has("next") 
                        ? json.get("links").get("next").asText() 
                        : null;
            } catch (Exception e) {
                logger.error("Error processing studies", e);
                throw new ApiException("Failed to process studies", e);
            }

            pageNumber++;

            // Add a small delay to avoid overwhelming the API
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ApiException("Thread interrupted while fetching studies", e);
            }

        } while (nextUrl != null && pageNumber <= maxPages);

        logger.info("Finished fetching studies. Total pages: {}, Total studies: {}", pageNumber - 1, studyCount);
    }

    /**
     * Fetches and saves a relationship resource.
     *
     * @param url The URL of the relationship resource
     * @param studyDir The directory to save the relationship data
     * @param relationType The type of relationship (biomes, samples, etc.)
     * @throws ApiException if an error occurs during the API call
     * @throws IOException if an error occurs while saving the files
     */
    private void fetchAndSaveRelationship(String url, Path studyDir, String relationType) throws ApiException, IOException {
        logger.info("Fetching relationship: {} from {}", relationType, url);

        try {
            HttpGet request = new HttpGet(url);
            ApiResponse response = executeRequest(request);

            if (!response.isSuccessful()) {
                logger.warn("Failed to fetch {} relationship. Status code: {}", relationType, response.getStatusCode());
                return;
            }

            // Save the relationship data
            Path relPath = studyDir.resolve(relationType + ".json");
            Files.writeString(relPath, response.getBody());
            logger.info("Saved {} relationship to {}", relationType, relPath);

            // If this is a samples relationship, also save each sample to its own file
            if ("samples".equals(relationType)) {
                saveSamplesToIndividualFiles(response.getBody(), studyDir);
            }

            // Add a small delay to avoid overwhelming the API
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Thread interrupted while fetching relationship", e);
            }
        } catch (Exception e) {
            logger.error("Error fetching relationship: " + relationType, e);
            // Don't throw the exception, just log it and continue with other relationships
        }
    }

    /**
     * Parses the samples JSON and saves each sample to its own file.
     *
     * @param samplesJson The JSON string containing the samples data
     * @param studyDir The directory of the study
     * @throws IOException if an error occurs while saving the files
     */
    private void saveSamplesToIndividualFiles(String samplesJson, Path studyDir) throws IOException {
        logger.info("Parsing samples and saving each to its own file");

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(samplesJson);

            // Create samples directory if it doesn't exist
            Path samplesDir = studyDir.resolve("samples");
            if (!Files.exists(samplesDir)) {
                Files.createDirectories(samplesDir);
                logger.info("Created samples directory: {}", samplesDir);
            }

            // Check if the JSON has a data array
            if (json.has("data") && json.get("data").isArray()) {
                int sampleCount = 0;

                // Process each sample in the data array
                for (JsonNode sample : json.get("data")) {
                    sampleCount++;

                    // Extract sample ID
                    String sampleId = sample.has("id") ? sample.get("id").asText() : "unknown-" + sampleCount;
                    logger.info("Processing sample: {}", sampleId);

                    // Create directory for this sample
                    Path sampleDir = samplesDir.resolve(sampleId);
                    if (!Files.exists(sampleDir)) {
                        Files.createDirectories(sampleDir);
                        logger.info("Created sample directory: {}", sampleDir);
                    }

                    // Save the sample as its own JSON file
                    Path sampleFilePath = sampleDir.resolve("sample.json");
                    Files.writeString(sampleFilePath, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sample));
                    logger.info("Saved sample {} to {}", sampleId, sampleFilePath);

                    // Check if the sample has runs relationship and fetch them
                    if (sample.has("relationships") && 
                        sample.get("relationships").has("runs") && 
                        sample.get("relationships").get("runs").has("links") && 
                        sample.get("relationships").get("runs").get("links").has("related")) {

                        String runsUrl = sample.get("relationships").get("runs").get("links").get("related").asText();
                        fetchAndSaveRuns(runsUrl, sampleDir);
                    }
                }

                logger.info("Finished saving {} individual samples", sampleCount);
            } else {
                logger.warn("Samples JSON does not have a data array or is not in the expected format");
            }
        } catch (Exception e) {
            logger.error("Error parsing samples JSON and saving individual files", e);
            // Don't throw the exception, just log it
        }
    }

    @Override
    public ApiResponse fetchDatasetDetails(String datasetId) throws ApiException {
        try {
            String url = BASE_URL + "/studies/" + datasetId;
            HttpGet request = new HttpGet(url);
            return executeRequest(request);
        } catch (Exception e) {
            logger.error("Error fetching dataset details for ID: " + datasetId, e);
            throw new ApiException("Failed to fetch dataset details for ID: " + datasetId, e);
        }
    }

    /**
     * Fetches and saves runs for a sample.
     *
     * @param url The URL of the runs resource
     * @param sampleDir The directory of the sample
     * @throws ApiException if an error occurs during the API call
     * @throws IOException if an error occurs while saving the files
     */
    private void fetchAndSaveRuns(String url, Path sampleDir) throws ApiException, IOException {
        logger.info("Fetching runs from {}", url);

        try {
            HttpGet request = new HttpGet(url);
            ApiResponse response = executeRequest(request);

            if (!response.isSuccessful()) {
                logger.warn("Failed to fetch runs. Status code: {}", response.getStatusCode());
                return;
            }

            // Create runs directory if it doesn't exist
            Path runsDir = sampleDir.resolve("runs");
            if (!Files.exists(runsDir)) {
                Files.createDirectories(runsDir);
                logger.info("Created runs directory: {}", runsDir);
            }

            // Save the runs data
            Path runsFilePath = runsDir.resolve("runs.json");
            Files.writeString(runsFilePath, response.getBody());
            logger.info("Saved runs to {}", runsFilePath);

            // Parse the runs JSON and save each run to its own file
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(response.getBody());

                // Check if the JSON has a data array
                if (json.has("data") && json.get("data").isArray()) {
                    int runCount = 0;

                    // Process each run in the data array
                    for (JsonNode run : json.get("data")) {
                        runCount++;

                        // Extract run ID
                        String runId = run.has("id") ? run.get("id").asText() : "unknown-" + runCount;
                        logger.info("Processing run: {}", runId);

                        // Save the run as its own JSON file
                        Path runFilePath = runsDir.resolve(runId + ".json");
                        Files.writeString(runFilePath, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(run));
                        logger.info("Saved run {} to {}", runId, runFilePath);

                        // Check if the run has assemblies relationship and fetch them
                        if (run.has("relationships") && 
                            run.get("relationships").has("assemblies") && 
                            run.get("relationships").get("assemblies").has("links") && 
                            run.get("relationships").get("assemblies").get("links").has("related")) {

                            String assembliesUrl = run.get("relationships").get("assemblies").get("links").get("related").asText();
                            logger.info("Found assemblies link for run {}: {}", runId, assembliesUrl);

                            // Create a directory for this run if it doesn't exist
                            Path runDir = runsDir.resolve(runId);
                            if (!Files.exists(runDir)) {
                                Files.createDirectories(runDir);
                                logger.info("Created run directory: {}", runDir);
                            }

                            // Fetch and save the assemblies data
                            try {
                                HttpGet assemblyRequest = new HttpGet(assembliesUrl);
                                ApiResponse assemblyResponse = executeRequest(assemblyRequest);

                                if (!assemblyResponse.isSuccessful()) {
                                    logger.warn("Failed to fetch assemblies for run {}. Status code: {}", runId, assemblyResponse.getStatusCode());
                                } else {
                                    // Save the assemblies data
                                    Path assembliesFilePath = runDir.resolve("assemblies.json");
                                    Files.writeString(assembliesFilePath, assemblyResponse.getBody());
                                    logger.info("Saved assemblies for run {} to {}", runId, assembliesFilePath);

                                    // Add a small delay to avoid overwhelming the API
                                    try {
                                        Thread.sleep(500);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        logger.warn("Thread interrupted while fetching assemblies", e);
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Error fetching assemblies for run {}", runId, e);
                                // Don't throw the exception, just log it and continue with other runs
                            }
                        }
                    }

                    logger.info("Finished saving {} individual runs", runCount);
                } else {
                    logger.warn("Runs JSON does not have a data array or is not in the expected format");
                }
            } catch (Exception e) {
                logger.error("Error parsing runs JSON and saving individual files", e);
                // Don't throw the exception, just log it
            }

            // Add a small delay to avoid overwhelming the API
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Thread interrupted while fetching runs", e);
            }
        } catch (Exception e) {
            logger.error("Error fetching runs", e);
            // Don't throw the exception, just log it and continue with other samples
        }
    }

    @Override
    public URI getDownloadUrl(String datasetId) throws ApiException {
        try {
            ApiResponse response = fetchDatasetDetails(datasetId);

            if (!response.isSuccessful()) {
                throw new ApiException("Failed to get download URL for dataset: " + datasetId, 
                        response.getStatusCode());
            }

            // In a real implementation, we would parse the JSON response to extract the download URL
            // For this placeholder, we'll just return a dummy URL
            return new URI(BASE_URL + "/studies/" + datasetId + "/downloads");
        } catch (URISyntaxException e) {
            logger.error("Invalid download URL for dataset: " + datasetId, e);
            throw new ApiException("Invalid download URL for dataset: " + datasetId, e);
        }
    }

    /**
     * Executes an HTTP request and converts the response to an ApiResponse.
     *
     * @param request The HTTP request to execute
     * @return The API response
     * @throws IOException if an I/O error occurs during the request
     */
    private ApiResponse executeRequest(HttpGet request) throws IOException {
        HttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        HttpEntity entity = response.getEntity();
        String body = entity != null ? EntityUtils.toString(entity) : "";

        Map<String, String> headers = new HashMap<>();
        for (org.apache.http.Header header : response.getAllHeaders()) {
            headers.put(header.getName(), header.getValue());
        }

        return new ApiResponse(statusCode, body, headers);
    }
}
