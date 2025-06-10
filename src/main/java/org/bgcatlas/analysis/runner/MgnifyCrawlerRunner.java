package org.bgcatlas.analysis.runner;

import org.bgcatlas.analysis.api.ApiClient;
import org.bgcatlas.analysis.api.ApiException;
import org.bgcatlas.analysis.api.ApiResponse;
import org.bgcatlas.analysis.api.MgnifyApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Temporary runner for testing the MGnify crawler functionality.
 * This class focuses only on the MGnify API client without requiring
 * dataset analysis or database components.
 */
public class MgnifyCrawlerRunner {

    private static final Logger logger = LoggerFactory.getLogger(MgnifyCrawlerRunner.class);
    private final ApiClient apiClient;

    /**
     * Creates a new MGnify crawler runner with a default API client.
     */
    public MgnifyCrawlerRunner() {
        this.apiClient = new MgnifyApiClient();
    }

    /**
     * Creates a new MGnify crawler runner with a custom API client.
     *
     * @param apiClient The API client to use
     */
    public MgnifyCrawlerRunner(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /**
     * Fetches and displays a list of datasets from MGnify.
     *
     * @param parameters Optional parameters to filter the datasets
     * @throws ApiException if an error occurs during the API call
     */
    public void fetchAndDisplayDatasets(Map<String, String> parameters) throws ApiException {
        logger.info("Fetching datasets from MGnify with parameters: {}", parameters);

        ApiResponse response = apiClient.fetchDatasets(parameters);

        if (response.isSuccessful()) {
            logger.info("Successfully fetched datasets. Response body: {}", response.getBody());
        } else {
            logger.error("Failed to fetch datasets. Status code: {}", response.getStatusCode());
            throw new ApiException("Failed to fetch datasets", response.getStatusCode());
        }
    }

    /**
     * Fetches and displays details for a specific dataset.
     *
     * @param datasetId The ID of the dataset to fetch
     * @throws ApiException if an error occurs during the API call
     */
    public void fetchAndDisplayDatasetDetails(String datasetId) throws ApiException {
        logger.info("Fetching details for dataset: {}", datasetId);

        ApiResponse response = apiClient.fetchDatasetDetails(datasetId);

        if (response.isSuccessful()) {
            logger.info("Successfully fetched dataset details. Response body: {}", response.getBody());
        } else {
            logger.error("Failed to fetch dataset details. Status code: {}", response.getStatusCode());
            throw new ApiException("Failed to fetch dataset details", response.getStatusCode());
        }
    }

    /**
     * Gets and displays the download URL for a specific dataset.
     *
     * @param datasetId The ID of the dataset
     * @throws ApiException if an error occurs during the API call
     */
    public void getAndDisplayDownloadUrl(String datasetId) throws ApiException {
        logger.info("Getting download URL for dataset: {}", datasetId);

        URI downloadUrl = apiClient.getDownloadUrl(datasetId);

        logger.info("Download URL for dataset {}: {}", datasetId, downloadUrl);
    }

    /**
     * Fetches all studies from MGnify and saves them to files.
     * Each study is saved in its own directory with its relationships.
     *
     * @param parameters Optional parameters to filter the studies
     * @param outputDir The directory to save the JSON responses
     * @throws ApiException if an error occurs during the API call
     * @throws IOException if an error occurs while saving the files
     */
    public void fetchAllStudiesAndSave(Map<String, String> parameters, String outputDir) throws ApiException, IOException {
        logger.info("Fetching all studies from MGnify and saving to {}", outputDir);
        apiClient.fetchAllStudiesAndSave(parameters, outputDir);
        logger.info("Finished fetching all studies and saving to {}", outputDir);
    }

    /**
     * Main method to run the MGnify crawler.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        try {
            MgnifyCrawlerRunner runner = new MgnifyCrawlerRunner();

            // Default output directory for studies
            String outputDir = "bgc-atlas-data/mgnify/studies";

            // Example: Fetch and save all studies with their relationships
            Map<String, String> parameters = new HashMap<>();
            logger.info("Starting to fetch and save all studies to {}", outputDir);
            runner.fetchAllStudiesAndSave(parameters, outputDir);
            logger.info("Finished fetching and saving all studies");

            // Example: Fetch details for a specific dataset if provided
            if (args.length > 0) {
                String datasetId = args[0];
                runner.fetchAndDisplayDatasetDetails(datasetId);
                runner.getAndDisplayDownloadUrl(datasetId);
            }

        } catch (Exception e) {
            logger.error("Error running MGnify crawler", e);
            System.exit(1);
        }
    }
}
