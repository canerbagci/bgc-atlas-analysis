package org.bgcatlas.analysis;

import org.bgcatlas.analysis.api.ApiClient;
import org.bgcatlas.analysis.api.ApiException;
import org.bgcatlas.analysis.api.ApiResponse;
import org.bgcatlas.analysis.api.MgnifyApiClient;
import org.bgcatlas.analysis.dataset.AnalysisResult;
import org.bgcatlas.analysis.dataset.DatasetAnalysisException;
import org.bgcatlas.analysis.dataset.DatasetAnalyzer;
import org.bgcatlas.analysis.dataset.MetagenomicDatasetAnalyzer;
import org.bgcatlas.analysis.db.DatabaseException;
import org.bgcatlas.analysis.db.DatabaseService;
import org.bgcatlas.analysis.db.HibernateDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main application class for BGC Atlas Analysis.
 * Demonstrates how the components work together to download and analyze metagenomic datasets.
 */
public class BgcAtlasAnalysisApp {
    
    private static final Logger logger = LoggerFactory.getLogger(BgcAtlasAnalysisApp.class);
    
    private final ApiClient apiClient;
    private final DatasetAnalyzer datasetAnalyzer;
    private final DatabaseService databaseService;
    private final ExecutorService executorService;
    
    /**
     * Creates a new BGC Atlas Analysis application with default components.
     *
     * @throws Exception if an error occurs during initialization
     */
    public BgcAtlasAnalysisApp() throws Exception {
        this.apiClient = new MgnifyApiClient();
        this.datasetAnalyzer = new MetagenomicDatasetAnalyzer();
        this.databaseService = new HibernateDatabaseService();
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()
        );
    }
    
    /**
     * Creates a new BGC Atlas Analysis application with custom components.
     *
     * @param apiClient The API client to use
     * @param datasetAnalyzer The dataset analyzer to use
     * @param databaseService The database service to use
     * @param executorService The executor service to use
     */
    public BgcAtlasAnalysisApp(
            ApiClient apiClient,
            DatasetAnalyzer datasetAnalyzer,
            DatabaseService databaseService,
            ExecutorService executorService
    ) {
        this.apiClient = apiClient;
        this.datasetAnalyzer = datasetAnalyzer;
        this.databaseService = databaseService;
        this.executorService = executorService;
    }
    
    /**
     * Runs the analysis pipeline for a specific dataset.
     *
     * @param datasetId The ID of the dataset to analyze
     * @return The ID of the saved analysis result
     * @throws Exception if an error occurs during the pipeline
     */
    public String runPipeline(String datasetId) throws Exception {
        logger.info("Starting pipeline for dataset: {}", datasetId);
        
        // Step 1: Fetch dataset details from the API
        logger.info("Fetching dataset details from API");
        ApiResponse response = apiClient.fetchDatasetDetails(datasetId);
        
        if (!response.isSuccessful()) {
            throw new ApiException("Failed to fetch dataset details: " + response.getStatusCode());
        }
        
        // Step 2: Get the download URL for the dataset
        logger.info("Getting download URL for dataset");
        URI downloadUrl = apiClient.getDownloadUrl(datasetId);
        
        // Step 3: Analyze the dataset
        logger.info("Analyzing dataset from URL: {}", downloadUrl);
        AnalysisResult result = datasetAnalyzer.analyzeDataset(downloadUrl.toString());
        
        // Step 4: Save the analysis result to the database
        logger.info("Saving analysis result to database");
        String resultId = databaseService.saveAnalysisResult(result);
        
        logger.info("Pipeline completed for dataset: {}", datasetId);
        return resultId;
    }
    
    /**
     * Runs the analysis pipeline for multiple datasets asynchronously.
     *
     * @param datasetIds The IDs of the datasets to analyze
     * @return A map of dataset IDs to future result IDs
     */
    public Map<String, CompletableFuture<String>> runPipelineAsync(String... datasetIds) {
        Map<String, CompletableFuture<String>> futures = new HashMap<>();
        
        for (String datasetId : datasetIds) {
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return runPipeline(datasetId);
                } catch (Exception e) {
                    logger.error("Error running pipeline for dataset: {}", datasetId, e);
                    throw new RuntimeException(e);
                }
            }, executorService);
            
            futures.put(datasetId, future);
        }
        
        return futures;
    }
    
    /**
     * Shuts down the application and releases resources.
     *
     * @throws Exception if an error occurs during shutdown
     */
    public void shutdown() throws Exception {
        logger.info("Shutting down BGC Atlas Analysis application");
        
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        databaseService.close();
        
        logger.info("BGC Atlas Analysis application shut down successfully");
    }
    
    /**
     * Main method to run the application.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        try {
            BgcAtlasAnalysisApp app = new BgcAtlasAnalysisApp();
            
            // Example: Run the pipeline for a sample dataset
            if (args.length > 0) {
                String resultId = app.runPipeline(args[0]);
                logger.info("Analysis result saved with ID: {}", resultId);
            } else {
                logger.info("No dataset ID provided. Please provide a dataset ID as a command-line argument.");
            }
            
            app.shutdown();
        } catch (Exception e) {
            logger.error("Error running BGC Atlas Analysis application", e);
            System.exit(1);
        }
    }
}