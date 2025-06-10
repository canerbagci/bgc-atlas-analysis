package org.bgcatlas.analysis.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;

/**
 * Interface for API clients that interact with public repositories
 * to retrieve metagenomic assembly datasets.
 */
public interface ApiClient {
    
    /**
     * Fetches metadata about available datasets from the repository.
     *
     * @param parameters Query parameters to filter the datasets
     * @return A response containing metadata about the datasets
     * @throws ApiException if an error occurs during the API call
     */
    ApiResponse fetchDatasets(Map<String, String> parameters) throws ApiException;
    
    /**
     * Fetches detailed information about a specific dataset.
     *
     * @param datasetId The unique identifier of the dataset
     * @return A response containing detailed information about the dataset
     * @throws ApiException if an error occurs during the API call
     */
    ApiResponse fetchDatasetDetails(String datasetId) throws ApiException;
    
    /**
     * Gets the download URL for a specific dataset.
     *
     * @param datasetId The unique identifier of the dataset
     * @return The URI to download the dataset
     * @throws ApiException if an error occurs during the API call
     */
    URI getDownloadUrl(String datasetId) throws ApiException;
}