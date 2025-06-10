package org.bgcatlas.analysis.api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
            logger.error("Error fetching datasets from MGnify", e);
            throw new ApiException("Failed to fetch datasets from MGnify", e);
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