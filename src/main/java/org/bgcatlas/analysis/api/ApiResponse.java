package org.bgcatlas.analysis.api;

import java.util.Map;

/**
 * Represents a response from an API call to a public repository.
 * Contains the data returned by the API and metadata about the response.
 */
public class ApiResponse {
    private final int statusCode;
    private final String body;
    private final Map<String, String> headers;
    
    /**
     * Creates a new API response.
     *
     * @param statusCode The HTTP status code of the response
     * @param body The response body as a string
     * @param headers The response headers
     */
    public ApiResponse(int statusCode, String body, Map<String, String> headers) {
        this.statusCode = statusCode;
        this.body = body;
        this.headers = headers;
    }
    
    /**
     * Gets the HTTP status code of the response.
     *
     * @return The status code
     */
    public int getStatusCode() {
        return statusCode;
    }
    
    /**
     * Gets the response body as a string.
     *
     * @return The response body
     */
    public String getBody() {
        return body;
    }
    
    /**
     * Gets the response headers.
     *
     * @return The headers as a map
     */
    public Map<String, String> getHeaders() {
        return headers;
    }
    
    /**
     * Checks if the response indicates a successful API call.
     *
     * @return true if the status code is in the 2xx range, false otherwise
     */
    public boolean isSuccessful() {
        return statusCode >= 200 && statusCode < 300;
    }
}