package org.bgcatlas.analysis.api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the MgnifyApiClient class.
 */
class MgnifyApiClientTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse httpResponse;

    @Mock
    private StatusLine statusLine;

    @Mock
    private HttpEntity httpEntity;

    private MgnifyApiClient apiClient;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Set up mock HTTP client
        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));
        when(httpResponse.getAllHeaders()).thenReturn(new org.apache.http.Header[0]);

        apiClient = new MgnifyApiClient(httpClient);
    }

    @Test
    void fetchDatasets_shouldBuildCorrectUrl() throws Exception {
        // Arrange
        Map<String, String> parameters = new HashMap<>();
        parameters.put("page", "1");
        parameters.put("size", "10");

        ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);

        // Act
        apiClient.fetchDatasets(parameters);

        // Assert
        verify(httpClient).execute(requestCaptor.capture());
        HttpGet request = requestCaptor.getValue();

        String url = request.getURI().toString();
        assertTrue(url.startsWith("https://www.ebi.ac.uk/metagenomics/api/v1/studies?"));
        assertTrue(url.contains("page=1"));
        assertTrue(url.contains("size=10"));
    }

    @Test
    void fetchDatasetDetails_shouldBuildCorrectUrl() throws Exception {
        // Arrange
        String datasetId = "MGYS00000001";

        ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);

        // Act
        apiClient.fetchDatasetDetails(datasetId);

        // Assert
        verify(httpClient).execute(requestCaptor.capture());
        HttpGet request = requestCaptor.getValue();

        String expectedUrl = "https://www.ebi.ac.uk/metagenomics/api/v1/studies/MGYS00000001";
        assertEquals(expectedUrl, request.getURI().toString());
    }

    @Test
    void getDownloadUrl_shouldReturnCorrectUri() throws Exception {
        // Arrange
        String datasetId = "MGYS00000001";

        // Act
        URI downloadUrl = apiClient.getDownloadUrl(datasetId);

        // Assert
        String expectedUrl = "https://www.ebi.ac.uk/metagenomics/api/v1/studies/MGYS00000001/downloads";
        assertEquals(expectedUrl, downloadUrl.toString());
    }

    @Test
    void fetchDatasets_shouldHandleErrorResponse() throws Exception {
        // Arrange
        when(statusLine.getStatusCode()).thenReturn(404);

        // Act & Assert
        ApiResponse response = apiClient.fetchDatasets(new HashMap<>());
        assertFalse(response.isSuccessful());
        assertEquals(404, response.getStatusCode());
    }
}
