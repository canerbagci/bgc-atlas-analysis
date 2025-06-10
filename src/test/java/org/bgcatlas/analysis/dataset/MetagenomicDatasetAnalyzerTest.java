package org.bgcatlas.analysis.dataset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the MetagenomicDatasetAnalyzer class.
 */
class MetagenomicDatasetAnalyzerTest {
    
    @TempDir
    Path tempDir;
    
    private MetagenomicDatasetAnalyzer analyzer;
    private Path testDatasetFile;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create a test dataset file
        testDatasetFile = tempDir.resolve("test-dataset.fasta");
        Files.writeString(testDatasetFile, ">Sequence1\nACGTACGT\n>Sequence2\nGCTAGCTA");
        
        // Create the analyzer with a custom temp directory and executor
        analyzer = new MetagenomicDatasetAnalyzer(
                tempDir.resolve("analyzer-temp"),
                Executors.newSingleThreadExecutor()
        );
    }
    
    @Test
    void analyzeDataset_withValidFile_shouldReturnSuccessfulResult() throws Exception {
        // Act
        AnalysisResult result = analyzer.analyzeDataset(testDatasetFile);
        
        // Assert
        assertTrue(result.isSuccessful());
        assertEquals(testDatasetFile.getFileName().toString(), result.getDatasetId());
        assertEquals(testDatasetFile.getFileName().toString(), result.getDatasetName());
        assertNotNull(result.getAnalysisTime());
        
        Map<String, Object> metrics = result.getMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.containsKey("fileSize"));
        assertEquals(Files.size(testDatasetFile), metrics.get("fileSize"));
    }
    
    @Test
    void analyzeDataset_withNonExistentFile_shouldThrowException() {
        // Arrange
        Path nonExistentFile = tempDir.resolve("non-existent-file.fasta");
        
        // Act & Assert
        assertThrows(DatasetAnalysisException.class, () -> {
            analyzer.analyzeDataset(nonExistentFile);
        });
    }
    
    @Test
    void analyzeDatasetAsync_shouldCompleteWithResult() throws Exception {
        // Act
        CompletableFuture<AnalysisResult> future = analyzer.analyzeDatasetAsync(testDatasetFile);
        
        // Assert
        assertNotNull(future);
        AnalysisResult result = future.get(); // Wait for completion
        assertTrue(result.isSuccessful());
    }
    
    @Test
    void analyzeDatasetAsync_withNonExistentFile_shouldCompleteExceptionally() {
        // Arrange
        Path nonExistentFile = tempDir.resolve("non-existent-file.fasta");
        
        // Act
        CompletableFuture<AnalysisResult> future = analyzer.analyzeDatasetAsync(nonExistentFile);
        
        // Assert
        assertNotNull(future);
        assertThrows(ExecutionException.class, () -> {
            future.get(); // Wait for completion
        });
    }
}