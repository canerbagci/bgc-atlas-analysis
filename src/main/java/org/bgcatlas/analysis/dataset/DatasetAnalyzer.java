package org.bgcatlas.analysis.dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for analyzing metagenomic assembly datasets.
 */
public interface DatasetAnalyzer {
    
    /**
     * Analyzes a dataset from a local file.
     *
     * @param datasetPath The path to the dataset file
     * @return The analysis result
     * @throws DatasetAnalysisException if an error occurs during analysis
     */
    AnalysisResult analyzeDataset(Path datasetPath) throws DatasetAnalysisException;
    
    /**
     * Analyzes a dataset from a remote URL.
     *
     * @param datasetUrl The URL of the dataset
     * @return The analysis result
     * @throws DatasetAnalysisException if an error occurs during analysis
     */
    AnalysisResult analyzeDataset(String datasetUrl) throws DatasetAnalysisException;
    
    /**
     * Asynchronously analyzes a dataset from a local file.
     *
     * @param datasetPath The path to the dataset file
     * @return A future that will complete with the analysis result
     */
    CompletableFuture<AnalysisResult> analyzeDatasetAsync(Path datasetPath);
    
    /**
     * Asynchronously analyzes a dataset from a remote URL.
     *
     * @param datasetUrl The URL of the dataset
     * @return A future that will complete with the analysis result
     */
    CompletableFuture<AnalysisResult> analyzeDatasetAsync(String datasetUrl);
}