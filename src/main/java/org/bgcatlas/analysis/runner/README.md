# MGnify Crawler Runner

This is a temporary runner for testing the MGnify crawler functionality. It focuses only on the MGnify API client without requiring dataset analysis or database components.

## Overview

The `MgnifyCrawlerRunner` class provides a simple way to test the MGnify API client functionality. It includes methods to:

1. Fetch and display a list of datasets from MGnify
2. Fetch and display details for a specific dataset
3. Get and display the download URL for a specific dataset

## How to Run

### From an IDE (IntelliJ IDEA, Eclipse, etc.)

1. Open the `MgnifyCrawlerRunner.java` file in your IDE
2. Right-click on the file and select "Run MgnifyCrawlerRunner.main()"
3. To run with a specific dataset ID, add the ID as a program argument in the run configuration

### From the Command Line

After building the project with Maven:

```bash
# Navigate to the project root directory
cd /path/to/bgc-atlas-analysis

# Compile the project
mvn clean compile

# Run the MgnifyCrawlerRunner
java -cp target/classes org.bgcatlas.analysis.runner.MgnifyCrawlerRunner

# Run with a specific dataset ID
java -cp target/classes org.bgcatlas.analysis.runner.MgnifyCrawlerRunner MGYS00000001
```

## Example Usage

The main method in `MgnifyCrawlerRunner` demonstrates how to use the class:

```java
public static void main(String[] args) {
    try {
        MgnifyCrawlerRunner runner = new MgnifyCrawlerRunner();
        
        // Example: Fetch all datasets
        Map<String, String> parameters = new HashMap<>();
        runner.fetchAndDisplayDatasets(parameters);
        
        // Example: Fetch details for a specific dataset if provided
        if (args.length > 0) {
            String datasetId = args[0];
            runner.fetchAndDisplayDatasetDetails(datasetId);
            runner.getAndDisplayDownloadUrl(datasetId);
        } else {
            logger.info("No dataset ID provided. Please provide a dataset ID as a command-line argument to fetch details.");
        }
        
    } catch (Exception e) {
        logger.error("Error running MGnify crawler", e);
        System.exit(1);
    }
}
```

## Customization

You can modify the `MgnifyCrawlerRunner` class to add more functionality or change the existing behavior. For example, you might want to:

- Add methods to fetch and display other types of data from the MGnify API
- Modify the output format to better suit your needs
- Add filtering options for the dataset list

## Next Steps

Once you're satisfied with the MGnify crawler functionality, you can integrate it into the main application by:

1. Updating the `MgnifyApiClient` class with any improvements or fixes
2. Implementing the dataset analysis and database components
3. Integrating the crawler with the rest of the application in `BgcAtlasAnalysisApp`