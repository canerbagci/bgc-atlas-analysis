# BGC Atlas Analysis

A Java-based tool for downloading and analyzing metagenomic assembly datasets from public repositories such as MGnify, in the scope of BGC Atlas.

## Project Overview

This project aims to facilitate the analysis of metagenomic assembly datasets by providing tools to:

1. Crawl public APIs to gather information about available datasets
2. Download and analyze these datasets
3. Populate a database with the analysis results

## Project Structure

The project follows a standard Maven structure:

```
bgc-atlas-analysis/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── org/bgcatlas/analysis/
│   │   │       ├── api/      # API crawler component
│   │   │       ├── dataset/  # Dataset analyzer component
│   │   │       └── db/       # Database component
│   │   └── resources/        # Configuration files and resources
│   └── test/
│       ├── java/             # Test classes
│       └── resources/        # Test resources
└── pom.xml                   # Maven configuration
```

## Components

### API Crawler

Responsible for interacting with public repositories' APIs to discover and fetch metadata about available metagenomic assembly datasets.

### Dataset Analyzer

Handles the downloading, processing, and analysis of metagenomic datasets.

### Database Component

Manages the storage and retrieval of analysis results and metadata in a database.

## Getting Started

### Prerequisites

- Java 11 or higher
- Maven 3.6 or higher

### Building the Project

```bash
mvn clean install
```

### Running the Application

*To be implemented*

## License

*To be determined*