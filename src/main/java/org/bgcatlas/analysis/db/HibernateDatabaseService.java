package org.bgcatlas.analysis.db;

import org.bgcatlas.analysis.dataset.AnalysisResult;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Implementation of DatabaseService using Hibernate ORM.
 */
public class HibernateDatabaseService implements DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(HibernateDatabaseService.class);
    private final SessionFactory sessionFactory;

    /**
     * Creates a new Hibernate database service with default configuration.
     *
     * @throws DatabaseException if an error occurs during initialization
     */
    public HibernateDatabaseService() throws DatabaseException {
        try {
            // In a real implementation, this would load the Hibernate configuration from hibernate.cfg.xml
            // For this placeholder, we'll just create a minimal configuration
            Configuration configuration = new Configuration();
            configuration.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
            configuration.setProperty("hibernate.connection.url", "jdbc:h2:mem:bgcatlas;DB_CLOSE_DELAY=-1");
            configuration.setProperty("hibernate.connection.username", "sa");
            configuration.setProperty("hibernate.connection.password", "");
            configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
            configuration.setProperty("hibernate.show_sql", "true");
            configuration.setProperty("hibernate.hbm2ddl.auto", "create-drop");

            // Add entity mappings
            configuration.addAnnotatedClass(AnalysisResultEntity.class);

            this.sessionFactory = configuration.buildSessionFactory();
            logger.info("Hibernate database service initialized");
        } catch (Exception e) {
            logger.error("Failed to initialize Hibernate database service", e);
            throw new DatabaseException("Failed to initialize Hibernate database service", e);
        }
    }

    /**
     * Creates a new Hibernate database service with a custom session factory.
     *
     * @param sessionFactory The Hibernate session factory to use
     */
    public HibernateDatabaseService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        logger.info("Hibernate database service initialized with custom session factory");
    }

    @Override
    public String saveAnalysisResult(AnalysisResult result) throws DatabaseException {
        logger.info("Saving analysis result for dataset: {}", result.getDatasetId());

        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.beginTransaction();

            try {
                // Convert AnalysisResult to AnalysisResultEntity
                AnalysisResultEntity entity = new AnalysisResultEntity();
                entity.setId(UUID.randomUUID().toString());
                entity.setDatasetId(result.getDatasetId());
                entity.setDatasetName(result.getDatasetName());
                entity.setAnalysisTime(result.getAnalysisTime());
                entity.setSuccessful(result.isSuccessful());
                entity.setErrorMessage(result.getErrorMessage());
                entity.setMetricsJson(convertMetricsToJson(result.getMetrics()));

                session.save(entity);
                transaction.commit();

                logger.info("Analysis result saved with ID: {}", entity.getId());
                return entity.getId();
            } catch (Exception e) {
                transaction.rollback();
                logger.error("Error saving analysis result", e);
                throw new DatabaseException("Failed to save analysis result", e);
            }
        }
    }

    @Override
    public Optional<AnalysisResult> getAnalysisResult(String resultId) throws DatabaseException {
        logger.info("Retrieving analysis result with ID: {}", resultId);

        try (Session session = sessionFactory.openSession()) {
            AnalysisResultEntity entity = session.get(AnalysisResultEntity.class, resultId);

            if (entity == null) {
                logger.info("Analysis result not found with ID: {}", resultId);
                return Optional.empty();
            }

            // Convert AnalysisResultEntity to AnalysisResult
            AnalysisResult result = convertEntityToAnalysisResult(entity);
            return Optional.of(result);
        } catch (Exception e) {
            logger.error("Error retrieving analysis result with ID: {}", resultId, e);
            throw new DatabaseException("Failed to retrieve analysis result", e);
        }
    }

    @Override
    public List<AnalysisResult> getAnalysisResultsByDataset(String datasetId) throws DatabaseException {
        logger.info("Retrieving analysis results for dataset: {}", datasetId);

        try (Session session = sessionFactory.openSession()) {
            Query<AnalysisResultEntity> query = session.createQuery(
                    "FROM AnalysisResultEntity WHERE datasetId = :datasetId",
                    AnalysisResultEntity.class);
            query.setParameter("datasetId", datasetId);

            List<AnalysisResultEntity> entities = query.list();

            if (entities.isEmpty()) {
                logger.info("No analysis results found for dataset: {}", datasetId);
                return Collections.emptyList();
            }

            // Convert entities to AnalysisResult objects
            return entities.stream()
                    .map(this::convertEntityToAnalysisResult)
                    .toList();
        } catch (Exception e) {
            logger.error("Error retrieving analysis results for dataset: {}", datasetId, e);
            throw new DatabaseException("Failed to retrieve analysis results for dataset", e);
        }
    }

    @Override
    public boolean deleteAnalysisResult(String resultId) throws DatabaseException {
        logger.info("Deleting analysis result with ID: {}", resultId);

        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.beginTransaction();

            try {
                AnalysisResultEntity entity = session.get(AnalysisResultEntity.class, resultId);

                if (entity == null) {
                    logger.info("Analysis result not found with ID: {}", resultId);
                    transaction.commit();
                    return false;
                }

                session.delete(entity);
                transaction.commit();

                logger.info("Analysis result deleted with ID: {}", resultId);
                return true;
            } catch (Exception e) {
                transaction.rollback();
                logger.error("Error deleting analysis result with ID: {}", resultId, e);
                throw new DatabaseException("Failed to delete analysis result", e);
            }
        }
    }

    @Override
    public boolean isDatabaseAvailable() {
        try (Session session = sessionFactory.openSession()) {
            session.createNativeQuery("SELECT 1").uniqueResult();
            return true;
        } catch (Exception e) {
            logger.warn("Database is not available", e);
            return false;
        }
    }

    @Override
    public void close() throws DatabaseException {
        try {
            if (sessionFactory != null && !sessionFactory.isClosed()) {
                sessionFactory.close();
                logger.info("Hibernate database service closed");
            }
        } catch (Exception e) {
            logger.error("Error closing Hibernate database service", e);
            throw new DatabaseException("Failed to close database service", e);
        }
    }

    /**
     * Converts a map of metrics to a JSON string.
     * In a real implementation, this would use a JSON library like Jackson.
     *
     * @param metrics The metrics to convert
     * @return A JSON string representation of the metrics
     */
    private String convertMetricsToJson(Map<String, Object> metrics) {
        // This is a placeholder implementation
        return metrics.toString();
    }

    /**
     * Converts an AnalysisResultEntity to an AnalysisResult.
     *
     * @param entity The entity to convert
     * @return The converted AnalysisResult
     */
    private AnalysisResult convertEntityToAnalysisResult(AnalysisResultEntity entity) {
        // This is a placeholder implementation
        // In a real implementation, this would parse the JSON metrics
        Map<String, Object> metrics = Collections.emptyMap();

        if (entity.isSuccessful()) {
            return new AnalysisResult(entity.getDatasetId(), entity.getDatasetName(), metrics);
        } else {
            return new AnalysisResult(entity.getDatasetId(), entity.getDatasetName(), entity.getErrorMessage());
        }
    }
}
