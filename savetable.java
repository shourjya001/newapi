package com.socgen.riskweb.dao;

import com.socgen.riskweb.Model.InternalRegistrations;
import com.socgen.riskweb.Model.Registration;
import com.socgen.riskweb.Model.ResponseInternal;
import com.socgen.riskweb.Model.SubBookingEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Component
public class DbeClientDaoImpl implements DbeClientDao {

    private static final int BATCH_SIZE = 1000; // Reduced batch size for better performance
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final int LOG_INTERVAL = 50000;
    private static final Logger log = Logger.getLogger(DbeClientDaoImpl.class.getName());

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private AtomicInteger totalInserted = new AtomicInteger(0);

    // Enum for SQL queries (assumed to be defined elsewhere, included here for clarity)
    public enum AppQueries {
        QRY_PRIMARYROLE_TRUNCATE("TRUNCATE TABLE WK_MAESTRO_PRIMROLE_DBE"),
        QRY_SAVE_PRIMARYROLE("INSERT INTO WK_TSMAESTRO (entityId, code, subbookingId) VALUES (?, ?, ?)");

        private final String value;

        AppQueries(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    @Transactional
    public void savePrimaryroleApi(ResponseInternal internalRatingsEventResponse) {
        List<InternalRegistrations> internalRegistrationsList = internalRatingsEventResponse.getInternalRegistrations();

        int totalSize = internalRegistrationsList.size();
        log.info("Total records to process: " + totalSize);

        // Truncate the table before inserting new data
        try {
            this.jdbcTemplate.update(AppQueries.QRY_PRIMARYROLE_TRUNCATE.value(), new Object[]{});
            log.info("Truncated table WK_MAESTRO_PRIMROLE_DBE");
        } catch (Exception e) {
            log.severe("Failed to truncate table WK_MAESTRO_PRIMROLE_DBE: " + e.getMessage());
            throw e;
        }

        if (totalSize > 0) {
            long startTime = System.currentTimeMillis();
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < totalSize; i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, totalSize);
                List<InternalRegistrations> batch = internalRegistrationsList.subList(i, end);
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processBatch(batch), executorService);
                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executorService.shutdown();

            logProgress(totalInserted.get(), startTime);
            log.info("Completed processing. Total inserted: " + totalInserted.get());
        }
    }

    private void processBatch(List<InternalRegistrations> batch) {
        int inserted = executeBatch(batch);
        int newTotal = totalInserted.addAndGet(inserted);
        if (newTotal % LOG_INTERVAL == 0) {
            logProgress(newTotal, System.currentTimeMillis());
        }
    }

    private int executeBatch(List<InternalRegistrations> batch) {
        log.info("Started inserting records in WK_TSMAESTRO");

        // Collect all records to insert
        List<Object[]> batchParams = new ArrayList<>();

        for (InternalRegistrations internalReg : batch) {
            String entityId = internalReg.getEntityId();
            List<Registration> registrations = internalReg.getRegistrations();

            if (registrations != null) {
                for (Registration reg : registrations) {
                    String code = reg.getCode();
                    List<SubBookingEntity> subBookingEntities = reg.getSubBookingEntities();

                    // If subBookingEntities is null or empty, insert a record with null subbookingId
                    if (subBookingEntities == null || subBookingEntities.isEmpty()) {
                        batchParams.add(new Object[]{entityId, code, null});
                    } else {
                        // Insert a record for each subbookingId
                        for (SubBookingEntity subBooking : subBookingEntities) {
                            String subbookingId = subBooking.getSubbookingId();
                            batchParams.add(new Object[]{entityId, code, subbookingId});
                        }
                    }
                }
            }
        }

        // Execute batch update
        int[] updateCounts = jdbcTemplate.batchUpdate(
                AppQueries.QRY_SAVE_PRIMARYROLE.value(),
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        Object[] params = batchParams.get(i);
                        ps.setString(1, (String) params[0]); // entityId
                        ps.setString(2, (String) params[1]); // code
                        if (params[2] == null) {
                            ps.setNull(3, java.sql.Types.VARCHAR); // subbookingId
                        } else {
                            ps.setString(3, (String) params[2]); // subbookingId
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return batchParams.size();
                    }
                }
        );

        return Arrays.stream(updateCounts).sum();
    }

    private void logProgress(int totalInserted, long startTime) {
        long currentTime = System.currentTimeMillis();
        long elapsedSeconds = (currentTime - startTime) / 1000;
        double recordsPerSecond = totalInserted / (double) Math.max(1, elapsedSeconds);
        log.info("Inserted " + totalInserted + " records. Rate: " + String.format("%.2f", recordsPerSecond) + " records/second");
    }
}
