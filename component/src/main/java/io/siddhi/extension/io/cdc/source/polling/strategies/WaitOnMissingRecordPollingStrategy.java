/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.cdc.source.polling.strategies;

import com.zaxxer.hikari.HikariDataSource;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.io.cdc.source.metrics.CDCStatus;
import io.siddhi.extension.io.cdc.source.metrics.PollingMetrics;
import io.siddhi.extension.io.cdc.source.polling.CDCPollingModeException;
import io.siddhi.extension.io.cdc.util.CDCPollingUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Polling strategy implementation to wait-on-missing records. If the polling strategy identifies a missed record in
 * the polled chunk, it holds the rest of the processing until the record comes in. This uses {@code pollingColumn},
 * {@code pollingInterval}, {@code missedRecordRetryIntervalMS} and {@code missedRecordWaitingTimeoutMS}.
 */
public class WaitOnMissingRecordPollingStrategy extends PollingStrategy {
    private static final Logger log = Logger.getLogger(WaitOnMissingRecordPollingStrategy.class);

    private String pollingColumn;
    private int pollingInterval;
    private int waitTimeout;
    // The 'wait on missed records' events only work with numeric type. Hence assuming the polling.column is a number.
    private Integer lastReadPollingColumnValue;

    public WaitOnMissingRecordPollingStrategy(HikariDataSource dataSource, ConfigReader configReader,
                                              SourceEventListener sourceEventListener, String tableName,
                                              String pollingColumn, int pollingInterval, int waitTimeout,
                                              String appName, PollingMetrics pollingMetrics,
                                              ExecutorService executorService) {
        super(dataSource, configReader, sourceEventListener, tableName, appName, pollingMetrics, executorService);
        this.pollingColumn = pollingColumn;
        this.pollingInterval = pollingInterval;
        this.waitTimeout = waitTimeout;
    }

    @Override
    public void poll() {
        String selectQuery;
        ResultSetMetaData metadata;
        Map<String, Object> detailsMap;
        Connection connection = getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        int waitingFor = -1;
        long waitingFrom = -1;
        try {
            // If lastReadPollingColumnValue is null, assign it with last record of the table.
            long startedTime = System.currentTimeMillis();
            if (lastReadPollingColumnValue == null) {
                selectQuery = getSelectQuery("MAX(" + pollingColumn + ")", "").trim();
                statement = connection.prepareStatement(selectQuery);
                resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    lastReadPollingColumnValue = resultSet.getInt(1);
                }
                // If the table is empty, set last offset to a negative value.
                if (lastReadPollingColumnValue == null) {
                    lastReadPollingColumnValue = -1;
                }
            }

            selectQuery = getSelectQuery("*", "WHERE " + pollingColumn + " > ?");
            statement = connection.prepareStatement(selectQuery);

            while (true) {
                if (paused) {
                    pauseLock.lock();
                    try {
                        while (paused) {
                            pauseLockCondition.await();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        pauseLock.unlock();
                    }
                }
                int eventsPerPollingInterval = 0;
                boolean isError = false;
                try {
                    statement.setInt(1, lastReadPollingColumnValue);
                    resultSet = statement.executeQuery();
                    metadata = resultSet.getMetaData();
                    while (resultSet.next()) {
                        eventsPerPollingInterval++;
                        boolean isTimedout = false;
                        int currentPollingColumnValue = resultSet.getInt(pollingColumn);
                        if (currentPollingColumnValue - lastReadPollingColumnValue > 1) {
                            if (waitingFor == -1) {
                                // This is the first time to wait for the current record. Hence set the expected record
                                // id and the current timestamp.
                                waitingFor = lastReadPollingColumnValue + 1;
                                waitingFrom = System.currentTimeMillis();
                            }

                            long waitEndTimestamp = waitTimeout > -1 ?
                                    waitingFrom + (waitTimeout * 1000L) : Long.MAX_VALUE;
                            isTimedout = waitEndTimestamp < System.currentTimeMillis();
                            if (!isTimedout) {
                                log.debug("Missed record found at " + waitingFor + " in table " + tableName +
                                        ". Hence pausing the process and " + "retry in " + pollingInterval +
                                        " seconds.");
                                break;
                            }
                        }
                        if (waitingFor > -1) {
                            if (isTimedout) {
                                log.debug("Waiting for missed record " + waitingFor + " in table " + tableName +
                                        " timed-out. Hence resuming the process.");
                            } else {
                                log.debug("Received the missed record " + waitingFor + " in table " + tableName +
                                        ". Hence resuming the process.");
                            }
                            waitingFor = -1;
                            waitingFrom = -1;
                        }
                        detailsMap = new HashMap<>();
                        for (int i = 1; i <= metadata.getColumnCount(); i++) {
                            String key = metadata.getColumnName(i);
                            Object value = resultSet.getObject(key);
                            detailsMap.put(key.toLowerCase(Locale.ENGLISH), value);
                        }
                        lastReadPollingColumnValue = resultSet.getInt(pollingColumn);
                        handleEvent(detailsMap);
                    }
                    if (pollingMetrics != null) {
                        pollingMetrics.setReceiveEventsPerPollingInterval(eventsPerPollingInterval);
                    }
//                    System.out.println("Event Per Polling: " + eventsPerPollingInterval);
                } catch (SQLException e) {
                    isError= true;
                    log.error(buildError("Error occurred while processing records in table %s.", tableName), e);
                } finally {
                    CDCPollingUtil.cleanupConnection(resultSet, null, null);
                }
                try {
                    if (pollingMetrics != null){
                        CDCStatus cdcStatus = isError ? CDCStatus.ERROR : CDCStatus.SUCCESS;
                        pollingMetrics.pollingDetailsMetric(eventsPerPollingInterval, startedTime,
                                System.currentTimeMillis() - startedTime, cdcStatus);
                    }
                    Thread.sleep((long) pollingInterval * 1000);
                } catch (InterruptedException e) {
                    log.error(buildError("Error while polling the table %s.", tableName), e);
                }
            }
        } catch (SQLException ex) {
            throw new CDCPollingModeException(buildError("Error in polling for changes on %s.", tableName), ex);
        } finally {
            CDCPollingUtil.cleanupConnection(resultSet, statement, connection);
        }
    }

    @Override
    public String getLastReadPollingColumnValue() {
        return String.valueOf(lastReadPollingColumnValue);
    }

    @Override
    public void setLastReadPollingColumnValue(String lastReadPollingColumnValue) {
        this.lastReadPollingColumnValue = Integer.parseInt(lastReadPollingColumnValue);
    }
}
