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
import io.siddhi.extension.io.cdc.source.config.CronConfiguration;
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

/**
 * Default implementation of the polling strategy. This uses {@code pollingColumn} and {@code pollingInterval} to poll
 * on.
 */
public class DefaultPollingStrategy extends PollingStrategy {
    private static final Logger log = Logger.getLogger(DefaultPollingStrategy.class);

    private String pollingColumn;
    private int pollingInterval;
    private String lastReadPollingColumnValue;
    private final CronConfiguration cronConfiguration;
    private int eventsPerPollingInterval;

    public DefaultPollingStrategy(HikariDataSource dataSource, ConfigReader configReader,
                                  SourceEventListener sourceEventListener, String tableName, String pollingColumn,
                                  int pollingInterval, String appName, PollingMetrics pollingMetrics,
                                  CronConfiguration cronConfiguration) {
        super(dataSource, configReader, sourceEventListener, tableName, appName, pollingMetrics);
        this.pollingColumn = pollingColumn;
        this.pollingInterval = pollingInterval;
        this.cronConfiguration = cronConfiguration;
    }

    @Override
    public void poll() {
        Connection connection = getConnection();
        try {
            lastReadPollingColumnValue = getLastReadPollingColumnValue(connection);
            if (cronConfiguration.getCronExpression() != null) {
                printEvent(connection);
            } else {
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
                    long startedTime = System.currentTimeMillis();
                    eventsPerPollingInterval = 0;
                    boolean isError = printEvent(connection);
                    try {
                        if (metrics != null) {
                            metrics.setReceiveEventsPerPollingInterval(eventsPerPollingInterval);
                            CDCStatus cdcStatus = isError ? CDCStatus.ERROR : CDCStatus.SUCCESS;
                            metrics.pollingDetailsMetric(eventsPerPollingInterval, startedTime,
                                    System.currentTimeMillis() - startedTime, cdcStatus);
                        }
                        Thread.sleep((long) pollingInterval * 1000);
                    } catch (InterruptedException e) {
                        if (metrics != null) {
                            metrics.setCDCStatus(CDCStatus.ERROR);
                        }
                        log.error(buildError("Error while polling the table %s.", tableName), e);
                    }
                }
            }
        } finally {
            CDCPollingUtil.cleanupConnection(null, null, connection);
        }
    }

    public String getLastReadPollingColumnValue(Connection connection) {
        String selectQuery;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            if (lastReadPollingColumnValue == null) {
                selectQuery = getSelectQuery("MAX(" + pollingColumn + ")", "").trim();
                statement = connection.prepareStatement(selectQuery);
                resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    lastReadPollingColumnValue = resultSet.getString(1);
                }
                //if the table is empty, set last offset to a negative value.
                if (lastReadPollingColumnValue == null) {
                    lastReadPollingColumnValue = "-1";
                }
            }
            return lastReadPollingColumnValue;
        } catch (SQLException ex) {
            throw new CDCPollingModeException(buildError("Error in polling for changes on %s.", tableName), ex);
        } finally {
            CDCPollingUtil.cleanupConnection(resultSet, statement, null);
        }
    }

    public boolean printEvent(Connection connection) {
        ResultSet resultSet = null;
        ResultSetMetaData metadata;
        Map<String, Object> detailsMap;
        String selectQuery;
        PreparedStatement statement = null;
        boolean isError = false;
        try {
            selectQuery = getSelectQuery("*", "WHERE " + pollingColumn + " > ?");
            statement = connection.prepareStatement(selectQuery);
            statement.setString(1, lastReadPollingColumnValue);
            resultSet = statement.executeQuery();
            metadata = resultSet.getMetaData();
            while (resultSet.next()) {
                eventsPerPollingInterval++;
                detailsMap = new HashMap<>();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    String key = metadata.getColumnName(i);
                    Object value = resultSet.getObject(key);
                    detailsMap.put(key.toLowerCase(Locale.ENGLISH), value);
                }
                lastReadPollingColumnValue = resultSet.getString(pollingColumn);
                handleEvent(detailsMap);
            }
        } catch (SQLException ex) {
            if (metrics != null) {
                isError = true;
                metrics.setCDCStatus(CDCStatus.ERROR);
            }
            log.error(buildError("Error occurred while processing records in table %s.", tableName), ex);
        } finally {
            CDCPollingUtil.cleanupConnection(resultSet, statement, null);
            return isError;
        }
    }

    @Override
    public String getLastReadPollingColumnValue() {
        return lastReadPollingColumnValue;
    }

    @Override
    public void setLastReadPollingColumnValue(String lastReadPollingColumnValue) {
        this.lastReadPollingColumnValue = lastReadPollingColumnValue;
    }
}
