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
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.io.cdc.source.config.Database;
import io.siddhi.extension.io.cdc.source.config.QueryConfiguration;
import io.siddhi.extension.io.cdc.source.metrics.CDCStatus;
import io.siddhi.extension.io.cdc.source.metrics.MetricsUtils;
import io.siddhi.extension.io.cdc.source.metrics.PollingMetrics;
import io.siddhi.extension.io.cdc.source.polling.CDCPollingModeException;
import io.siddhi.extension.io.cdc.util.CDCPollingUtil;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import io.siddhi.extension.io.cdc.util.MyYamlConstructor;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract definition of polling strategy to poll DB changes.
 */
public abstract class PollingStrategy {
    private static final Logger log = Logger.getLogger(PollingStrategy.class);
    private static final String PLACE_HOLDER_TABLE_NAME = "{{TABLE_NAME}}";
    private static final String PLACE_HOLDER_COLUMN_LIST = "{{COLUMN_LIST}}";
    private static final String PLACE_HOLDER_CONDITION = "{{CONDITION}}";
    private static final String SELECT_QUERY_CONFIG_FILE = "query-config.yaml";
    private static final String RECORD_SELECT_QUERY = "recordSelectQuery";

    private HikariDataSource dataSource;
    private String selectQueryStructure = "";
    private ConfigReader configReader;
    private SourceEventListener sourceEventListener;
    private String appName;
    private String streamName;

    protected boolean paused = false;
    protected ReentrantLock pauseLock = new ReentrantLock();
    protected Condition pauseLockCondition = pauseLock.newCondition();
    protected String tableName;
    protected PollingMetrics metrics;

    public PollingStrategy(HikariDataSource dataSource, ConfigReader configReader,
                           SourceEventListener sourceEventListener, String tableName, String appName,
                           PollingMetrics metrics) {
        this.dataSource = dataSource;
        this.configReader = configReader;
        this.sourceEventListener = sourceEventListener;
        this.tableName = tableName;
        this.appName = appName;
        this.streamName = sourceEventListener.getStreamDefinition().getId();
        this.metrics = metrics;
    }

    public abstract void poll();

    public abstract String getLastReadPollingColumnValue();

    public abstract void setLastReadPollingColumnValue(String lastReadPollingColumnValue);

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
        try {
            pauseLock.lock();
            pauseLockCondition.signal();
        } finally {
            pauseLock.unlock();
        }
    }

    protected Connection getConnection() {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
            if (metrics != null) {
                metrics.setHost(MetricsUtils.getShortenJDBCURL(conn.getMetaData().getURL()));
                metrics.setDbType(conn.getMetaData().getDatabaseProductName());
                metrics.setDatabaseName(conn.getCatalog());
                metrics.getTotalReadsMetrics();
                metrics.getEventCountMetric();
                metrics.getValidEventCountMetric();
            }
            log.debug("A connection is initialized.");
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setCDCStatus(CDCStatus.ERROR);
            }
            throw new CDCPollingModeException(buildError("Error initializing datasource connection."), e);
        }
        return conn;
    }

    protected String getSelectQuery(String columnList, String condition) {
        String selectQuery;
        if (selectQueryStructure.isEmpty()) {
            //Get the database product name
            String databaseName;
            Connection conn = null;
            try {
                conn = getConnection();
                DatabaseMetaData dmd = conn.getMetaData();
                databaseName = dmd.getDatabaseProductName();
            } catch (SQLException e) {
                if (metrics != null) {
                    metrics.setCDCStatus(CDCStatus.ERROR);
                }
                throw new CDCPollingModeException(buildError("Error in looking up database type."), e);
            } finally {
                CDCPollingUtil.cleanupConnection(null, null, conn);
            }

            //Read configs from config reader.
            selectQueryStructure = configReader.readConfig(databaseName + "." + RECORD_SELECT_QUERY, "");

            if (selectQueryStructure.isEmpty()) {
                //Read configs from yaml file
                QueryConfiguration queryConfiguration;
                InputStream inputStream = null;
                try {
                    MyYamlConstructor constructor = new MyYamlConstructor(QueryConfiguration.class);
                    TypeDescription queryTypeDescription = new TypeDescription(QueryConfiguration.class);
                    queryTypeDescription.putListPropertyType("databases", Database.class);
                    constructor.addTypeDescription(queryTypeDescription);
                    Yaml yaml = new Yaml(constructor);
                    ClassLoader classLoader = getClass().getClassLoader();
                    inputStream = classLoader.getResourceAsStream(SELECT_QUERY_CONFIG_FILE);
                    if (inputStream == null) {
                        throw new CDCPollingModeException(buildError("%s is not found in the classpath",
                                SELECT_QUERY_CONFIG_FILE));
                    }
                    queryConfiguration = (QueryConfiguration) yaml.load(inputStream);
                } finally {
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            if (metrics != null) {
                                metrics.setCDCStatus(CDCStatus.ERROR);
                            }
                            log.error(buildError("Failed to close the input stream for %s.", SELECT_QUERY_CONFIG_FILE));
                        }
                    }
                }

                //Get database related select query structure
                if (queryConfiguration != null) {
                    for (Database database : queryConfiguration.getDatabases()) {
                        if (database.getName().equalsIgnoreCase(databaseName)) {
                            selectQueryStructure = database.getSelectQuery();
                            break;
                        }
                    }
                }
            }

            if (selectQueryStructure.isEmpty()) {
                if (metrics != null) {
                    metrics.setCDCStatus(CDCStatus.ERROR);
                }
                throw new CDCPollingModeException(buildError("Unsupported database: %s. Configure system " +
                                "parameter: %s.%s.", databaseName, databaseName, RECORD_SELECT_QUERY));
            }
        }
        //create the select query with given constraints
        selectQuery = selectQueryStructure.replace(PLACE_HOLDER_TABLE_NAME, tableName)
                .replace(PLACE_HOLDER_COLUMN_LIST, columnList)
                .replace(PLACE_HOLDER_CONDITION, condition);

        return selectQuery;
    }

    protected void handleEvent(Map detailsMap) {
        long previousEventCount = ((SourceMapper) sourceEventListener).getEventCount();
        sourceEventListener.onEvent(detailsMap, null);
        if (metrics != null) {
            metrics.getTotalReadsMetrics().inc();
            metrics.getEventCountMetric().inc();
            long eventCount = ((SourceMapper) sourceEventListener).getEventCount() - previousEventCount;
            metrics.getValidEventCountMetric().inc(eventCount);
            metrics.setCDCStatus(CDCStatus.CONSUMING);
            metrics.setLastReceivedTime(System.currentTimeMillis());
        }
    }

    protected String buildError(String message, Object... args) {
        return String.format(message, args) + " {mode=" + CDCSourceConstants.MODE_POLLING + ", app=" + appName +
                ", stream=" + streamName + "}";
    }
}
