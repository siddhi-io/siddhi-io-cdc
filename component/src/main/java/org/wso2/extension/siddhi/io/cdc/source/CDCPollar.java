/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.io.cdc.source;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceUtil;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Polls a given table for changes. Use {@code pollingColumn} to poll on.
 */
public class CDCPollar implements Runnable {

    private static final Logger log = Logger.getLogger(CDCPollar.class);
    private static final String CONNECTION_PROPERTY_JDBC_URL = "jdbcUrl";
    private static final String CONNECTION_PROPERTY_DATASOURCE_USER = "dataSource.user";
    private static final String CONNECTION_PROPERTY_DATASOURCE_PASSWORD = "dataSource.password";
    private static final String CONNECTION_PROPERTY_DRIVER_CLASSNAME = "driverClassName";
    private String url;
    private String tableName;
    private String username;
    private String password;
    private String driverClassName;
    private HikariDataSource dataSource;
    private String lastOffset;
    private SourceEventListener sourceEventListener;
    private String pollingColumn;
    private CDCSource cdcSource;
    private String datasourceName;
    private int pollingInterval;
    private boolean usingDatasourceName;
    private CompletionCallback completionCallback;
    private boolean paused = false;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public CDCPollar(String url, String username, String password, String tableName, String driverClassName,
                     String lastOffset, String pollingColumn, int pollingInterval,
                     SourceEventListener sourceEventListener, CDCSource cdcSource) {
        this.url = url;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.lastOffset = lastOffset;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.cdcSource = cdcSource;
        this.pollingInterval = pollingInterval;
        this.usingDatasourceName = false;
    }

    public CDCPollar(String datasourceName, String tableName, String lastOffset, String pollingColumn,
                     int pollingInterval, SourceEventListener sourceEventListener, CDCSource cdcSource) {
        this.datasourceName = datasourceName;
        this.tableName = tableName;
        this.lastOffset = lastOffset;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.cdcSource = cdcSource;
        this.pollingInterval = pollingInterval;
        this.usingDatasourceName = true;
    }

    public void addCompletionCallback(CompletionCallback completionCallback) {
        this.completionCallback = completionCallback;
    }

    private void initializeDatasource() {
        if (!usingDatasourceName) {
            Properties connectionProperties = new Properties();

            connectionProperties.setProperty(CONNECTION_PROPERTY_JDBC_URL, url);
            connectionProperties.setProperty(CONNECTION_PROPERTY_DATASOURCE_USER, username);
            if (!CDCSourceUtil.isEmpty(password)) {
                connectionProperties.setProperty(CONNECTION_PROPERTY_DATASOURCE_PASSWORD, password);
            }
            connectionProperties.setProperty(CONNECTION_PROPERTY_DRIVER_CLASSNAME, driverClassName);

            HikariConfig config = new HikariConfig(connectionProperties);
            this.dataSource = new HikariDataSource(config);
        } else {
            try {
                BundleContext bundleContext = FrameworkUtil.getBundle(DataSourceService.class).getBundleContext();
                ServiceReference serviceRef = bundleContext.getServiceReference(DataSourceService.class.getName());
                if (serviceRef == null) {
                    throw new SiddhiAppCreationException("DatasourceService : '" +
                            DataSourceService.class.getCanonicalName() + "' cannot be found.");
                } else {
                    DataSourceService dataSourceService = (DataSourceService) bundleContext.getService(serviceRef);
                    this.dataSource = (HikariDataSource) dataSourceService.getDataSource(datasourceName);

                    if (log.isDebugEnabled()) {
                        log.debug("Lookup for datasource '" + datasourceName + "' completed through " +
                                "DataSource Service lookup.");
                    }
                }
            } catch (DataSourceException e) {
                throw new SiddhiAppCreationException("Datasource '" + datasourceName + "' cannot be connected.", e);
            }
        }
    }

    private Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }

    /**
     * Poll for inserts and updates.
     *
     * @param tableName       The table to be monitored.
     * @param lastOffset      The last captured row's timestamp value. If {@code lastOffset} is null,
     *                        the table will be polled from the last existing record. i.e. change data capturing
     *                        could be lost in this case.
     * @param pollingColumn   The column name to poll the table.
     * @param pollingInterval The interval in milliseconds to poll the given table for changes.
     */
    @SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    private void pollForChanges(String tableName, String lastOffset, String pollingColumn, int pollingInterval)
            throws SQLException {

        initializeDatasource();

        String selectQuery;
        Connection connection = getConnection();
        PreparedStatement statement;
        Map<String, Object> detailsMap;
        ResultSet resultSet;
        ResultSetMetaData metadata;

        //If lastOffset is null, assign it with last record of the table.
        if (lastOffset == null) {
            selectQuery = "select " + pollingColumn + " from " + tableName + ";";
            statement = connection.prepareStatement(selectQuery);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                lastOffset = resultSet.getString(pollingColumn);
            }
        }

        selectQuery = "select * from `" + tableName + "` where `" + pollingColumn + "` > ?;";
        statement = connection.prepareStatement(selectQuery);

        while (true) {
            statement.setString(1, lastOffset);
            resultSet = statement.executeQuery();
            metadata = resultSet.getMetaData();
            while (resultSet.next()) {
                detailsMap = new HashMap<>();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    String key = metadata.getColumnName(i);
                    String value = resultSet.getString(key);
                    detailsMap.put(key, value);
                }
                lastOffset = resultSet.getString(pollingColumn);
                cdcSource.setLastOffset(lastOffset);
                handleEvent(detailsMap);
            }

            try {
                Thread.sleep(pollingInterval);
            } catch (InterruptedException e) {
                log.error("Error while polling.", e);
            }
        }
    }

    private void handleEvent(Map detailsMap) {
        if (paused) {
            lock.lock();
            try {
                while (paused) {
                    condition.await();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        sourceEventListener.onEvent(detailsMap, null);
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        try {
            pollForChanges(tableName, lastOffset, pollingColumn, pollingInterval);
        } catch (SQLException e) {
            completionCallback.handle(e);
        }
    }

    public interface CompletionCallback {
        /**
         * Handle the completion of the CDCPollar.
         *
         * @param error the error.
         */
        void handle(Throwable error);
    }
}
