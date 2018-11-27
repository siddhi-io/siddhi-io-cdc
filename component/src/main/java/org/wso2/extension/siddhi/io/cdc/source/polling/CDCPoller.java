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

package org.wso2.extension.siddhi.io.cdc.source.polling;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;
import org.wso2.extension.siddhi.io.cdc.source.config.QueryConfiguration;
import org.wso2.extension.siddhi.io.cdc.source.config.QueryConfigurationEntry;
import org.wso2.extension.siddhi.io.cdc.util.CDCPollingUtil;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

/**
 * Polls a given table for changes. Use {@code pollingColumn} to poll on.
 */
public class CDCPoller implements Runnable {

    private static final Logger log = Logger.getLogger(CDCPoller.class);
    private static final String PLACE_HOLDER_TABLE_NAME = "{{TABLE_NAME}}";
    private static final String PLACE_HOLDER_FIELD_LIST = "{{FIELD_LIST}}";
    private static final String PLACE_HOLDER_CONDITION = "{{CONDITION}}";
    private static final String SELECT_QUERY_CONFIG_FILE = "query-config.xml"; // TODO: 11/27/18 move yaml file
    private static final String RECORD_SELECT_QUERY = "recordSelectQuery";
    private String selectQueryStructure = "";
    private String url;
    private String tableName;
    private String username;
    private String password;
    private String driverClassName;
    private HikariDataSource dataSource;
    private String lastReadPollingColumnValue;
    private SourceEventListener sourceEventListener;
    private String pollingColumn;
    private String datasourceName;
    private int pollingInterval;
    private CompletionCallback completionCallback;
    private boolean paused = false;
    private ReentrantLock pauseLock = new ReentrantLock();
    private Condition pauseLockCondition = pauseLock.newCondition();
    private ConfigReader configReader;
    // TODO: 11/27/18 have an optional param, pool.properties
    // TODO: 11/27/18 support jndi also

    public CDCPoller(String url, String username, String password, String tableName, String driverClassName,
                     String lastReadPollingColumnValue, String pollingColumn, int pollingInterval,
                     SourceEventListener sourceEventListener, ConfigReader configReader) {
        this.url = url;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.lastReadPollingColumnValue = lastReadPollingColumnValue;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.pollingInterval = pollingInterval;
        this.configReader = configReader;
    }

    public CDCPoller(String datasourceName, String tableName, String lastReadPollingColumnValue, String pollingColumn,
                     int pollingInterval, SourceEventListener sourceEventListener, ConfigReader configReader) {
        this.datasourceName = datasourceName;
        this.tableName = tableName;
        this.lastReadPollingColumnValue = lastReadPollingColumnValue;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.pollingInterval = pollingInterval;
        this.configReader = configReader;
    }

    public void setCompletionCallback(CompletionCallback completionCallback) {
        this.completionCallback = completionCallback;
    }

    private void initializeDatasource() {
        if (datasourceName == null) {
            Properties connectionProperties = new Properties();

            connectionProperties.setProperty("jdbcUrl", url);
            connectionProperties.setProperty("dataSource.user", username);
            if (!CDCPollingUtil.isEmpty(password)) {
                connectionProperties.setProperty("dataSource.password", password);
            }
            connectionProperties.setProperty("driverClassName", driverClassName);

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

    public String getLastReadPollingColumnValue() {
        return lastReadPollingColumnValue;
    }

    private Connection getConnection() {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
            if (log.isDebugEnabled()) {
                log.debug("A connection is initialized ");
            }
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("Error initializing datasource connection: "
                    + e.getMessage(), e);
        }
        return conn;
    }

    private String getSelectQuery(String fieldList, String condition, ConfigReader configReader) {
        String selectQuery;
// TODO: 11/27/18 use configReader class variable
        if (selectQueryStructure.isEmpty()) {
            //Get the database product name
            String databaseName;
            Connection conn = null;
            try {
                conn = getConnection();
                DatabaseMetaData dmd = conn.getMetaData();
                databaseName = dmd.getDatabaseProductName();
            } catch (SQLException e) {
                throw new SiddhiAppRuntimeException("Error in looking up database type: " + e.getMessage(), e);
            } finally {
                CDCPollingUtil.cleanupConnection(null, null, conn);
            }
// TODO: 11/27/18 give the above val as default val for config reader
            //Read configs from config reader.
            selectQueryStructure = configReader.readConfig(databaseName + "." + RECORD_SELECT_QUERY, "");

            if (selectQueryStructure.isEmpty()) {
                //Read configs from file
                QueryConfiguration queryConfiguration = null;
                InputStream inputStream = null;
                try {
                    JAXBContext ctx = JAXBContext.newInstance(QueryConfiguration.class);
                    Unmarshaller unmarshaller = ctx.createUnmarshaller();
                    ClassLoader classLoader = getClass().getClassLoader();
                    inputStream = classLoader.getResourceAsStream(SELECT_QUERY_CONFIG_FILE);
                    if (inputStream == null) {
                        throw new SiddhiAppRuntimeException(SELECT_QUERY_CONFIG_FILE
                                + " is not found in the classpath");
                    }
                    queryConfiguration = (QueryConfiguration) unmarshaller.unmarshal(inputStream);

                } catch (JAXBException e) {
                    log.error("Query Configuration read error", e);
                } finally {
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            log.error("Failed to close the input stream for " + SELECT_QUERY_CONFIG_FILE);
                        }
                    }
                }

                // TODO: 11/27/18 handle the null below
                //Get database related select query structure
                for (QueryConfigurationEntry entry : queryConfiguration.getDatabases()) {
                    if (entry.getDatabaseName().equalsIgnoreCase(databaseName)) {
                        selectQueryStructure = entry.getRecordSelectQuery();
                        break;
                    }
                }
            }

            if (selectQueryStructure.isEmpty()) {
                throw new SiddhiAppRuntimeException("Unsupported database: " + databaseName + ". Configure system" +
                        " parameter: " + databaseName + "." + RECORD_SELECT_QUERY);
            }
        }

        //create the select query with given constraints
        selectQuery = selectQueryStructure.replace(PLACE_HOLDER_TABLE_NAME, tableName)
                .replace(PLACE_HOLDER_FIELD_LIST, fieldList)
                .replace(PLACE_HOLDER_CONDITION, condition);

        return selectQuery;
    }

    /**
     * Poll for inserts and updates.
     */
    private void pollForChanges() {
        initializeDatasource();

        String selectQuery;
        ResultSetMetaData metadata;
        Map<String, Object> detailsMap;
        Connection connection = getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            // TODO: 11/27/18 check on this block, (2 app runtimes)
            synchronized (new Object()) { //assign null lastReadPollingColumnValue atomically.
                //If lastReadPollingColumnValue is null, assign it with last record of the table.
                if (lastReadPollingColumnValue == null) {
                    selectQuery = getSelectQuery(pollingColumn, "", configReader).trim();
                    statement = connection.prepareStatement(selectQuery);
                    resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        lastReadPollingColumnValue = resultSet.getString(pollingColumn);
                    }
                    // TODO: 11/27/18 take max
                    //if the table is empty, set last offset to a negative value.
                    if (lastReadPollingColumnValue == null) {
                        lastReadPollingColumnValue = "-1";
                    }
                }
            }

            selectQuery = getSelectQuery("*", "WHERE " + pollingColumn + " > ?", configReader);
            statement = connection.prepareStatement(selectQuery);

            while (true) {
                try {
                    statement.setString(1, lastReadPollingColumnValue);
                    resultSet = statement.executeQuery();
                    metadata = resultSet.getMetaData();
                    while (resultSet.next()) {
                        detailsMap = new HashMap<>();
                        for (int i = 1; i <= metadata.getColumnCount(); i++) {
                            String key = metadata.getColumnName(i);
                            String value = resultSet.getString(key);
                            detailsMap.put(key, value);
                        }
                        lastReadPollingColumnValue = resultSet.getString(pollingColumn);
                        handleEvent(detailsMap);
                    }
                } catch (SQLException ex) {
                    log.error(ex);
                }
                try {
                    Thread.sleep(pollingInterval * 1000);
                } catch (InterruptedException e) {
                    log.error("Error while polling.", e);
                }
                // TODO: 11/27/18 consider cleaning resultset
                // TODO: 11/27/18 catch throwables and log inside the while
            }
        } catch (SQLException ex) {
            throw new SiddhiAppRuntimeException("Error in polling for changes on " + tableName, ex);
        } finally {
            CDCPollingUtil.cleanupConnection(resultSet, statement, connection);
        }
    }

    private void handleEvent(Map detailsMap) {
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
        sourceEventListener.onEvent(detailsMap, null);
    }

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

    @Override
    public void run() {
        try {
            pollForChanges();
        } catch (SiddhiAppRuntimeException e) {
            completionCallback.handle(e);
        }
    }

    /**
     * A callback function to be notified when {@code CDCPoller} throws an Error.
     */
    public interface CompletionCallback {
        /**
         * Handle errors from {@link CDCPoller}.
         *
         * @param error the error.
         */
        void handle(Throwable error);
    }
}
