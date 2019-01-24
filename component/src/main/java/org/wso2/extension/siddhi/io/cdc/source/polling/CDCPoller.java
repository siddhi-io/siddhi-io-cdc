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
import org.wso2.extension.siddhi.io.cdc.source.config.Database;
import org.wso2.extension.siddhi.io.cdc.source.config.QueryConfiguration;
import org.wso2.extension.siddhi.io.cdc.util.CDCPollingUtil;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;
import org.wso2.extension.siddhi.io.cdc.util.MyYamlConstructor;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Polls a given table for changes. Use {@code pollingColumn} to poll on.
 */
public class CDCPoller implements Runnable {

    private static final Logger log = Logger.getLogger(CDCPoller.class);
    private static final String PLACE_HOLDER_TABLE_NAME = "{{TABLE_NAME}}";
    private static final String PLACE_HOLDER_COLUMN_LIST = "{{COLUMN_LIST}}";
    private static final String PLACE_HOLDER_CONDITION = "{{CONDITION}}";
    private static final String SELECT_QUERY_CONFIG_FILE = "query-config.yaml";
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
    private String poolPropertyString;
    private String jndiResource;
    private boolean isLocalDataSource = false;

    public CDCPoller(String url, String username, String password, String tableName, String driverClassName,
                     String datasourceName, String jndiResource,
                     String pollingColumn, int pollingInterval, String poolPropertyString,
                     SourceEventListener sourceEventListener, ConfigReader configReader) {
        this.url = url;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.pollingInterval = pollingInterval;
        this.configReader = configReader;
        this.poolPropertyString = poolPropertyString;
        this.datasourceName = datasourceName;
        this.jndiResource = jndiResource;
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public void setCompletionCallback(CompletionCallback completionCallback) {
        this.completionCallback = completionCallback;
    }

    private void initializeDatasource() throws NamingException {
        if (datasourceName == null) {
            if (jndiResource == null) {
                //init using query parameters
                Properties connectionProperties = new Properties();

                connectionProperties.setProperty("jdbcUrl", url);
                connectionProperties.setProperty("dataSource.user", username);
                if (!CDCPollingUtil.isEmpty(password)) {
                    connectionProperties.setProperty("dataSource.password", password);
                }
                connectionProperties.setProperty("driverClassName", driverClassName);
                if (poolPropertyString != null) {
                    List<String[]> poolProps = CDCPollingUtil.processKeyValuePairs(poolPropertyString);
                    poolProps.forEach(pair -> connectionProperties.setProperty(pair[0], pair[1]));
                }

                HikariConfig config = new HikariConfig(connectionProperties);
                this.dataSource = new HikariDataSource(config);
                isLocalDataSource = true;
                if (log.isDebugEnabled()) {
                    log.debug("Database connection for '" + this.tableName + "' created through connection" +
                            " parameters specified in the query.");
                }
            } else {
                //init using jndi resource name
                this.dataSource = InitialContext.doLookup(jndiResource);
                isLocalDataSource = false;
                if (log.isDebugEnabled()) {
                    log.debug("Lookup for resource '" + jndiResource + "' completed through " +
                            "JNDI lookup.");
                }
            }
        } else {
            //init using jndi datasource name.
            try {
                BundleContext bundleContext = FrameworkUtil.getBundle(DataSourceService.class).getBundleContext();
                ServiceReference serviceRef = bundleContext.getServiceReference(DataSourceService.class.getName());
                if (serviceRef == null) {
                    throw new CDCPollingModeException("DatasourceService : '" +
                            DataSourceService.class.getCanonicalName() + "' cannot be found.");
                } else {
                    DataSourceService dataSourceService = (DataSourceService) bundleContext.getService(serviceRef);
                    this.dataSource = (HikariDataSource) dataSourceService.getDataSource(datasourceName);
                    isLocalDataSource = false;
                    if (log.isDebugEnabled()) {
                        log.debug("Lookup for datasource '" + datasourceName + "' completed through " +
                                "DataSource Service lookup. Current mode: " + CDCSourceConstants.MODE_POLLING);
                    }
                }
            } catch (DataSourceException e) {
                throw new CDCPollingModeException("Datasource '" + datasourceName + "' cannot be connected. " +
                        "Current mode: " + CDCSourceConstants.MODE_POLLING, e);
            }
        }
    }

    public boolean isLocalDataSource() {
        return isLocalDataSource;
    }

    public String getLastReadPollingColumnValue() {
        return lastReadPollingColumnValue;
    }

    public void setLastReadPollingColumnValue(String lastReadPollingColumnValue) {
        this.lastReadPollingColumnValue = lastReadPollingColumnValue;
    }

    private Connection getConnection() {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
            if (log.isDebugEnabled()) {
                log.debug("A connection is initialized ");
            }
        } catch (SQLException e) {
            throw new CDCPollingModeException("Error initializing datasource connection. Current mode: " +
                    CDCSourceConstants.MODE_POLLING, e);
        }
        return conn;
    }

    private String getSelectQuery(String columnList, String condition) {
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
                throw new CDCPollingModeException("Error in looking up database type. Current mode: " +
                        CDCSourceConstants.MODE_POLLING, e);
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
                        throw new CDCPollingModeException(SELECT_QUERY_CONFIG_FILE
                                + " is not found in the classpath. Current mode: " + CDCSourceConstants.MODE_POLLING);
                    }
                    queryConfiguration = (QueryConfiguration) yaml.load(inputStream);
                } finally {
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            log.error("Failed to close the input stream for " + SELECT_QUERY_CONFIG_FILE + ". " +
                                    "Current mode: " + CDCSourceConstants.MODE_POLLING);
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
                throw new CDCPollingModeException("Unsupported database: " + databaseName + ". Configure system" +
                        " parameter: " + databaseName + "." + RECORD_SELECT_QUERY + ". Current mode: " +
                        CDCSourceConstants.MODE_POLLING);
            }
        }

        //create the select query with given constraints
        selectQuery = selectQueryStructure.replace(PLACE_HOLDER_TABLE_NAME, tableName)
                .replace(PLACE_HOLDER_COLUMN_LIST, columnList)
                .replace(PLACE_HOLDER_CONDITION, condition);

        return selectQuery;
    }

    /**
     * Poll for inserts and updates.
     */
    private void pollForChanges() {
        try {
            initializeDatasource();
        } catch (NamingException e) {
            throw new CDCPollingModeException("Error in initializing connection for " + tableName + ". " +
                    "Current mode: " + CDCSourceConstants.MODE_POLLING, e);
        }

        String selectQuery;
        ResultSetMetaData metadata;
        Map<String, Object> detailsMap;
        Connection connection = getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            //If lastReadPollingColumnValue is null, assign it with last record of the table.
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
                try {
                    statement.setString(1, lastReadPollingColumnValue);
                    resultSet = statement.executeQuery();
                    metadata = resultSet.getMetaData();
                    while (resultSet.next()) {
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
                    log.error(ex);
                } finally {
                    CDCPollingUtil.cleanupConnection(resultSet, null, null);
                }
                try {
                    Thread.sleep((long) pollingInterval * 1000);
                } catch (InterruptedException e) {
                    log.error("Error while polling. Current mode: " + CDCSourceConstants.MODE_POLLING, e);
                }
            }
        } catch (SQLException ex) {
            throw new CDCPollingModeException("Error in polling for changes on " + tableName + ". Current mode: " +
                    CDCSourceConstants.MODE_POLLING, ex);
        } finally {
            CDCPollingUtil.cleanupConnection(resultSet, statement, connection);
        }
    }

    private void handleEvent(Map detailsMap) {
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
        } catch (CDCPollingModeException e) {
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
