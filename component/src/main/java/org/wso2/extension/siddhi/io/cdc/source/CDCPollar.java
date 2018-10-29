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

    public CDCPollar(String url, String username, String password, String tableName, String driverClassName,
                     String lastOffset, String pollingColumn, SourceEventListener sourceEventListener,
                     CDCSource cdcSource) {
        this.url = url;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.lastOffset = lastOffset;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.cdcSource = cdcSource;
        this.datasourceName = "";
    }

    public CDCPollar(String datasourceName, String tableName, String driverClassName,
                     String lastOffset, String pollingColumn, SourceEventListener sourceEventListener,
                     CDCSource cdcSource) {
        this.datasourceName = datasourceName;
        this.tableName = tableName;
        this.driverClassName = driverClassName;
        this.lastOffset = lastOffset;
        this.sourceEventListener = sourceEventListener;
        this.pollingColumn = pollingColumn;
        this.cdcSource = cdcSource;
    }

    private void initializeDatasource() {
        if (datasourceName.isEmpty()) {
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
            // TODO: 10/29/18 check for the proper exception types, check pom, check for logging msgs
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
     * @param tableName     is the table to be monitored.
     * @param lastOffset    is the last captured row's timestamp value. If @param lastOffset is -1,
     *                      the table will be polled from the last existing record. i.e. change data capturing
     *                      could be lost in this case.
     * @param pollingColumn is the column name to poll the table.
     */
    @SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    private void pollForChanges(String tableName, String lastOffset, String pollingColumn) throws SQLException {

        initializeDatasource();
        Connection connection = getConnection();
        String selectQuery = "select * from `" + tableName + "` where `" + pollingColumn + "` > ?;";
        PreparedStatement statement = connection.prepareStatement(selectQuery);
        Map<String, Object> detailsMap;
        ResultSetMetaData metadata;

        while (true) {
            statement.setString(1, lastOffset);
            ResultSet resultSet = statement.executeQuery();
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
                sourceEventListener.onEvent(detailsMap, null);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }


    @Override
    public void run() {
        if (lastOffset == null) {
            lastOffset = "";
        }
        try {
            pollForChanges(tableName, lastOffset, pollingColumn);
        } catch (SQLException e) {
            log.error("error", e);
        }
        // TODO: 10/25/18 add meaningful error messages
// TODO: 10/26/18 when lastoffset is empty, get the last record's pollingColumn value
// to avoid producing a lot of old data as captured change data.
    }
}
