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

package io.siddhi.extension.io.cdc.source.polling;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.io.cdc.source.metrics.PollingMetrics;
import io.siddhi.extension.io.cdc.source.polling.strategies.DefaultPollingStrategy;
import io.siddhi.extension.io.cdc.source.polling.strategies.PollingStrategy;
import io.siddhi.extension.io.cdc.source.polling.strategies.WaitOnMissingRecordPollingStrategy;
import io.siddhi.extension.io.cdc.util.CDCPollingUtil;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.log4j.Logger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;

import java.util.List;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Polls a given table for changes. Use {@code pollingColumn} to poll on.
 */
public class CDCPoller implements Runnable {

    private static final Logger log = Logger.getLogger(CDCPoller.class);
    private String url;
    private String tableName;
    private String username;
    private String password;
    private String driverClassName;
    private HikariDataSource dataSource;
    private String datasourceName;
    private CompletionCallback completionCallback;
    private String poolPropertyString;
    private String jndiResource;
    private boolean isLocalDataSource = false;
    private String appName;
    private String streamName;

    private PollingStrategy pollingStrategy;

    public CDCPoller(String url, String username, String password, String tableName, String driverClassName,
                     String datasourceName, String jndiResource,
                     String pollingColumn, int pollingInterval, String poolPropertyString,
                     SourceEventListener sourceEventListener, ConfigReader configReader, boolean waitOnMissedRecord,
                     int missedRecordWaitingTimeout, String appName, PollingMetrics pollingMetrics) {
        this.url = url;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.poolPropertyString = poolPropertyString;
        this.datasourceName = datasourceName;
        this.jndiResource = jndiResource;
        this.appName = appName;
        this.streamName = sourceEventListener.getStreamDefinition().getId();

        try {
            initializeDatasource();
        } catch (NamingException e) {
            throw new CDCPollingModeException("Error in initializing connection for " + tableName + ". {mode=" +
                    CDCSourceConstants.MODE_POLLING + ", app=" + appName + ", stream=" + streamName + "}", e);
        }

        if (waitOnMissedRecord) {
            log.debug(WaitOnMissingRecordPollingStrategy.class + " is selected as the polling strategy.");
            this.pollingStrategy = new WaitOnMissingRecordPollingStrategy(dataSource, configReader, sourceEventListener,
                    tableName, pollingColumn, pollingInterval, missedRecordWaitingTimeout, appName, pollingMetrics);
        } else {
            log.debug(DefaultPollingStrategy.class + " is selected as the polling strategy.");
            this.pollingStrategy = new DefaultPollingStrategy(dataSource, configReader, sourceEventListener,
                    tableName, pollingColumn, pollingInterval, appName, pollingMetrics);
        }
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
        return pollingStrategy.getLastReadPollingColumnValue();
    }

    public void setLastReadPollingColumnValue(String lastReadPollingColumnValue) {
        pollingStrategy.setLastReadPollingColumnValue(lastReadPollingColumnValue);
    }

    public void pause() {
        pollingStrategy.pause();
    }

    public void resume() {
        pollingStrategy.resume();
    }

    @Override
    public void run() {
        try {
            pollingStrategy.poll();
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
