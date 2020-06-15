/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.io.cdc.source.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Parent class of the ListeningMetrics and PollingMetrics. Holds the common metrics that can be used with both
 * listening and polling modes.
 */
public abstract class Metrics {

    static final Map<CDCDatabase, Long> CDC_LAST_RECEIVED_TIME_MAP = new HashMap<>();
    static final Map<CDCDatabase, CDCStatus> CDC_STATUS_MAP = new HashMap<>();
    static final Map<String, Boolean> CDC_STATUS_SERVICE_STARTED_MAP = new ConcurrentHashMap<>();

    protected final String siddhiAppName;
    protected String host;
    protected String databaseName;
    protected final String tableName;
    protected String dbType;
    protected final CDCDatabase cdcDatabase;

    public Metrics(String siddhiAppName, String url, String tableName) {
        this.siddhiAppName = siddhiAppName;
        this.tableName = tableName;
        this.cdcDatabase = new CDCDatabase(siddhiAppName, url + ":" + tableName);
        MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc", siddhiAppName), Level.INFO).inc();
    }

    public abstract void updateTableStatus(ExecutorService executorService, String siddhiAppName);

    public abstract Counter getEventCountMetric();

    protected abstract void lastReceivedTimeMetric();

    protected abstract void setCDCDBStatusMetric();

    public abstract void setCDCStatus(CDCStatus cdcStatus);

    public abstract void setLastReceivedTime(long lastPublishedTime);

    protected abstract void idleTimeMetric();

    protected String getDatabaseURL() {
        return dbType + ":" + host + "/" + databaseName + "/" + tableName;
    }

    public Counter getTotalReadsMetrics() { //to count the total reads from siddhi app level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Reads.%s", siddhiAppName, "cdc"),
                        Level.INFO);
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * CDCDatabase holds the SiddhiAppName and the database URL to be use as key.
     */
    protected static class CDCDatabase {
        protected String cdcURL; //dbURL + ":" + tableName
        protected String siddhiAppName;

        public CDCDatabase(String siddhiAppName, String cdcURL) {
            this.cdcURL = cdcURL;
            this.siddhiAppName = siddhiAppName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CDCDatabase that = (CDCDatabase) o;
            return cdcURL.equals(that.cdcURL) &&
                    siddhiAppName.equals(that.siddhiAppName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cdcURL, siddhiAppName);
        }
    }
}

