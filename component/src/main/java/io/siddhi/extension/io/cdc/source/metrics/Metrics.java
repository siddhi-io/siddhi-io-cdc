package io.siddhi.extension.io.cdc.source.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public abstract class Metrics {

    protected static final Map<CDCDatabase, Long> cdcLastReceivedTimeMap = new HashMap<>();
    protected static final Map<CDCDatabase, CDCStatus> cdcStatusMap = new HashMap<>();
    protected static final Map<String, Boolean> cdcStatusServiceStartedMap = new ConcurrentHashMap<>();

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

    public abstract void updateFileStatus(ExecutorService executorService, String siddhiAppName);

    public abstract Counter getEventCountMetric();

    protected abstract void lastReceivedTimeMetric();

    protected abstract void setCDCDBStatusMetric();

    public abstract void setCDCStatus(CDCStatus cdcStatus);

    public abstract void setLastReceivedTime(long lastPublishedTime);

    protected abstract void idleTimeMetric();

    protected String getDatabaseURL() {
        return dbType + ":" + databaseName + "/" + tableName;
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

    protected static class CDCDatabase {
        protected String cdcURL; //dbURL + ":" + tableName
        protected String siddhiAppName;

        public CDCDatabase(String siddhiAppName, String cdcURL) {
            this.cdcURL = cdcURL;
            this.siddhiAppName = siddhiAppName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
