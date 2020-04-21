package io.siddhi.extension.io.cdc.source.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public class PollingMetrics {

    private static final Map<PollingMetrics.CDCDatabase, Long> cdcLastPublishedTimeMap = new HashMap<>();
    private static final Map<PollingMetrics.CDCDatabase, CDCStatus> cdcStatusMap = new HashMap<>();
    private static final Map<String, Boolean> cdcStatusServiceStartedMap = new HashMap<>();

    private final String siddhiAppName;
    private final String dbURL;
    private final String tableName;
    private int receiveEventsPerPollingInterval;
    private final PollingMetrics.CDCDatabase cdcDatabase;

    public PollingMetrics(String siddhiAppName, String dbURL, String tableName) {
        this.siddhiAppName = siddhiAppName;
        this.dbURL = dbURL;
        this.tableName = tableName;
        this.cdcDatabase = new PollingMetrics.CDCDatabase(siddhiAppName, dbURL + ":" + tableName);
        cdcStatusServiceStartedMap.putIfAbsent(siddhiAppName, false);
    }

    public static void updateFileStatus(ExecutorService executorService, String siddhiAppName) {
        if (!cdcStatusServiceStartedMap.get(siddhiAppName)) {
            cdcStatusServiceStartedMap.replace(siddhiAppName, true);
            executorService.execute(() -> {
                while (cdcStatusServiceStartedMap.get(siddhiAppName)) {
                    if (!cdcStatusMap.isEmpty()) {
                        cdcLastPublishedTimeMap.forEach((cdcDatabase, lastPublishedTime) -> {
                            if (cdcDatabase.siddhiAppName.equals(siddhiAppName)) {
                                long idleTime = System.currentTimeMillis() - lastPublishedTime;
                                if (idleTime / 1000 > 8) {
                                    cdcStatusMap.replace(cdcDatabase, CDCStatus.IDLE);
                                }
                            }
                        });
                    }
                    cdcStatusMap.forEach((cdcDatabase, cdcStatus) -> System.out.println(
                            "URL: " + cdcDatabase.cdcURL + ", Status: " + cdcStatus + ", Thread ID: "
                            + Thread.currentThread().getId()));
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public Counter getEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.event.count.%s",
                        siddhiAppName, dbURL + ":" + tableName), Level.INFO);
    }

    private void setLastReceivedTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "last_receive_time", dbURL + ":" + tableName),
                        Level.INFO, () -> {
                            if (cdcLastPublishedTimeMap.containsKey(cdcDatabase)) {
                                return cdcLastPublishedTimeMap.get(cdcDatabase);
                            }
                            return 0L;
                        });
    }

    private void setCDCDBStatusMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "db_status", dbURL + ":" + tableName),
                        Level.INFO, () -> {
                            if (cdcStatusMap.containsKey(cdcDatabase)) {
                                return cdcStatusMap.get(cdcDatabase).ordinal();
                            }
                            return -1;
                        });
    }

    private void setEventsPerPollingMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "events_per_polling_interval", dbURL + ":" + tableName),
                        Level.INFO, () -> receiveEventsPerPollingInterval);
    }



    public void setCDCStatus(CDCStatus cdcStatus) {
        if (cdcStatus == CDCStatus.ERROR) {
            if (cdcStatusMap.containsKey(cdcDatabase)) {
                cdcStatusMap.replace(cdcDatabase, CDCStatus.ERROR);
            } else {
                cdcStatusMap.put(cdcDatabase, CDCStatus.ERROR);
                setCDCDBStatusMetric();
            }
        } else {
            if (cdcStatusMap.containsKey(cdcDatabase)) {
                if (cdcStatusMap.get(cdcDatabase) != CDCStatus.ERROR) {
                    cdcStatusMap.replace(cdcDatabase, CDCStatus.CONSUMING);
                }
            } else {
                cdcStatusMap.put(cdcDatabase, CDCStatus.CONSUMING);
                setCDCDBStatusMetric();
            }
        }
    }

    public void setLastReceivedTime(long lastPublishedTime) {
        if (cdcLastPublishedTimeMap.containsKey(cdcDatabase)) {
            if (cdcLastPublishedTimeMap.get(cdcDatabase) < lastPublishedTime) {
                cdcLastPublishedTimeMap.replace(cdcDatabase, lastPublishedTime);
            }
        } else {
            cdcLastPublishedTimeMap.put(cdcDatabase, lastPublishedTime);
            setLastReceivedTimeMetric();
            setEventsPerPollingMetric();
        }
    }

    public void setReceiveEventsPerPollingInterval(int receiveEventsPerPollingInterval) {
        this.receiveEventsPerPollingInterval = receiveEventsPerPollingInterval;
    }

    private static class CDCDatabase {
        private final String cdcURL; //dbURL + ":" + tableName
        private final String siddhiAppName;

        public CDCDatabase(String siddhiAppName, String cdcURL) {
            this.cdcURL = cdcURL;
            this.siddhiAppName = siddhiAppName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PollingMetrics.CDCDatabase that = (PollingMetrics.CDCDatabase) o;
            return cdcURL.equals(that.cdcURL) &&
                    siddhiAppName.equals(that.siddhiAppName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cdcURL, siddhiAppName);
        }
    }

}
