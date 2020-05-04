package io.siddhi.extension.io.cdc.source.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class PollingMetrics extends Metrics{

/*    private static final Map<Metrics.CDCDatabase, Long> cdcLastReceivedTimeMap = new HashMap<>();
    private static final Map<Metrics.CDCDatabase, CDCStatus> cdcStatusMap = new HashMap<>();
    private static final Map<String, Boolean> cdcStatusServiceStartedMap = new ConcurrentHashMap<>();*/

    private Queue<String> pollingDetails = new LinkedList<>();
    private int receiveEventsPerPollingInterval;
    private int pollingHistorySize = 5;

    public PollingMetrics(String siddhiAppName, String dbURL, String tableName) {
        super(siddhiAppName, dbURL, tableName);
        cdcStatusServiceStartedMap.putIfAbsent(siddhiAppName, false);
    }

    @Override
    public void updateFileStatus(ExecutorService executorService, String siddhiAppName) {
        if (!cdcStatusServiceStartedMap.get(siddhiAppName)) {
            cdcStatusServiceStartedMap.replace(siddhiAppName, true);
            executorService.execute(() -> {
                while (cdcStatusServiceStartedMap.get(siddhiAppName)) {
                    if (!cdcStatusMap.isEmpty()) {
                        cdcLastReceivedTimeMap.forEach((cdcDatabase, lastPublishedTime) -> {
                            if (cdcDatabase.siddhiAppName.equals(siddhiAppName)) {
                                long idleTime = System.currentTimeMillis() - lastPublishedTime;
                                if (idleTime / 1000 > 8) {
                                    cdcStatusMap.replace(cdcDatabase, CDCStatus.IDLE);
                                }
                            }
                        });
                    }
                    /*cdcStatusMap.forEach((cdcDatabase, cdcStatus) -> System.out.println(
                            "URL: " + cdcDatabase.cdcURL + ", Status: " + cdcStatus + ", Thread ID: "
                            + Thread.currentThread().getId()));*/
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    @Override
    public Counter getEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.event.count.%s.%s.%s.%s",
                        siddhiAppName, dbType, databaseName, tableName, getDatabaseURL()), Level.INFO);
    }

    @Override
    protected void lastReceivedTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "last_receive_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (cdcLastReceivedTimeMap.containsKey(cdcDatabase)) {
                                return cdcLastReceivedTimeMap.get(cdcDatabase);
                            }
                            return 0L;
                        });
    }

    @Override
    protected void idleTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "idle_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (cdcLastReceivedTimeMap.containsKey(cdcDatabase)) {
                                return (System.currentTimeMillis() - cdcLastReceivedTimeMap.get(cdcDatabase)) / 1000;
                            }
                            return 0L;
                        });
    }

    @Override
    protected void setCDCDBStatusMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "db_status", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (cdcStatusMap.containsKey(cdcDatabase)) {
                                return cdcStatusMap.get(cdcDatabase).ordinal();
                            }
                            return -1;
                        });
    }

    private void setEventsInLastPollingMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "events_in_last_polling_interval", getDatabaseURL()),
                        Level.INFO, () -> receiveEventsPerPollingInterval);
    }

    public void pollingDetailsMetric(int events, long startedTime, long duration, CDCStatus cdcStatus) {
        String metricName = String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.Details.%s.%s.%s.%s",
                siddhiAppName, startedTime, duration, cdcStatus.ordinal(), getDatabaseURL());
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(metricName, Level.INFO, () -> events);
        pollingDetails.add(metricName);
        if (pollingDetails.size() > pollingHistorySize ) {
            String poll = pollingDetails.poll();
            MetricsDataHolder.getInstance().getMetricService().remove(poll);
        }
    }

    @Override
    public synchronized void setCDCStatus(CDCStatus cdcStatus) {
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

    @Override
    public synchronized void setLastReceivedTime(long lastPublishedTime) {
        if (cdcLastReceivedTimeMap.containsKey(cdcDatabase)) {
            if (cdcLastReceivedTimeMap.get(cdcDatabase) < lastPublishedTime) {
                cdcLastReceivedTimeMap.replace(cdcDatabase, lastPublishedTime);
            }
        } else {
            cdcLastReceivedTimeMap.put(cdcDatabase, lastPublishedTime);
            lastReceivedTimeMetric();
            setEventsInLastPollingMetric();
            idleTimeMetric();
        }
    }

    public void setReceiveEventsPerPollingInterval(int receiveEventsPerPollingInterval) {
        this.receiveEventsPerPollingInterval = receiveEventsPerPollingInterval;
    }

    public void setPollingHistorySize(int pollingHistorySize) {
        this.pollingHistorySize = pollingHistorySize;
    }
}
