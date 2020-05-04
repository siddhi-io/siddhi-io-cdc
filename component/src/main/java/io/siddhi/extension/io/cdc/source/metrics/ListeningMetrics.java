package io.siddhi.extension.io.cdc.source.metrics;

import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class ListeningMetrics extends Metrics{

    private final String operationType;
    private boolean isLastReceivedTimeMetricsRegistered;
    private long lastReceivedTime;

    public ListeningMetrics(String siddhiAppName, String dbURL, String tableName, String operationType) {
        super(siddhiAppName, dbURL, tableName);
        this.operationType = operationType.substring(0, 1).toUpperCase(Locale.ENGLISH) + operationType.substring(1);
        cdcStatusServiceStartedMap.putIfAbsent(siddhiAppName, false);
    }

    @Override
    public  void updateFileStatus(ExecutorService executorService, String siddhiAppName) {
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
                    /*if (cdcStatusMap.isEmpty()) {
                        continue;
                    }
                    cdcStatusMap.forEach((cdcDatabase, cdcStatus) ->
                            System.out.println("DB:: " + cdcDatabase.cdcURL + ", " + cdcStatus + ",  ThreadID: " +
                                    Thread.currentThread().getId()));*/
                    // TODO: 4/20/20 Remove
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
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.event.count.%s.%s.%s.%s.%s",
                        siddhiAppName, dbType, operationType, databaseName, tableName, getDatabaseURL()),
                        Level.INFO);
    }

    public Counter getTotalEventCounterMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.events.per.table.%s.%s",
                        siddhiAppName, tableName, getDatabaseURL()), Level.INFO);
    }

    @Override
    protected void lastReceivedTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
                        siddhiAppName, "last_receive_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (cdcLastReceivedTimeMap.containsKey(cdcDatabase)) {
                                return cdcLastReceivedTimeMap.get(cdcDatabase);
                            }
                            return 0L;
                        });
    }

    private void setLastReceivedTimeByOperationMetric() {
        if (!isLastReceivedTimeMetricsRegistered) {
            MetricsDataHolder.getInstance().getMetricService()
                    .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s.%s",
                            siddhiAppName, "last_receive_time_by_operation", operationType, getDatabaseURL()),
                            Level.INFO, () -> lastReceivedTime);
            isLastReceivedTimeMetricsRegistered = true;
        }
    }

    @Override
    protected void idleTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
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
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
                        siddhiAppName, "db_status", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (cdcStatusMap.containsKey(cdcDatabase)) {
                                return cdcStatusMap.get(cdcDatabase).ordinal();
                            }
                            return -1;
                        });
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
    public synchronized void setLastReceivedTime(long lastReceivedTime) {
        this.lastReceivedTime = lastReceivedTime;
        setLastReceivedTimeByOperationMetric();
        if (cdcLastReceivedTimeMap.containsKey(cdcDatabase)) {
            if (cdcLastReceivedTimeMap.get(cdcDatabase) < lastReceivedTime) {
                cdcLastReceivedTimeMap.replace(cdcDatabase, lastReceivedTime);
            }
        } else {
            cdcLastReceivedTimeMap.put(cdcDatabase, lastReceivedTime);
            lastReceivedTimeMetric();
            idleTimeMetric();
        }
    }


}
