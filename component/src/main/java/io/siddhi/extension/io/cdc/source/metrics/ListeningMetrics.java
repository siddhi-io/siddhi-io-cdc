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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.Locale;
import java.util.concurrent.ExecutorService;

/**
 * Class which holds the metrics for cdc listening mode.
 */
public class ListeningMetrics extends Metrics {

    private static final Logger log = LogManager.getLogger(ListeningMetrics.class);
    private final String operationType;
    private boolean isLastReceivedTimeMetricsRegistered;
    private long lastReceivedTime;

    public ListeningMetrics(String siddhiAppName, String dbURL, String tableName, String operationType) {
        super(siddhiAppName, dbURL, tableName);
        this.operationType = operationType.substring(0, 1).toUpperCase(Locale.ENGLISH) + operationType.substring(1);
        CDC_STATUS_SERVICE_STARTED_MAP.putIfAbsent(siddhiAppName, false);
    }

    @Override
    public void updateTableStatus(ExecutorService executorService, String siddhiAppName) {
        if (!CDC_STATUS_SERVICE_STARTED_MAP.get(siddhiAppName)) {
            CDC_STATUS_SERVICE_STARTED_MAP.replace(siddhiAppName, true);
            executorService.execute(() -> {
                while (CDC_STATUS_SERVICE_STARTED_MAP.get(siddhiAppName)) {
                    if (!CDC_STATUS_MAP.isEmpty()) {
                        CDC_LAST_RECEIVED_TIME_MAP.forEach((cdcDatabase, lastReceiveTime) -> {
                            if (cdcDatabase.siddhiAppName.equals(siddhiAppName)) {
                                long idleTime = System.currentTimeMillis() - lastReceiveTime;
                                if (idleTime / 1000 > 8) {
                                    CDC_STATUS_MAP.replace(cdcDatabase, CDCStatus.IDLE);
                                }
                            }
                        });
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.error("{}: Error while updating the tables status.", siddhiAppName);
                    }
                }
            });
        }
    }

    @Override
    public Counter getEventCountMetric() { //counts events per operation.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.event.count." +
                                "%s.%s.host.%s.%s.%s.%s", siddhiAppName, dbType, host, operationType, databaseName,
                        tableName, getDatabaseURL()), Level.INFO);
    }

    public Counter getTotalEventCounterMetric() { //count events per table.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.events.per.table.%s.%s",
                        siddhiAppName, tableName, getDatabaseURL()), Level.INFO);
    }

    public Counter getValidEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService().counter(
                String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
                        siddhiAppName, "total_valid_events_count", getDatabaseURL()), Level.INFO);
    }

    private Counter getTotalErrorCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService().counter(
                String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
                        siddhiAppName, "total_error_count", getDatabaseURL()), Level.INFO);
    }

    @Override
    protected void lastReceivedTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
                        siddhiAppName, "last_receive_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (CDC_LAST_RECEIVED_TIME_MAP.containsKey(cdcDatabase)) {
                                return CDC_LAST_RECEIVED_TIME_MAP.get(cdcDatabase);
                            }
                            return 0L;
                        });
    }

    private void setLastReceivedTimeByOperationMetric() {
        if (!isLastReceivedTimeMetricsRegistered) {
            MetricsDataHolder.getInstance().getMetricService()
                    .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s.%s",
                            siddhiAppName, "last_receive_time_by_operation", operationType, getDatabaseURL()),
                            Level.INFO, () -> {
                                synchronized (this) {
                                    return lastReceivedTime;
                                }
                            });
            isLastReceivedTimeMetricsRegistered = true;
        }
    }

    @Override
    protected void idleTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Listening.%s.%s",
                        siddhiAppName, "idle_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (CDC_LAST_RECEIVED_TIME_MAP.containsKey(cdcDatabase)) {
                                return (System.currentTimeMillis() - CDC_LAST_RECEIVED_TIME_MAP.get(cdcDatabase))
                                        / 1000;
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
                            if (CDC_STATUS_MAP.containsKey(cdcDatabase)) {
                                return CDC_STATUS_MAP.get(cdcDatabase).ordinal();
                            }
                            return -1;
                        });
    }

    @Override
    public synchronized void setCDCStatus(CDCStatus cdcStatus) {
        if (cdcStatus == CDCStatus.ERROR) {
            getTotalErrorCountMetric().inc();
        }
        if (CDC_STATUS_MAP.containsKey(cdcDatabase)) {
            CDC_STATUS_MAP.replace(cdcDatabase, cdcStatus);
        } else {
            CDC_STATUS_MAP.put(cdcDatabase, cdcStatus);
            setCDCDBStatusMetric();
        }
    }

    @Override
    public synchronized void setLastReceivedTime(long lastReceivedTime) {
        this.lastReceivedTime = lastReceivedTime;
        setLastReceivedTimeByOperationMetric();
        if (CDC_LAST_RECEIVED_TIME_MAP.containsKey(cdcDatabase)) {
            if (CDC_LAST_RECEIVED_TIME_MAP.get(cdcDatabase) < lastReceivedTime) {
                CDC_LAST_RECEIVED_TIME_MAP.replace(cdcDatabase, lastReceivedTime);
            }
        } else {
            CDC_LAST_RECEIVED_TIME_MAP.put(cdcDatabase, lastReceivedTime);
            lastReceivedTimeMetric();
            idleTimeMetric();
            getTotalErrorCountMetric();
        }
    }

}
