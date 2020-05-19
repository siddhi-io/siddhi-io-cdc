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

import org.apache.log4j.Logger;
import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

/**
 * Class which holds the metrics for cdc polling mode.
 */
public class PollingMetrics extends Metrics {

    private static final Logger log  = Logger.getLogger(PollingMetrics.class);
    private final Queue<String> pollingDetails = new LinkedList<>();
    private int receiveEventsPerPollingInterval;
    private int pollingHistorySize;

    public PollingMetrics(String siddhiAppName, String dbURL, String tableName) {
        super(siddhiAppName, dbURL, tableName);
        CDC_STATUS_SERVICE_STARTED_MAP.putIfAbsent(siddhiAppName, false);
    }

    @Override
    public void updateTableStatus(ExecutorService executorService, String siddhiAppName) {
        if (!CDC_STATUS_SERVICE_STARTED_MAP.get(siddhiAppName)) {
            CDC_STATUS_SERVICE_STARTED_MAP.replace(siddhiAppName, true);
            executorService.execute(() -> {
                while (CDC_STATUS_SERVICE_STARTED_MAP.get(siddhiAppName)) {
                    if (!CDC_STATUS_MAP.isEmpty()) {
                        CDC_LAST_RECEIVED_TIME_MAP.forEach((cdcDatabase, lastPublishedTime) -> {
                            if (cdcDatabase.siddhiAppName.equals(siddhiAppName)) {
                                long idleTime = System.currentTimeMillis() - lastPublishedTime;
                                if (idleTime / 1000 > 8) {
                                    CDC_STATUS_MAP.replace(cdcDatabase, CDCStatus.IDLE);
                                }
                            }
                        });
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.error(siddhiAppName + ": Error while updating the tables status.");
                    }
                }
            });
        }
    }

    @Override
    public Counter getEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.event.count.%s.%s.host." +
                                "%s.%s.%s", siddhiAppName, dbType, host, databaseName, tableName, getDatabaseURL()),
                        Level.INFO);
    }

    @Override
    protected void lastReceivedTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "last_receive_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (CDC_LAST_RECEIVED_TIME_MAP.containsKey(cdcDatabase)) {
                                return CDC_LAST_RECEIVED_TIME_MAP.get(cdcDatabase);
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
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Cdc.Source.Polling.%s.%s",
                        siddhiAppName, "db_status", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (CDC_STATUS_MAP.containsKey(cdcDatabase)) {
                                return CDC_STATUS_MAP.get(cdcDatabase).ordinal();
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
        if (pollingDetails.size() > pollingHistorySize) {
            String poll = pollingDetails.poll();
            MetricsDataHolder.getInstance().getMetricService().remove(poll);
        }
    }

    @Override
    public synchronized void setCDCStatus(CDCStatus cdcStatus) {
        if (cdcStatus == CDCStatus.ERROR) {
            if (CDC_STATUS_MAP.containsKey(cdcDatabase)) {
                CDC_STATUS_MAP.replace(cdcDatabase, CDCStatus.ERROR);
            } else {
                CDC_STATUS_MAP.put(cdcDatabase, CDCStatus.ERROR);
                setCDCDBStatusMetric();
            }
        } else {
            if (CDC_STATUS_MAP.containsKey(cdcDatabase)) {
                if (CDC_STATUS_MAP.get(cdcDatabase) != CDCStatus.ERROR) {
                    CDC_STATUS_MAP.replace(cdcDatabase, CDCStatus.CONSUMING);
                }
            } else {
                CDC_STATUS_MAP.put(cdcDatabase, CDCStatus.CONSUMING);
                setCDCDBStatusMetric();
            }
        }
    }

    @Override
    public synchronized void setLastReceivedTime(long lastPublishedTime) {
        if (CDC_LAST_RECEIVED_TIME_MAP.containsKey(cdcDatabase)) {
            if (CDC_LAST_RECEIVED_TIME_MAP.get(cdcDatabase) < lastPublishedTime) {
                CDC_LAST_RECEIVED_TIME_MAP.replace(cdcDatabase, lastPublishedTime);
            }
        } else {
            CDC_LAST_RECEIVED_TIME_MAP.put(cdcDatabase, lastPublishedTime);
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
