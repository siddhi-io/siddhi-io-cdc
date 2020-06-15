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

package io.siddhi.extension.io.cdc.source.listening;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.extension.io.cdc.source.metrics.CDCStatus;
import io.siddhi.extension.io.cdc.source.metrics.ListeningMetrics;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is for capturing change data using debezium embedded engine.
 **/
public abstract class ChangeDataCapture {
    private String operation;
    private Configuration config;
    private SourceEventListener sourceEventListener;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private boolean paused = false;
    private final ListeningMetrics metrics;
    private long previousEventCount;

    public ChangeDataCapture(String operation, SourceEventListener sourceEventListener, ListeningMetrics metrics) {
        this.operation = operation;
        this.sourceEventListener = sourceEventListener;
        this.metrics = metrics;
    }

    /**
     * Initialize this.config according to user specified parameters.
     */
    public void setConfig(Map<String, Object> configMap) {

        //set config from configMap.
        config = Configuration.empty();
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            config = config.edit().with(entry.getKey(), entry.getValue()).build();
        }
    }

    /**
     * Create a new Debezium embedded engine with the configuration {@code config} and,
     *
     * @return {@code engine}.
     */
    public EmbeddedEngine getEngine(EmbeddedEngine.CompletionCallback completionCallback) {
        // Create and return Engine with above set configuration ...
        EmbeddedEngine.Builder builder = EmbeddedEngine.create()
                .using(OffsetCommitPolicy.always())
                .using(completionCallback)
                .using(config);
        if (builder == null) {
            if (metrics != null) {
                metrics.setCDCStatus(CDCStatus.ERROR);
            }
            throw new SiddhiAppRuntimeException("CDC Engine create failed. Check parameters.");
        } else {
            EmbeddedEngine engine = builder.notifying(this::handleEvent).build();
            return engine;
        }
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * When an event is received, create and send the event details to the sourceEventListener.
     */
    private void handleEvent(ConnectRecord connectRecord) {
        Map<String, Object> detailsMap;

        if (paused) {
            lock.lock();
            try {
                while (paused) {
                    condition.await();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        detailsMap = createMap(connectRecord, operation);
        if (!detailsMap.isEmpty()) {
            previousEventCount = ((SourceMapper) sourceEventListener).getEventCount();
            sourceEventListener.onEvent(detailsMap, null);
            if (metrics != null) {
                metrics.getTotalReadsMetrics().inc();
                metrics.getEventCountMetric().inc();
                metrics.getTotalEventCounterMetric().inc();
                long eventCount = ((SourceMapper) sourceEventListener).getEventCount() - previousEventCount;
                metrics.getValidEventCountMetric().inc(eventCount);
                metrics.setCDCStatus(CDCStatus.CONSUMING);
                metrics.setLastReceivedTime(System.currentTimeMillis());
            }
        }
    }

    /**
     * Create Hash map using the connect record and operation,
     *
     * @param connectRecord is the change data object which is received from debezium embedded engine.
     * @param operation     is the change data event which is specified by the user.
     **/

    abstract Map<String, Object> createMap(ConnectRecord connectRecord, String operation);

}
