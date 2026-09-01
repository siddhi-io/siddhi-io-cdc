/*
 * Copyright (c) 2026, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.siddhi.extension.io.cdc.source.CDCSource;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs the real Debezium embedded engine end to end, with a synthetic connector in place of a database.
 * <p>
 * Every other test in this suite exercises translation and configuration logic only, and never starts the engine.
 * That leaves the whole Kafka Connect runtime this extension depends on — {@code WorkerConfig}, the converters, the
 * offset backing store, the commit policy, the transform chain — completely unexercised, which matters because
 * Debezium is compiled against a different Kafka Connect minor line than the one we resolve at build time. A binary
 * incompatibility there surfaces as a {@link LinkageError} the moment the engine is constructed or run, and this test
 * is what turns that into a build failure instead of a runtime surprise.
 * <p>
 * {@code io.debezium.connector.simple.SimpleSourceConnector} ships in the debezium-embedded artifact and emits
 * synthetic records, so no database or container is involved.
 */
public class DebeziumEngineCompatibilityTest {

    private static final String SIMPLE_SOURCE_CONNECTOR = "io.debezium.connector.simple.SimpleSourceConnector";
    private static final int RECORDS_PER_BATCH = 2;
    private static final int BATCH_COUNT = 3;
    private static final int EXPECTED_RECORDS = RECORDS_PER_BATCH * BATCH_COUNT;

    /**
     * Collects the records the engine delivers. Returning an empty map keeps
     * {@code ChangeDataCapture.handleEvent} from going on to the source event listener, which a unit test has no
     * way to supply.
     */
    private static final class RecordingChangeDataCapture extends ChangeDataCapture {
        private final AtomicInteger records = new AtomicInteger();
        private final CountDownLatch latch;

        private RecordingChangeDataCapture(int expected) {
            super(CDCSourceConstants.INSERT, null, null);
            this.latch = new CountDownLatch(expected);
        }

        @Override
        Map<String, Object> createMap(ConnectRecord connectRecord, String operation) {
            records.incrementAndGet();
            latch.countDown();
            return new HashMap<>();
        }
    }

    private Map<String, Object> engineConfig(int cdcSourceHashCode) {
        Map<String, Object> config = new HashMap<>();
        config.put(CDCSourceConstants.CONNECTOR_NAME, "debezium-compatibility-test");
        config.put(CDCSourceConstants.CONNECTOR_CLASS, SIMPLE_SOURCE_CONNECTOR);
        config.put(CDCSourceConstants.OFFSET_STORAGE, InMemoryOffsetBackingStore.class.getName());
        config.put(CDCSourceConstants.CDC_SOURCE_OBJECT, cdcSourceHashCode);
        config.put("offset.flush.interval.ms", 1000);
        // Kafka Connect 4.x made bootstrap.servers a required WorkerConfig property with no default.
        // The embedded engine never talks to a broker, so the value is inert, but it must be present.
        config.put("bootstrap.servers", "localhost:9092");
        config.put("topic.name", "compatibility.topic");
        config.put("record.count.per.batch", RECORDS_PER_BATCH);
        config.put("batch.count", BATCH_COUNT);
        return config;
    }

    /**
     * Constructs the engine through {@link ChangeDataCapture#getEngine} — the production code path — and runs it.
     */
    @Test(timeOut = 90000)
    public void embeddedEngineStartsAndDeliversRecords() throws Exception {
        CDCSource cdcSource = new CDCSource();
        CDCSourceObjectKeeper.getCdcSourceObjectKeeper().addCdcObject(cdcSource);
        RecordingChangeDataCapture capture = new RecordingChangeDataCapture(EXPECTED_RECORDS);
        capture.setConfig(engineConfig(cdcSource.hashCode()));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<String> failureMessage = new AtomicReference<>();
        DebeziumEngine.CompletionCallback callback = (success, message, error) -> {
            if (!success) {
                failure.set(error);
                failureMessage.set(message);
            }
        };

        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = capture.getEngine(callback);
        Assert.assertNotNull(engine, "Debezium engine could not be created");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            executorService.execute(engine);
            boolean delivered = capture.latch.await(45, TimeUnit.SECONDS);

            assertNoLinkageError(failure.get(), failureMessage.get());
            Assert.assertTrue(delivered, "Engine delivered only " + capture.records.get() + " of "
                    + EXPECTED_RECORDS + " records. Engine message: " + failureMessage.get()
                    + ", error: " + failure.get());
        } finally {
            closeWithoutWaiting(engine);
            executorService.shutdownNow();
            CDCSourceObjectKeeper.getCdcSourceObjectKeeper().removeObject(cdcSource.hashCode());
        }

        assertNoLinkageError(failure.get(), failureMessage.get());
    }

    /**
     * The engine writes offsets through {@link InMemoryOffsetBackingStore}, which extends a Kafka Connect class and
     * is otherwise never instantiated by any test. Reaching the point where offsets are handed back to the
     * {@link CDCSource} proves the whole store contract still binds.
     */
    @Test(timeOut = 90000)
    public void offsetsAreHandedBackToTheSource() throws Exception {
        CDCSource cdcSource = new CDCSource();
        CDCSourceObjectKeeper.getCdcSourceObjectKeeper().addCdcObject(cdcSource);
        RecordingChangeDataCapture capture = new RecordingChangeDataCapture(EXPECTED_RECORDS);
        capture.setConfig(engineConfig(cdcSource.hashCode()));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<String> failureMessage = new AtomicReference<>();
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = capture.getEngine(
                (success, message, error) -> {
                    if (!success) {
                        failure.set(error);
                        failureMessage.set(message);
                    }
                });

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            executorService.execute(engine);
            capture.latch.await(45, TimeUnit.SECONDS);
            assertNoLinkageError(failure.get(), failureMessage.get());
            Assert.assertNotNull(cdcSource.getOffsetData(),
                    "No offsets reached the CDCSource, so the offset backing store never completed a save");
        } finally {
            closeWithoutWaiting(engine);
            executorService.shutdownNow();
            CDCSourceObjectKeeper.getCdcSourceObjectKeeper().removeObject(cdcSource.hashCode());
        }
    }

    /**
     * {@code DebeziumEngine.close()} waits up to five minutes for the connector to stop, which exceeds any sensible
     * test timeout and would mask the assertion result.
     */
    private void closeWithoutWaiting(DebeziumEngine<?> engine) {
        Thread closer = new Thread(() -> {
            try {
                engine.close();
            } catch (Exception ignored) {
                // shutting down; nothing useful to do here
            }
        });
        closer.setDaemon(true);
        closer.start();
    }

    /**
     * A {@link LinkageError} here means Debezium and the resolved Kafka Connect version are binary incompatible,
     * which is a different and far more serious problem than the engine failing to start for a configuration reason.
     */
    private void assertNoLinkageError(Throwable error, String message) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof LinkageError) {
                Assert.fail("Debezium is binary incompatible with the resolved Kafka Connect version. "
                        + t.getClass().getName() + ": " + t.getMessage() + " (engine message: " + message + ")");
            }
        }
    }
}
