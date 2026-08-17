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

import io.debezium.data.Envelope;
import io.debezium.data.VariableScaleDecimal;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link RdbmsChangeDataCapture}, covering the translation of Debezium change events into the detail
 * map handed to Siddhi.
 * <p>
 * The change events are assembled through Debezium's own {@link Envelope} builder rather than hand written schemas,
 * so an upgrade that reshapes the envelope fails here instead of silently producing empty streams at runtime.
 */
public class RdbmsChangeDataCaptureTest {

    private static final String TOPIC = "testApp.SimpleDB.login";
    private static final long SOURCE_TIMESTAMP = 1700000000000L;
    private static final long EVENT_TIMESTAMP = 1700000000123L;
    private static final String ALL_OPERATIONS = "insert,update,delete";

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("test.Source")
            .field(CDCSourceConstants.EVENT_TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    /**
     * A row with one column per value conversion {@code RdbmsChangeDataCapture} performs.
     */
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
            .name("test.login.Value")
            .field("id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("amount", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("quantity", Schema.OPTIONAL_INT16_SCHEMA)
            .field("flag", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("tiny", Schema.OPTIONAL_INT8_SCHEMA)
            .field("price", VariableScaleDecimal.builder().optional().build())
            .build();

    private static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("test.login.Envelope")
            .withRecord(ROW_SCHEMA)
            .withSource(SOURCE_SCHEMA)
            .build();

    private static RdbmsChangeDataCapture capture(String operation) {
        return new RdbmsChangeDataCapture(operation, null, null);
    }

    private static Struct row(String id, String name, Double amount, Short quantity, Boolean flag, Byte tiny,
                              BigDecimal price) {
        Struct row = new Struct(ROW_SCHEMA);
        row.put("id", id);
        row.put("name", name);
        row.put("amount", amount);
        row.put("quantity", quantity);
        row.put("flag", flag);
        row.put("tiny", tiny);
        if (price != null) {
            row.put("price", VariableScaleDecimal.fromLogical(
                    VariableScaleDecimal.builder().optional().build(), price));
        }
        return row;
    }

    private static Struct simpleRow(String id, String name) {
        return row(id, name, 100.0, (short) 5, true, (byte) 3, new BigDecimal("12.34"));
    }

    private static SourceRecord record(String op, Struct before, Struct after) {
        Struct source = new Struct(SOURCE_SCHEMA);
        source.put(CDCSourceConstants.EVENT_TIMESTAMP, SOURCE_TIMESTAMP);

        Struct value = new Struct(ENVELOPE.schema());
        value.put(Envelope.FieldName.OPERATION, op);
        value.put(Envelope.FieldName.SOURCE, source);
        value.put(Envelope.FieldName.TIMESTAMP, EVENT_TIMESTAMP);
        if (before != null) {
            value.put(Envelope.FieldName.BEFORE, before);
        }
        if (after != null) {
            value.put(Envelope.FieldName.AFTER, after);
        }
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), TOPIC, null, null,
                ENVELOPE.schema(), value);
    }

    /**
     * The envelope this extension reads from is Debezium's, so confirm it still exposes the fields being read.
     */
    @Test
    public void envelopeExposesTheFieldsTheExtensionReads() {
        Schema schema = ENVELOPE.schema();
        Assert.assertNotNull(schema.field(CDCSourceConstants.CONNECT_RECORD_OPERATION));
        Assert.assertNotNull(schema.field(CDCSourceConstants.BEFORE));
        Assert.assertNotNull(schema.field(CDCSourceConstants.AFTER));
        Assert.assertNotNull(schema.field(CDCSourceConstants.SOURCE_SCHEMA));
        Assert.assertNotNull(schema.field(CDCSourceConstants.EVENT_TIMESTAMP));
        Assert.assertNotNull(schema.field(CDCSourceConstants.SOURCE_SCHEMA).schema()
                .field(CDCSourceConstants.EVENT_TIMESTAMP));
    }

    @Test
    public void insertEmitsTheAfterImage() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null, simpleRow("e001", "employer")),
                CDCSourceConstants.INSERT);

        Assert.assertEquals(detailsMap.get("id"), "e001");
        Assert.assertEquals(detailsMap.get("name"), "employer");
        Assert.assertFalse(detailsMap.containsKey(CDCSourceConstants.BEFORE_PREFIX + "id"),
                "Single operation mode should not emit a before image for an insert");
    }

    @Test
    public void deleteEmitsTheBeforeImage() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.DELETE).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION, simpleRow("e001", "employer"), null),
                CDCSourceConstants.DELETE);

        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "id"), "e001");
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "name"), "employer");
        Assert.assertFalse(detailsMap.containsKey("id"),
                "Single operation mode should not emit an after image for a delete");
    }

    @Test
    public void updateEmitsBothImages() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.UPDATE).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION, simpleRow("e001", "old"),
                        simpleRow("e001", "new")),
                CDCSourceConstants.UPDATE);

        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "name"), "old");
        Assert.assertEquals(detailsMap.get("name"), "new");
    }

    /**
     * With several operations configured, one Siddhi stream receives all of them, so the absent image is padded with
     * type defaults to keep the attribute set stable across events.
     */
    @Test
    public void multiOperationInsertPadsTheBeforeImageWithDefaults() {
        Map<String, Object> detailsMap = capture(ALL_OPERATIONS).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null, simpleRow("e001", "employer")),
                ALL_OPERATIONS);

        Assert.assertEquals(detailsMap.get("id"), "e001");
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "id"), "");
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "amount"), 0.0);
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "flag"), false);
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "quantity"), 0);
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "price"), 0);
    }

    @Test
    public void multiOperationDeletePadsTheAfterImageWithDefaults() {
        Map<String, Object> detailsMap = capture(ALL_OPERATIONS).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION, simpleRow("e001", "employer"), null),
                ALL_OPERATIONS);

        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "id"), "e001");
        Assert.assertEquals(detailsMap.get("id"), "");
        Assert.assertEquals(detailsMap.get("amount"), 0.0);
        Assert.assertEquals(detailsMap.get("flag"), false);
    }

    @Test
    public void multiOperationUpdateEmitsBothImages() {
        Map<String, Object> detailsMap = capture(ALL_OPERATIONS).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION, simpleRow("e001", "old"),
                        simpleRow("e001", "new")),
                ALL_OPERATIONS);

        Assert.assertEquals(detailsMap.get(CDCSourceConstants.BEFORE_PREFIX + "name"), "old");
        Assert.assertEquals(detailsMap.get("name"), "new");
    }

    @Test
    public void multiOperationListToleratesWhitespace() {
        Map<String, Object> detailsMap = capture("insert, update , delete").createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null, simpleRow("e001", "employer")),
                "insert, update , delete");

        Assert.assertEquals(detailsMap.get("id"), "e001");
    }

    /**
     * Siddhi has no 16 or 8 bit integer type, so these are widened rather than passed through as Short and Byte.
     */
    @Test
    public void shortAndByteColumnsAreWidenedToInteger() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null,
                        row("e001", "employer", 1.0, (short) 42, true, (byte) 7, null)),
                CDCSourceConstants.INSERT);

        Assert.assertEquals(detailsMap.get("quantity"), 42);
        Assert.assertEquals(detailsMap.get("tiny"), 7);
    }

    @Test
    public void variableScaleDecimalWithoutAFractionBecomesLong() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null,
                        row("e001", "employer", 1.0, (short) 1, true, (byte) 1, new BigDecimal("500.00"))),
                CDCSourceConstants.INSERT);

        Assert.assertEquals(detailsMap.get("price"), 500L);
    }

    @Test
    public void variableScaleDecimalWithAFractionBecomesDouble() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null,
                        row("e001", "employer", 1.0, (short) 1, true, (byte) 1, new BigDecimal("123.45"))),
                CDCSourceConstants.INSERT);

        Assert.assertEquals(detailsMap.get("price"), 123.45);
    }

    @Test
    public void transportPropertiesCarryTheOperationAndTimestamps() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION, null, simpleRow("e001", "employer")),
                CDCSourceConstants.INSERT);

        List transportProperties = (List) detailsMap.get(CDCSourceConstants.TRANSPORT_PROPERTIES);
        Assert.assertEquals(transportProperties.get(0), CDCSourceConstants.INSERT);
        Assert.assertEquals(transportProperties.get(1), SOURCE_TIMESTAMP);
        Assert.assertEquals(transportProperties.get(2), EVENT_TIMESTAMP);
    }

    @Test
    public void eventsNotMatchingTheConfiguredOperationAreIgnored() {
        Assert.assertTrue(capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION, simpleRow("e001", "old"),
                        simpleRow("e001", "new")),
                CDCSourceConstants.INSERT).isEmpty());
    }

    /**
     * Snapshot records carry the read operation, which is not one a user can subscribe to.
     */
    @Test
    public void snapshotReadEventsAreIgnored() {
        Assert.assertTrue(capture(CDCSourceConstants.INSERT).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INITIAL_SYNC, null, simpleRow("e001", "employer")),
                CDCSourceConstants.INSERT).isEmpty());

        Assert.assertTrue(capture(ALL_OPERATIONS).createMap(
                record(CDCSourceConstants.CONNECT_RECORD_INITIAL_SYNC, null, simpleRow("e001", "employer")),
                ALL_OPERATIONS).isEmpty());
    }

    /**
     * Debezium emits schema change and heartbeat records without an operation; these must be skipped rather than
     * failing the engine.
     */
    @Test
    public void recordsWithoutAnOperationFieldAreIgnored() {
        Schema schemaWithoutOperation = SchemaBuilder.struct().name("test.NoOp")
                .field("unrelated", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(schemaWithoutOperation);
        value.put("unrelated", "value");
        SourceRecord sourceRecord = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), TOPIC,
                null, null, schemaWithoutOperation, value);

        Assert.assertTrue(capture(CDCSourceConstants.INSERT)
                .createMap(sourceRecord, CDCSourceConstants.INSERT).isEmpty());
    }
}
