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

import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.connector.mongodb.MongoDbSchemaFactory;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MongoChangeDataCapture}, covering the translation of Debezium MongoDB change events into the
 * detail map handed to Siddhi.
 * <p>
 * The event schemas are assembled from Debezium's own constants and schema factory rather than hand written strings,
 * so that a future Debezium upgrade which renames or reshapes the change event fails these tests instead of silently
 * producing empty streams at runtime.
 */
public class MongoChangeDataCaptureTest {

    private static final String TOPIC = "test.SimpleDB.SweetProductionTable";
    private static final String OBJECT_ID = "5f2b1c3d4e5f6a7b8c9d0e1f";

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("test.SimpleDB.SweetProductionTable.Key")
            .field(CDCSourceConstants.MONGO_COLLECTION_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    /**
     * Mirrors the value schema Debezium's MongoDB connector produces: an envelope carrying the full document as
     * relaxed extended JSON plus, for updates, the {@code updateDescription} struct built by Debezium itself.
     */
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("test.SimpleDB.SweetProductionTable.Envelope")
            .field(Envelope.FieldName.AFTER, Json.builder().optional().build())
            .field(Envelope.FieldName.BEFORE, Json.builder().optional().build())
            .field(MongoDbFieldName.UPDATE_DESCRIPTION, MongoDbSchemaFactory.get().updatedDescriptionSchema())
            .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
            .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private static MongoChangeDataCapture capture(String operation) {
        return new MongoChangeDataCapture(operation, null, null);
    }

    private static SourceRecord record(Struct value) {
        Struct key = new Struct(KEY_SCHEMA);
        key.put(CDCSourceConstants.MONGO_COLLECTION_ID, "{\"$oid\": \"" + OBJECT_ID + "\"}");
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), TOPIC,
                KEY_SCHEMA, key, VALUE_SCHEMA, value);
    }

    private static Struct value(String op) {
        Struct value = new Struct(VALUE_SCHEMA);
        value.put(Envelope.FieldName.OPERATION, op);
        value.put(Envelope.FieldName.TIMESTAMP, 1700000000000L);
        return value;
    }

    private static Struct updateDescription(String updatedFieldsJson) {
        Struct updateDescription = new Struct(MongoDbSchemaFactory.get().updatedDescriptionSchema());
        updateDescription.put(MongoDbFieldName.UPDATED_FIELDS, updatedFieldsJson);
        return updateDescription;
    }

    /**
     * Guards the field names this extension hardcodes against the constants Debezium actually publishes. This is the
     * check that would have caught the {@code patch} to {@code updateDescription} rename in Debezium 2.x.
     */
    @Test
    public void mongoFieldNamesMatchDebeziumConstants() {
        Assert.assertEquals(CDCSourceConstants.MONGO_UPDATE_DESCRIPTION, MongoDbFieldName.UPDATE_DESCRIPTION,
                "Debezium renamed the update description field");
        Assert.assertEquals(CDCSourceConstants.MONGO_UPDATED_FIELDS, MongoDbFieldName.UPDATED_FIELDS,
                "Debezium renamed the updated fields entry of the update description");
        Assert.assertEquals(CDCSourceConstants.CONNECT_RECORD_OPERATION, Envelope.FieldName.OPERATION);
        Assert.assertEquals(CDCSourceConstants.AFTER, Envelope.FieldName.AFTER);
        Assert.assertEquals(CDCSourceConstants.BEFORE, Envelope.FieldName.BEFORE);
    }

    @Test
    public void insertEventCapturesTheFullDocument() {
        Struct value = value(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION);
        value.put(Envelope.FieldName.AFTER,
                "{\"_id\": {\"$oid\": \"" + OBJECT_ID + "\"}, \"name\": \"sweets\", \"amount\": 100.0, "
                        + "\"volume\": 5}");

        Map<String, Object> detailsMap = capture(CDCSourceConstants.INSERT)
                .createMap(record(value), CDCSourceConstants.INSERT);

        Assert.assertEquals(detailsMap.get("name"), "sweets");
        Assert.assertEquals(detailsMap.get("amount"), 100.0);
        Assert.assertEquals(detailsMap.get("volume"), 5);
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.MONGO_COLLECTION_ID), OBJECT_ID);
    }

    /**
     * Debezium 2.x reports updates through {@code updateDescription.updatedFields} rather than the 1.x {@code patch}
     * document. Only the changed fields are emitted, preserving the pre-upgrade contract.
     */
    @Test
    public void updateEventCapturesChangedFieldsFromUpdateDescription() {
        Struct value = value(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION);
        value.put(MongoDbFieldName.UPDATE_DESCRIPTION, updateDescription("{\"amount\": 500.0}"));

        Map<String, Object> detailsMap = capture(CDCSourceConstants.UPDATE)
                .createMap(record(value), CDCSourceConstants.UPDATE);

        Assert.assertEquals(detailsMap.get("amount"), 500.0);
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.MONGO_COLLECTION_ID), OBJECT_ID);
        Assert.assertFalse(detailsMap.containsKey("name"), "Only the updated fields should be emitted");
    }

    /**
     * Under {@code capture.mode=change_streams} Debezium omits the full document but still reports the update
     * description. Under the default {@code change_streams_update_full} both are present; the update description
     * remains the source of truth so that the emitted attributes stay limited to what actually changed.
     */
    @Test
    public void updateEventPrefersUpdateDescriptionOverFullDocument() {
        Struct value = value(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION);
        value.put(Envelope.FieldName.AFTER,
                "{\"_id\": {\"$oid\": \"" + OBJECT_ID + "\"}, \"name\": \"sweets\", \"amount\": 500.0}");
        value.put(MongoDbFieldName.UPDATE_DESCRIPTION, updateDescription("{\"amount\": 500.0}"));

        Map<String, Object> detailsMap = capture(CDCSourceConstants.UPDATE)
                .createMap(record(value), CDCSourceConstants.UPDATE);

        Assert.assertEquals(detailsMap.get("amount"), 500.0);
        Assert.assertFalse(detailsMap.containsKey("name"), "Only the updated fields should be emitted");
    }

    /**
     * A replace operation carries a full document but no update description; falling back to the full document keeps
     * those events flowing instead of dropping them.
     */
    @Test
    public void updateEventFallsBackToFullDocumentWhenUpdateDescriptionIsAbsent() {
        Struct value = value(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION);
        value.put(Envelope.FieldName.AFTER,
                "{\"_id\": {\"$oid\": \"" + OBJECT_ID + "\"}, \"name\": \"sweets\", \"amount\": 500.0}");

        Map<String, Object> detailsMap = capture(CDCSourceConstants.UPDATE)
                .createMap(record(value), CDCSourceConstants.UPDATE);

        Assert.assertEquals(detailsMap.get("name"), "sweets");
        Assert.assertEquals(detailsMap.get("amount"), 500.0);
        Assert.assertEquals(detailsMap.get(CDCSourceConstants.MONGO_COLLECTION_ID), OBJECT_ID);
    }

    @Test
    public void deleteEventCapturesTheDocumentId() {
        Map<String, Object> detailsMap = capture(CDCSourceConstants.DELETE)
                .createMap(record(value(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION)),
                        CDCSourceConstants.DELETE);

        Assert.assertEquals(detailsMap.get(CDCSourceConstants.MONGO_COLLECTION_ID), OBJECT_ID);
    }

    /**
     * {@code ChangeDataCapture.handleEvent} casts the transport properties entry to a {@link List}. Storing anything
     * else there fails with a {@link ClassCastException} once the event reaches the source event listener.
     */
    @Test
    public void transportPropertiesAreAListForEveryOperation() {
        Struct insert = value(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION);
        insert.put(Envelope.FieldName.AFTER, "{\"name\": \"sweets\"}");
        Struct update = value(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION);
        update.put(MongoDbFieldName.UPDATE_DESCRIPTION, updateDescription("{\"amount\": 500.0}"));
        Struct delete = value(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION);

        assertTransportProperties(capture(CDCSourceConstants.INSERT)
                .createMap(record(insert), CDCSourceConstants.INSERT), CDCSourceConstants.INSERT);
        assertTransportProperties(capture(CDCSourceConstants.UPDATE)
                .createMap(record(update), CDCSourceConstants.UPDATE), CDCSourceConstants.UPDATE);
        assertTransportProperties(capture(CDCSourceConstants.DELETE)
                .createMap(record(delete), CDCSourceConstants.DELETE), CDCSourceConstants.DELETE);
    }

    private void assertTransportProperties(Map<String, Object> detailsMap, String expectedOperation) {
        Object transportProperties = detailsMap.get(CDCSourceConstants.TRANSPORT_PROPERTIES);
        Assert.assertTrue(transportProperties instanceof List,
                "Transport properties must be a List for " + expectedOperation + " but was " + transportProperties);
        Assert.assertEquals(((List) transportProperties).get(0), expectedOperation);
    }

    /**
     * A document keyed by something other than an ObjectId serialises to a bare JSON scalar rather than an
     * {@code {"$oid": ...}} wrapper, and must not blow up the engine.
     */
    @Test
    public void deleteEventHandlesNonObjectIdKeys() {
        Struct key = new Struct(KEY_SCHEMA);
        key.put(CDCSourceConstants.MONGO_COLLECTION_ID, "\"e001\"");
        SourceRecord sourceRecord = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), TOPIC,
                KEY_SCHEMA, key, VALUE_SCHEMA, value(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION));

        Map<String, Object> detailsMap = capture(CDCSourceConstants.DELETE)
                .createMap(sourceRecord, CDCSourceConstants.DELETE);

        Assert.assertEquals(detailsMap.get(CDCSourceConstants.MONGO_COLLECTION_ID), "e001");
    }

    @Test
    public void eventsNotMatchingTheConfiguredOperationAreIgnored() {
        Struct value = value(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION);
        value.put(MongoDbFieldName.UPDATE_DESCRIPTION, updateDescription("{\"amount\": 500.0}"));

        Assert.assertTrue(capture(CDCSourceConstants.INSERT)
                .createMap(record(value), CDCSourceConstants.INSERT).isEmpty());
    }

    @Test
    public void snapshotReadEventsAreIgnored() {
        Struct value = value(CDCSourceConstants.CONNECT_RECORD_INITIAL_SYNC);
        value.put(Envelope.FieldName.AFTER, "{\"name\": \"sweets\"}");

        Assert.assertTrue(capture(CDCSourceConstants.INSERT)
                .createMap(record(value), CDCSourceConstants.INSERT).isEmpty());
    }
}
