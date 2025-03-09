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

package io.siddhi.extension.io.cdc.source.listening;

import io.debezium.data.VariableScaleDecimal;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.cdc.source.metrics.ListeningMetrics;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is for capturing change data for RDBMS using debezium embedded engine.
 **/
public class RdbmsChangeDataCapture extends ChangeDataCapture {
    private static final Logger log = LogManager.getLogger(RdbmsChangeDataCapture.class);

    public RdbmsChangeDataCapture(String operation, SourceEventListener sourceEventListener, ListeningMetrics metrics) {
        super(operation, sourceEventListener, metrics);
    }

    Map<String, Object> createMap(ConnectRecord connectRecord, String operation) {
        //Map to return
        Map<String, Object> detailsMap = new HashMap<>();
        List<Object> transportProperties = new ArrayList();
        Struct record = (Struct) connectRecord.value();
        //get the change data object's operation.
        String op;
        try {
            op = (String) record.get(CDCSourceConstants.CONNECT_RECORD_OPERATION);
        } catch (NullPointerException | DataException ex) {
            return detailsMap;
        }
        //match the change data's operation with user specifying operation and proceed.
        Struct rawDetails;
        List<Field> fields;
        String fieldName;
        if (operation.contains(",")) {
            List<String> operationList = createOperationList(operation);

            switch (op) {
                case CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION:
                    if (operationList.contains(CDCSourceConstants.INSERT)) {
                        transportProperties.add(CDCSourceConstants.INSERT);
                        transportProperties.add(record.getStruct(CDCSourceConstants.SOURCE_SCHEMA)
                                .get(CDCSourceConstants.EVENT_TIMESTAMP));
                        transportProperties.add(record.get(CDCSourceConstants.EVENT_TIMESTAMP));
                        detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
                        rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                        fields = rawDetails.schema().fields();
                        for (Field key : fields) {
                            fieldName = key.name();
                            detailsMap.put(fieldName, getValue(rawDetails.get(fieldName)));
                            detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName, getDefaultValue(key.schema()));
                        }
                        break;
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION:
                    if (operationList.contains(CDCSourceConstants.UPDATE)) {
                        transportProperties.add(CDCSourceConstants.UPDATE);
                        transportProperties.add(record.getStruct(CDCSourceConstants.SOURCE_SCHEMA)
                                .get(CDCSourceConstants.EVENT_TIMESTAMP));
                        transportProperties.add(record.get(CDCSourceConstants.EVENT_TIMESTAMP));
                        detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
                        rawDetails = (Struct) record.get(CDCSourceConstants.BEFORE);
                        fields = rawDetails.schema().fields();
                        for (Field key : fields) {
                            fieldName = key.name();
                            detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName,
                                    getValue(rawDetails.get(fieldName)));
                        }
                        //append row details after update.
                        rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                        fields = rawDetails.schema().fields();
                        for (Field key : fields) {
                            fieldName = key.name();
                            detailsMap.put(fieldName, getValue(rawDetails.get(fieldName)));
                        }
                        break;
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION:
                    if (operationList.contains(CDCSourceConstants.DELETE)) {
                        transportProperties.add(CDCSourceConstants.DELETE);
                        transportProperties.add(record.getStruct(CDCSourceConstants.SOURCE_SCHEMA)
                                .get(CDCSourceConstants.EVENT_TIMESTAMP));
                        transportProperties.add(record.get(CDCSourceConstants.EVENT_TIMESTAMP));
                        detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
                        rawDetails = (Struct) record.get(CDCSourceConstants.BEFORE);
                        fields = rawDetails.schema().fields();
                        for (Field key : fields) {
                            fieldName = key.name();
                            detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName,
                                    getValue(rawDetails.get(fieldName)));
                            detailsMap.put(fieldName, getDefaultValue(key.schema()));
                        }
                        break;
                    }
                    break;
                default:
                    log.info("Provided value for \"op\" : {} is not supported.", op);
                    break;
            }
        } else if (operation.equalsIgnoreCase(CDCSourceConstants.INSERT) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION)
                || operation.equalsIgnoreCase(CDCSourceConstants.DELETE) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION)
                || operation.equalsIgnoreCase(CDCSourceConstants.UPDATE) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION)) {

            switch (op) {
                case CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION:
                    transportProperties.add(CDCSourceConstants.INSERT);
                    transportProperties.add(record.getStruct(CDCSourceConstants.SOURCE_SCHEMA)
                            .get(CDCSourceConstants.EVENT_TIMESTAMP));
                    transportProperties.add(record.get(CDCSourceConstants.EVENT_TIMESTAMP));
                    detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
                    //append row details after insert.
                    rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(fieldName, getValue(rawDetails.get(fieldName)));
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION:
                    transportProperties.add(CDCSourceConstants.DELETE);
                    transportProperties.add(record.getStruct(CDCSourceConstants.SOURCE_SCHEMA)
                            .get(CDCSourceConstants.EVENT_TIMESTAMP));
                    transportProperties.add(record.get(CDCSourceConstants.EVENT_TIMESTAMP));
                    detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
                    //append row details before delete.
                    rawDetails = (Struct) record.get(CDCSourceConstants.BEFORE);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName,
                                getValue(rawDetails.get(fieldName)));
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION:
                    transportProperties.add(CDCSourceConstants.UPDATE);
                    transportProperties.add(record.getStruct(CDCSourceConstants.SOURCE_SCHEMA)
                            .get(CDCSourceConstants.EVENT_TIMESTAMP));
                    transportProperties.add(record.get(CDCSourceConstants.EVENT_TIMESTAMP));
                    detailsMap.put(CDCSourceConstants.TRANSPORT_PROPERTIES, transportProperties);
                    //append row details before update.
                    rawDetails = (Struct) record.get(CDCSourceConstants.BEFORE);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(CDCSourceConstants.BEFORE_PREFIX + fieldName,
                                getValue(rawDetails.get(fieldName)));
                    }
                    //append row details after update.
                    rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(fieldName, getValue(rawDetails.get(fieldName)));
                    }
                    break;
                default:
                    log.warn("Provided value for \"op\" : {} is not supported.", op);
                    break;
            }
        }
        return detailsMap;
    }

    private Object getValue(Object v) {
        if (v instanceof Struct) {
            Optional<BigDecimal> value = VariableScaleDecimal.toLogical((Struct) v).getDecimalValue();
            BigDecimal bigDecimal = value.orElse(null);
            if (bigDecimal == null) {
                return null;
            }
            return bigDecimal.longValue();
        }
        if (v instanceof Short) {
            return ((Short) v).intValue();
        }
        if (v instanceof Byte) {
            return ((Byte) v).intValue();
        }
        return v;
    }
}
