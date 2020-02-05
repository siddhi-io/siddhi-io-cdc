package io.siddhi.extension.io.cdc.source.listening;

import io.debezium.data.VariableScaleDecimal;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.cdc.util.CDCSourceConstants;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is for capturing change data for RDBMS using debezium embedded engine.
 **/
public class RdbmsChangeDataCapture extends ChangeDataCapture {

    public RdbmsChangeDataCapture(String operation, SourceEventListener sourceEventListener) {
        super(operation, sourceEventListener);
    }

    Map<String, Object> createMap(ConnectRecord connectRecord, String operation) {

        //Map to return
        Map<String, Object> detailsMap = new HashMap<>();

        Struct record = (Struct) connectRecord.value();

        //get the change data object's operation.
        String op;

        try {
            op = (String) record.get(CDCSourceConstants.CONNECT_RECORD_OPERATION);
        } catch (NullPointerException | DataException ex) {
            return detailsMap;
        }

        //match the change data's operation with user specifying operation and proceed.
        if (operation.equalsIgnoreCase(CDCSourceConstants.INSERT) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION)
                || operation.equalsIgnoreCase(CDCSourceConstants.DELETE) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION)
                || operation.equalsIgnoreCase(CDCSourceConstants.UPDATE) &&
                op.equals(CDCSourceConstants.CONNECT_RECORD_UPDATE_OPERATION)) {

            Struct rawDetails;
            List<Field> fields;
            String fieldName;

            switch (op) {
                case CDCSourceConstants.CONNECT_RECORD_INSERT_OPERATION:
                    //append row details after insert.
                    rawDetails = (Struct) record.get(CDCSourceConstants.AFTER);
                    fields = rawDetails.schema().fields();
                    for (Field key : fields) {
                        fieldName = key.name();
                        detailsMap.put(fieldName, getValue(rawDetails.get(fieldName)));
                    }
                    break;
                case CDCSourceConstants.CONNECT_RECORD_DELETE_OPERATION:
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
