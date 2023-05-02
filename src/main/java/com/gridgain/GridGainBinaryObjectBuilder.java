package com.gridgain;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.util.List;

public class GridGainBinaryObjectBuilder extends PartitionSpecParent {

    public GridGainBinaryObjectBuilder(String partitionSpec1, String partitionSpec2, String partitionSpec3) {
        super(partitionSpec1, partitionSpec2, partitionSpec3);
    }

    public BinaryObject process(BinaryObjectBuilder builder, GenericRecord record) {
        addValuesFor(builder, record);
        return builder.build();
    }

    private void addValuesFor(BinaryObjectBuilder builder, GenericRecord record) {
        builder.setField("pk", java.util.UUID.randomUUID());
        List<Schema.Field> fields = record.getSchema().getFields();
        int fieldCount = fields.size();
        for(int i = 0; i < fieldCount; ++i) {
            Schema.Field currentField = fields.get(i);
            if(SchemaTypeHelper.shouldProcessField(currentField)) {
                Object value = SchemaTypeHelper.maybeConvert(currentField, record.get(i));
                builder.setField(currentField.name(), value);
            }
        }
        super.maybeAddSpec1(builder, record);
        super.maybeAddSpec2(builder, record);
        super.maybeAddSpec3(builder, record);
    }

}
