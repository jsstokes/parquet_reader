package com.gridgain;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.List;

public class GridGainParquetCreateTablePrinter extends PartitionSpecParent {

    private String pathAndFileName_;
    private String tableName_;


    public GridGainParquetCreateTablePrinter(String pathAndFileName, String tableName, String partitionSpec1, String partitionSpec2, String partitionSpec3) {
        super(partitionSpec1, partitionSpec2, partitionSpec3);
        pathAndFileName_ = pathAndFileName;
        tableName_ = tableName;
    }

    public void printCreateTableStatement() {
        Path file = new Path(pathAndFileName_);
        GenericRecord record;
        try {
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
            if ((record = reader.read()) != null) {
                StringBuilder builder = new StringBuilder();
                builder.append("CREATE TABLE IF NOT EXISTS ");
                builder.append(tableName_.split("[.]")[1]);
                builder.append(" (\n  pk uuid,\n");
                List<Schema.Field> fields = record.getSchema().getFields();
                int fieldCount = fields.size();
                for(int i = 0; i < fieldCount; ++i) {
                    Schema.Field currentField = fields.get(i);
                    if(SchemaTypeHelper.shouldProcessField(currentField)) {
                        String fieldStatement = SchemaTypeHelper.maybeCreateField(currentField);
                        if(null != fieldStatement) {
                            builder.append("  ");
                            builder.append(fieldStatement);
                        }
                        if((i + 1) < fieldCount) {
                            builder.append(",\n");
                        }
                    }
                }
                super.maybeAddSpec1(builder);
                super.maybeAddSpec2(builder);
                super.maybeAddSpec3(builder);
                builder.append(",\n  PRIMARY KEY (pk)\n) WITH \"CACHE_NAME=");
                builder.append(tableName_);
                builder.append(",KEY_TYPE=java.util.UUID,VALUE_TYPE=");
                builder.append(tableName_.split("[.]")[1]);
                builder.append("\";");
                System.out.print(builder.toString());
            } else {
                System.out.println("Error! Unable to obtain schema from the designated file");
            }
            reader.close();
        } catch (IOException ioe) {
            System.out.println("Error! IOException caught while trying to extract schema from the following file: " + pathAndFileName_);
        }
    }

}
