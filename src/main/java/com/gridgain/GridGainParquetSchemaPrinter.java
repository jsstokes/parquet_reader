package com.gridgain;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

public class GridGainParquetSchemaPrinter {

    private String pathAndFileName_;

    public GridGainParquetSchemaPrinter(String pathAndFileName) {
        pathAndFileName_ = pathAndFileName;
    }

    public void printSchema() {
        Path file = new Path(pathAndFileName_);
        GenericRecord record;
        try {
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
            if ((record = reader.read()) != null) {
                System.out.println(record.getSchema());
            } else {
                System.out.println("Error! Unable to obtain schema from the designated file");
            }
            reader.close();
        } catch (IOException ioe) {
            System.out.println("Error! IOException caught while trying to extract schema from the following file: " + pathAndFileName_);
        }
    }

}
