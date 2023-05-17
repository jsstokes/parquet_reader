package com.gridgain;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.UUID;

public class GridGainParquetBinaryObjectStreamer {

    private String pathAndFileName_;
    private String tableName_;
    private GridGainBinaryObjectBuilder builder_;

    public GridGainParquetBinaryObjectStreamer(String pathAndFileName, String tableName, String partitionSpec1, String partitionSpec2, String partitionSpec3) {
        pathAndFileName_ = pathAndFileName;
        tableName_ = tableName;
        builder_ = new GridGainBinaryObjectBuilder(partitionSpec1, partitionSpec2, partitionSpec3);
    }

    public void loadParquetData() {
        Path file = new Path(pathAndFileName_);
        long recordCount = 0;
        GenericRecord record = null;
        try {
            long appTimeStart = System.currentTimeMillis();
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
            ClusterClient clusterClient = new ClusterClient();
            Ignite cluster = clusterClient.startClient();
            try (IgniteDataStreamer<UUID, BinaryObject> streamer = cluster.dataStreamer(tableName_)) {
                while ((record = reader.read()) != null) {
                    ++recordCount;
                    String tableOnly = tableName_.split("[.]")[1];
                    BinaryObjectBuilder bob = cluster.binary().builder(tableOnly);
                    BinaryObject bo = builder_.process(bob, record);
                    UUID pk = bo.field("pk");
                    streamer.addData(pk, bo);
                }
            }
            reader.close();
            cluster.close();
            long appTimeEnd = System.currentTimeMillis();
            System.out.println("Total process time: " + (appTimeEnd - appTimeStart));
        } catch (IOException ioe) {
            System.out.println("IOException caught while processing the following file: " + pathAndFileName_);
        }
    }

}
