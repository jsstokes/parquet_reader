package com.gridgain;

import java.io.IOException;

public class GridGainParquetOperations {

    private static String PRINT_SCHEMA = "-print_schema";
    private static String CREATE_TABLE = "-create_table";
    private static String INGEST_DATA = "-ingest_data";

    public static void main(String[] args) throws IOException {
        if(args.length < 1 || args.length > 6) {
            System.out.println("Invalid invocation!");
            displayUsage();
        } else {
            if(PRINT_SCHEMA.equals(args[0]) && args.length == 2) {
                GridGainParquetSchemaPrinter printer = new GridGainParquetSchemaPrinter(args[1]);
                printer.printSchema();
            } else if(CREATE_TABLE.equals(args[0]) && args.length >= 3) {
                String partitionSpec3 = null;
                String partitionSpec2 = null;
                String partitionSpec1 = null;
                if(args.length == 6) {
                    partitionSpec3 = args[5];
                }
                if(args.length >= 5) {
                    partitionSpec2 = args[4];
                }
                if(args.length >= 4) {
                    partitionSpec1 = args[3];
                }
                GridGainParquetCreateTablePrinter creator = new GridGainParquetCreateTablePrinter(args[1], args[2], partitionSpec1, partitionSpec2, partitionSpec3);
                creator.printCreateTableStatement();
            } else if(INGEST_DATA.equals(args[0]) && args.length >= 3) {
                String partitionSpec3 = null;
                String partitionSpec2 = null;
                String partitionSpec1 = null;
                if(args.length == 6) {
                    partitionSpec3 = args[5];
                }
                if(args.length >= 5) {
                    partitionSpec2 = args[4];
                }
                if(args.length >= 4) {
                    partitionSpec1 = args[3];
                }
                GridGainParquetBinaryObjectStreamer loader = new GridGainParquetBinaryObjectStreamer(args[1], args[2], partitionSpec1, partitionSpec2, partitionSpec3);
                loader.loadParquetData();
            }
        }
    }

    private static void displayUsage() {
        System.out.println("Example usages");
        System.out.println("  *********************************************************************************************************");
        System.out.println("  To print the schema from a parquet file invoke as follows:");
        System.out.println("  GridGainParquetOperations -print_schema pathAndFileName");
        System.out.println("  GridGainParquetOperations -print_schema /mnt/data/some_file_name");
        System.out.println("  *********************************************************************************************************");
        System.out.println("  To print the SQL table creation statement for a parquet file invoke as follows:");
        System.out.println("  GridGainParquetOperations -create_table pathAndFileName schema_name.table_name [partitionSpec1] [partitionSpec2] [partitionSpec3]");
        System.out.println("  GridGainParquetOperations -create_table /mnt/data/some_file_name PUBLIC.TABLE_X year=2019 month=01 day=02");
        System.out.println("  OR");
        System.out.println("  GridGainParquetOperations -create_table /mnt/data/some_file_name PUBLIC.TABLE_X year=2019 month=01");
        System.out.println("  OR");
        System.out.println("  GridGainParquetOperations -create_table /mnt/data/some_file_name PUBLIC.TABLE_X year=2019");
        System.out.println("  OR");
        System.out.println("  GridGainParquetOperations -create_table /mnt/data/some_file_name PUBLIC.TABLE_X");
        System.out.println("  *********************************************************************************************************");
        System.out.println("  To ingest data from a parquet files invoke as follows:");
        System.out.println("  GridGainParquetOperations -ingest_data pathAndFileName schema_name.table_name [partitionSpec1] [partitionSpec2] [partitionSpec3]");
        System.out.println("  GridGainParquetOperations -ingest_data /mnt/data/some_file_name PUBLIC.TABLE_X year=2019 month=01 day=02");
        System.out.println("  OR");
        System.out.println("  GridGainParquetOperations -ingest_data /mnt/data/some_file_name PUBLIC.TABLE_X year=2019 month=01");
        System.out.println("  OR");
        System.out.println("  GridGainParquetOperations -ingest_data /mnt/data/some_file_name PUBLIC.TABLE_X year=2019");
        System.out.println("  OR");
        System.out.println("  GridGainParquetOperations -ingest_data /mnt/data/some_file_name PUBLIC.TABLE_X");
        System.out.println("  *********************************************************************************************************");
    }

    /**
     * Test scenario 1:
     *   3 Node small cluster deployed to Amazon East (Virginia)
     *   JDBC Thin client running on my laptop inserting ~7 million rows to the above cluster
     *   Most batches were 67 rows due to 10000 character limitation for SQL in GrinGain
     *   New SQL statement was created for each batch.
     *   Execution duration = 11192455 milli-seconds or 3.1 hours
     *   Cluster count = 7019375
     *
     * Test scenario 2:
     *   Distributed compute job using affinity inserted ~7 million rows (7019375) in
     *   25 minutes into the above cluster.
     *
     * Test scenario 3:
     *   Distributed compute job using affinity inserted 10000 rows
     *   Read time:  15015 ms, 15 seconds
     *   Write time: 67022 ms, 67 seconds
     *   Total time: 82038 ms, 82 seconds
     *
     * Test scenario 4:
     *   Distributed compute job using affinity inserted ~7 million rows (7019375) in
     *   25 minutes 33 seconds into the above cluster. The second execution affirms that
     *   the execution time is reasonably consistent.
     *
     * Test scenario 5:
     *   Ignite data Streamer inserted ~7 million rows (7019375) in
     *   25 minutes 56 seconds into the above cluster.
     *
     */

}
