# GridGain Parquet Data Reader & Loader

This application is a basic Java application that is intended to read arbitrary data<br/>
files in parquet format and store the associated data into a target GridGain cluster.

## Core Capabilities
1. Read and print the schema resident in a target parquet file 
2. Read and print a create table statement that would support ingesting data for a target parquet file
3. Ingest the data for a target parquet file and store data in a target cluster cache / table

## Parquet Details
- Parquet files contain the schema for the data columns that are in the file
- Parquet files are very often stored in partitioned data sets
- Parquet files do not hold the partition column names or values
- Most parquet files do not have a column that servers as a primary key for the file
- Parquet supports nested data types such as list (array), map, struct (object) 

## Implementation Details
- To address the absence of primary keys a new field named "pk" (type=UUID) is added as the first column
- The application supports up to three optional partition columns
- Partition columns are specified as column_name=column_value
- Note that partitions specs do not have any spaces
- Example partition specification: year=2023 or month=02
- It is up to the user to provide the proper partition specs for each application invocation
- Only primitive types are supported (Boolean, Bytes, Double, Enum, Float, Int, Long, String)
- All byte arrays are converted to String (varchar)
- All Float fields are explicitly converted to double to support GridGain SQL operators such as min, max, ...
- Nested types are not supported. 
- All nested types will simply be skipped

## Build Details
This Java application was build using:<br/>
Maven version 3.8.7<br/>
Java version 1.8.0_351<br/>
GridGain version 8.8.26<br/>
<br/>
Check out the application from git.<br/>
Navigate to the resources directory.<br/>
Open the client_config.xml file in an editor.<br/>
Search for "USER_ENTRY_REQUIRED".<br/>
Update the addresses, user name and password values for your target cluster.<br/>
Navigate to the top level application directory and execute:<br/>
<br/>
mvn clean compile assembly:single<br/>
<br/>
As expected the application jar file will be built in the ./target subdirectory

## Execution Details
### Display Help Page
Execute the application with no arguments to display help page:<br/>
java -jar parquet-reader-1.0-SNAPSHOT.jar

### Print File Schema
Execute the application with with the -print_schema argument as follows:<br/>
java -jar parquet-reader-1.0-SNAPSHOT.jar -print_schema /target/path/and/file_name

### Print Create Table Statement
Execute the application with with the -create_table argument<br/>
Note be sure to double quote the path and file name argument should it have any spaces<br/>
Note that the table_name argument must be specified as SCHEMA_NAME.TABLE_NAME (i.e. PUBLIC.TABLE_X).<br/>
Note that partition_spec1, partition_spec2, partition_spec3 are all optional arguments<br/>
Note that any partition columns MUST be specified in the create table statement of ingesting data will NOT support any partitions!<br/>
java -jar parquet-reader-1.0-SNAPSHOT.jar -create_table /target/path/and/file_name table_name partition_spec1 partition_spec2 partition_spec3

### Ingest File Data
Execute the application with with the -ingest_data argument<br/>
Note be sure to double quote the path and file name argument should it have any spaces<br/>
Note that the table_name argument must be specified as SCHEMA_NAME.TABLE_NAME (i.e. PUBLIC.TABLE_X).<br/>
Note that partition_spec1, partition_spec2, partition_spec3 are all optional arguments<br/>
Note that the partition column names MUST be the same column names used in the create statement invocation above<br/>
Note that the partition column values can be different for each file<br/>
java -jar parquet-reader-1.0-SNAPSHOT.jar -ingest_data /target/path/and/file_name table_name partition_spec1 partition_spec2 partition_spec3
