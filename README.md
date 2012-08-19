RichImportTsv
=============

## About
RichImportTsv is build on top of ImportTsv and loads data into HBase. 

It enhances the usage of ImportTsv and allows you to load data where:
* fields are separated by multi-character separator,
* records are separated by any separators (not only new line as it is hard-coded in ImportTsv). A non-default record speparator can be specified using -Dimporttsv.record.separator=separator. 

RichImportTsv internally uses SeparatorInputFormat (can be changed using -Dimporttsv.input.format.class=input_format_class).

In order to avoid confusion (so that similarly to ImportTsv), RichImportTsv interprets configuration options that start from 'importtsv.'.

## Quick Start

This example will load data where records are separated by "#" and fields (within a record) are separated by ".".

### Data preparation
Some sample data can be taken from src/test/resource/richinput.
```
# put input data to HDFS
$ hadoop fs -put src/test/resource/richinput/ .

# download the jar
$ wget -O RichImportTsv-1.0-SNAPSHOT.jar 'https://github.com/kawaa/RichImportTsv/raw/master/RichImportTsv-1.0-SNAPSHOT.jar'
```

### Load data via Puts (i.e. non-bulk loading):

#### Example 1
```
# (optional) familiarize with input file
$ hadoop fs -cat richinput/hash_dot.dat

KEY1.VALUE1#KEY2.VALUE2#KEY3.VALUE3a
VALUE3b#KEY4.VALUE4
# create the target table
echo "create 'tab_hash_dot', 'cf'" | hbase shell
# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=# -Dimporttsv.separator=. -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq tab_hash_dot richinput/hash_dot.dat
# examine the results
echo "scan 'tab_hash_dot'" | hbase shell
```

#### Example 2
```
# (optional) familiarize with input file
$ hadoop fs -cat richinput/hash3_dot3.dat

KEY1...VALUE1###KEY2...VALUE2###KEY3...VALUE3a
VALUE3b###KEY4...VALUE4
# create the target table
echo "create 'tab_hash3_dot3', 'cf'" | hbase shell
# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=### -Dimporttsv.separator=... -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq tab_hash3_dot3 richinput/hash3_dot3.dat
# examine the results (should be the same as in the previous example)
echo "scan 'tab_hash3_dot3'" | hbase shell
```

### Generate StoreFiles for bulk-loading:
Use -Dimporttsv.bulk.output=output_dir option.
```
# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=# -Dimporttsv.separator=. -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq -Dimporttsv.bulk.output=richoutput tab richinput/hash_dot.dat
# scan the results
hadoop fs -ls richoutput/cf/
# use the listed file with -f parameter
hbase org.apache.hadoop.hbase.io.hfile.HFile -v -p -f richoutput/cf/<SUFIX>
```

### SeparatorInputFormat

RichImportTsv internally uses SeparatorInputFormat in order to read records separated by any separator (not only new line as TextInputFormat does). It is based on implementation code and description presented at http://blog.rguha.net/?p=293. We extended the code by adding parameter (i.e. record.separator) to specify a separator and caluclating the progress of reading the input.

### Tests

I use HBaseTestingUtility to test RichImportTsv. I discovered that it works better if all Hadoop/HBase deamons are stopped before running the "local" tests.

```
mvn test
```
