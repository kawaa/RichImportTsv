RichImportTsv
=============

## About
RichImportTsv is build on top of ImportTsv and loads data into HBase. 
It enhances the usage of ImportTsv and allows you to load data where records are separated by any separators (not only new line as it is hard-coded in ImportTsv). A non-default record speparator can be specified using -Dimporttsv.record.separator=<separator>. RichImportTsv internally uses SeparatorInputFormat (can be changed using -Dimporttsv.input.format.class=<input_format_class>).

## Quick Start

This example will load data where records are separated by "#", while fields (within a record) are separated by ".".

### Data preparation
```
# create some input data
mkdir richinput
echo "KEY1.VALUE#KEY2.VALUE2#KEY3.VALUE3a" > richinput/hash_dot.dat
echo "VALUE3b#KEY4.VALUE4" >> richinput/hash_dot.dat 
# put input data to HDFS
hadoop fs -put richinput .

# download jar
wget https://github.com/kawaa/RichImportTsv/blob/master/RichImportTsv-1.0-SNAPSHOT.jar
```

### Load data via Puts (i.e. non-bulk loading):
```
# create the target table
echo "create 'tab', 'cf'" | hbase shell
# run the application
hadoop jar RichImportTSV-1.0-SNAPSHOT.jar pl.ceon.research.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTSV-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=# -Dimporttsv.separator=. -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq tab richinput/hash_dot.dat
# scan the table
echo "scan 'tab'" | hbase shell
```
