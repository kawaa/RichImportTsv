RichImportTsv
=============

## About
RichImportTsv is build on top of ImportTsv and loads data into HBase. 

It enhances the usage of ImportTsv and allows you to load data where records are separated by any separators (not only new line as it is hard-coded in ImportTsv). A non-default record speparator can be specified using -Dimporttsv.record.separator=separator. 

RichImportTsv internally uses SeparatorInputFormat (can be changed using -Dimporttsv.input.format.class=input_format_class).

## Quick Start

This example will load data where records are separated by "#" and fields (within a record) are separated by ".".

### Data preparation
```
# create some input data
mkdir richinput
echo "KEY1.VALUE#KEY2.VALUE2#KEY3.VALUE3a" > richinput/hash_dot.dat
echo "VALUE3b#KEY4.VALUE4" >> richinput/hash_dot.dat 
# put input data to HDFS
hadoop fs -put richinput .

# download the jar
wget -O RichImportTsv-1.0-SNAPSHOT.jar 'https://github.com/kawaa/RichImportTsv/blob/master/RichImportTsv-1.0-SNAPSHOT.jar?raw=true'
```

### Load data via Puts (i.e. non-bulk loading):
```
# create the target table
echo "create 'tab', 'cf'" | hbase shell
# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=# -Dimporttsv.separator=. -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq tab richinput/hash_dot.dat
# scan the results
echo "scan 'tab'" | hbase shell
```

### Generate StoreFiles for bulk-loading:
Use -Dimporttsv.bulk.output=output_dir option.
```
# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=# -Dimporttsv.separator=. -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq -Dimporttsv.bulk.output=richoutput tab richinput/hash_dot.dat
# scan the results
hadoop fs -ls richoutput/cf/
hbase org.apache.hadoop.hbase.io.hfile.HFile -v -p -f richoutput/cf/a3caf62794f44eb6b3d99c083faa65da
```

### SeparatorInputFormat

RichImportTsv internally uses SeparatorInputFormat in order to read records separated by any separator (not only new line as TextInputFormat does). It is based on implementation code and description presented at http://blog.rguha.net/?p=293. We extended the code by adding parameter (i.e. record.separator) to specify a separator and caluclating the progress of reading the input.

```
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.TextLiner -libjars RichImportTsv-1.0-SNAPSHOT.jar,/usr/lib/hbase/hbase.jar -Drecord.separator=# richinput/hash_dot.dat textliner-output/
hadoop fs -cat textliner-output/part-m-00000
```

### Tests

Run tests by typing:

```
mvn test
```
