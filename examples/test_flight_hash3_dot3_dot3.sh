hadoop fs -rm -r richinput
hadoop fs -put src/test/resource/richinput/ .

# create the target table

echo "disable 'tab_hash3_dot3_dot3'" | hbase shell
echo "drop 'tab_hash3_dot3_dot3'" | hbase shell
echo "create 'tab_hash3_dot3_dot3', 'cf'" | hbase shell

# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=### -Dimporttsv.separator=... -Dimporttsv.columns=HBASE_ROW_KEY,cf:cqA,cf:cqB tab_hash3_dot3_dot3 richinput/hash3_dot3_dot3.dat

# examine the results
echo "scan 'tab_hash3_dot3_dot3'" | hbase shell
hadoop fs -cat richinput/hash3_dot3_dot3.dat
