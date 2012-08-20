hadoop fs -rm -r richinput
hadoop fs -put src/test/resource/richinput/ .

# create the target table
echo "disable 'tab_hash_dot'" | hbase shell
echo "drop 'tab_hash_dot'" | hbase shell
echo "create 'tab_hash_dot', 'cf'" | hbase shell

# run the application
hadoop jar RichImportTsv-1.0-SNAPSHOT.jar pl.edu.icm.coansys.richimporttsv.jobs.mapreduce.RichImportTsv -libjars RichImportTsv-1.0-SNAPSHOT.jar -Dimporttsv.record.separator=# -Dimporttsv.separator=. -Dimporttsv.columns=HBASE_ROW_KEY,cf:cq tab_hash_dot richinput/hash_dot.dat

# examine the results
echo "scan 'tab_hash_dot'" | hbase shell
hadoop fs -cat richinput/hash_dot.dat
