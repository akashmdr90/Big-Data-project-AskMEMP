cd $HADOOP_PREFIX
bin/hadoop fs -rmr /final/*.csv
bin/hadoop fs -copyFromLocal ~/*.csv hdfs://192.168.0.33:54310/final/
