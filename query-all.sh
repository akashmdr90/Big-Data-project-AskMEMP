echo "Searching for " $1
/usr/local/spark-1.6.1-bin-hadoop1/bin/spark-submit --master spark://master:7077 query-all.py $1
