import sys
import numpy as np
from pyspark import SparkContext
from amazonproduct import API
api = API(locale='us')
def parseVector(line):
    parts = line.split('+')
    return parts[0],(parts[1],parts[2],parts[4],parts[3])

sc = SparkContext(appName="AskMeMPQuery")
amazon_rdd = sc.textFile('hdfs://192.168.0.33:54310/final/amazon.txt')
walmart_rdd = sc.textFile('hdfs://192.168.0.33:54310/final/walmart.txt')
ebay_rdd = sc.textFile('hdfs://192.168.0.33:54310/final/ebay.txt')
for book in api.item_search('Books', Keywords=sys.argv[1], Condition='New', Availability='Available'):
#    print '%s %s' %(book.ASIN,book.ItemAttributes.Title)
    amazon_data = amazon_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
    if amazon_data.count()>0:
        for x in amazon_data.collect():
            print x
        walmart_data = walmart_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
        for x in walmart_data.collect():
            print x
        ebay_data = ebay_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
        for x in ebay_data.collect():
            print x
sc.stop()
