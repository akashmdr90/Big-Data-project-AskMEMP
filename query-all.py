import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import numpy as np
from pyspark import SparkContext
from amazonproduct import API
import time

api = API(locale='us')

def parseVector(line):
    parts = line.split('+')
    return parts[0],(parts[1],parts[2],parts[4],parts[3])

pre = time.time()
sc = SparkContext(appName="AskMeMPQuery")
amazon_rdd = sc.textFile('hdfs://192.168.0.33:54310/final/amazon.csv')
walmart_rdd = sc.textFile('hdfs://192.168.0.33:54310/final/walmart.csv')
ebay_rdd = sc.textFile('hdfs://192.168.0.33:54310/final/ebay.csv')
all_rdd = amazon_rdd.join(walmart_rdd).join(ebay_rdd)

start = time.time()
query = str(sys.argv[1])
#print '%s %s' %(book.ASIN,book.ItemAttributes.Title)
all_data = all_rdd.filter(lambda x : query in x)
all_data = all_data.map(parseVector).cache()
all_data = all_data.reduceByKey(lambda a,b:a).cache()

end = time.time()
all_data_result = all_data.collect()
'''
for x in range(0,len(amazon_result)):
    upc,[a,b,c] = all_data_result[x]
    (title, author, price, url) = a
    print '+++' + str(upc)
    print '***"%s" by "%s"' % (title, author);
    print '\tAmazon\t$%.2f\t%s' % (float(price)/100, url)
    (title, author, price, url) = b
    print '\tWalmart\t$%.2f\t%s' % (float(price)/100, url)
    (title, author, price, url) = c
    print '\tEbay\t$%.2f\t%s' % (float(price)/100, url) 
'''
sc.stop()

print 'Load time: %.5f seconds' % (start - pre)
print 'Search time: %.5f seconds' % (end - start)
 
