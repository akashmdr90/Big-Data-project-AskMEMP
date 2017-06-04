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


start = time.time()
query = str(sys.argv[1])
#print '%s %s' %(book.ASIN,book.ItemAttributes.Title)
amazon_data = amazon_rdd.filter(lambda x : query in x)
amazon_data = amazon_data.map(parseVector).cache()
amazon_data = amazon_data.reduceByKey(lambda a,b:a).cache()
amazon_result = amazon_data.collect()

walmart_data = walmart_rdd.filter(lambda x : query in x)
walmart_data = walmart_data.map(parseVector).cache()
walmart_data = walmart_data.reduceByKey(lambda a,b:a).cache()
walmart_result = walmart_data.collect()

ebay_data = ebay_rdd.filter(lambda x : query in x)
ebay_data = ebay_data.map(parseVector).cache()
ebay_data = ebay_data.reduceByKey(lambda a,b:a).cache()
ebay_result = ebay_data.collect()
end = time.time()
for x in range(0,len(amazon_result)):
    upc, (title, author, price, url) = amazon_result[x]
    print '+++' + str(upc)
    print '***"%s" by "%s"' % (title, author);
    print '\tAmazon\t$%.2f\t%s' % (float(price)/100, url)
    upc, (title, author, price, url) = walmart_result[x]
    print '\tWalmart\t$%.2f\t%s' % (float(price)/100, url)
    upc, (title, author, price, url) = ebay_result[x]
    print '\tEbay\t$%.2f\t%s' % (float(price)/100, url) 
'''
amazon_data = amazon_rdd.filter(lambda x : query in x)
amazon_data = amazon_data.map(parseVector).cache()
amazon_data = amazon_data.reduceByKey(lambda a,b:a).cache()        
        walmart_data = walmart_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
        upc, (title, author, price, url) = walmart_data.first()
        print '\tWalmart\t$%.2f\t%s' % (float(price)/100, url)

        ebay_data = ebay_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
        upc, (title, author, price, url) = ebay_data.first()
        print '\tEbay\t$%.2f\t%s' % (float(price)/100, url)

'''
sc.stop()

print 'Load time: %.5f seconds' % (start - pre)
print 'Search time: %.5f seconds' % (end - start)
 
