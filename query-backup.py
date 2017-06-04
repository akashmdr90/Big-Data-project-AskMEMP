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


count = 0
start = time.time()
for book in api.item_search('Books', Keywords=sys.argv[1], Condition='New', Availability='Available'):
#    print '%s %s' %(book.ASIN,book.ItemAttributes.Title)
    count +=1
    if count > 10:
	    break
    amazon_data = amazon_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
    if amazon_data.count()>0:
        upc, (title, author, price, url) = amazon_data.first()
        print '+++' + str(upc)
        print '***"%s" by "%s"' % (title, author);
        print '\tAmazon\t$%.2f\t%s' % (float(price)/100, url)

        
        walmart_data = walmart_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
        upc, (title, author, price, url) = walmart_data.first()
        print '\tWalmart\t$%.2f\t%s' % (float(price)/100, url)

        ebay_data = ebay_rdd.map(parseVector).filter(lambda x : str(book.ASIN) in x[0]).cache()
        upc, (title, author, price, url) = ebay_data.first()
        print '\tEbay\t$%.2f\t%s' % (float(price)/100, url)
sc.stop()
end = time.time()
print 'Load time: %.5f seconds' % (start - pre)
print 'Search time: %.5f seconds' % (end - start)
 
