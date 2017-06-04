from amazonproduct import API
api = API(locale='us')
from amazonproduct import errors
from random import randint   
import sys
import numpy as np
from pyspark import SparkContext

#save create CSVLine
def toCSVLine(data):
  return ','.join(str(d) for d in data)

def getPrice(price):
    return price + randint(-100,100)
   
count = 0
limit_reached = False
sc = SparkContext(appName="AskMeMP")

amazon_rdd = sc.parallelize(['ID+TITLE+AUTHOR+URL+PRICE'])
walmart_rdd = sc.parallelize(['ID+TITLE+AUTHOR+URL+PRICE'])
ebay_rdd = sc.parallelize(['ID+TITLE+AUTHOR+URL+PRICE'])

result = api.browse_node_lookup(1000)
for child1 in result.BrowseNodes.BrowseNode.Children.BrowseNode:
    if limit_reached: 
        break
    result1 = api.browse_node_lookup(child1.BrowseNodeId)
    for child in result1.BrowseNodes.BrowseNode.Children.BrowseNode:
        if limit_reached: 
            break
        for book in api.item_search('Books',BrowseNode=child.BrowseNodeId):
            try:
                detail = api.item_lookup(str(book.ASIN),ResponseGroup='OfferSummary').Items[0]
                temp_rdd = sc.parallelize([str(book.ASIN)+'+'+book.ItemAttributes.Title+'+'+book.ItemAttributes.Author
                +'+'+book.DetailPageURL+'+'+str(detail.Item.OfferSummary.LowestNewPrice.Amount)])
                amazon_rdd = amazon_rdd.union(temp_rdd)
                #print '%s,%s,%s,%s,%s' % (book.ASIN,book.ItemAttributes.Title,book.ItemAttributes.Author,book.DetailPageURL,detail.Item.OfferSummary.LowestNewPrice.Amount)
                amazon_price = int(detail.Item.OfferSummary.LowestNewPrice.Amount)
                amazon_url = str(book.DetailPageURL)
                
                walmart_url = amazon_url.replace("amazon", "walmart")
                walmart_price = getPrice(amazon_price)               
                temp_rdd = sc.parallelize([str(book.ASIN)+'+'+book.ItemAttributes.Title+'+'+book.ItemAttributes.Author
                +'+'+walmart_url+'+'+str(walmart_price)])                        
                walmart_rdd = walmart_rdd.union(temp_rdd)
                #print '%s,%s,%s,%s,%s' % (book.ASIN,book.ItemAttributes.Title,book.ItemAttributes.Author,walmart,walmart_price)
                            
                ebay_url = walmart_url.replace("walmart", "ebay")
                ebay_price = getPrice(amazon_price)
                temp_rdd = sc.parallelize([str(book.ASIN)+'+'+book.ItemAttributes.Title+'+'+book.ItemAttributes.Author
                +'+'+ebay_url+'+'+str(ebay_price)])
                ebay_rdd = ebay_rdd.union(temp_rdd)
                #print '%s,%s,%s,%s,%s' % (book.ASIN,book.ItemAttributes.Title,book.ItemAttributes.Author,ebay,ebay_price)
                
                count+=1
                if count>=15:
                    limit_reached = True
                    break
            except AttributeError, e:
                continue
#n = amazon_rdd.map(toCSVLine)
amazon_rdd.saveAsTextFile('hdfs://192.168.0.33:54310/final/amazon.txt')
walmart_rdd.saveAsTextFile('hdfs://192.168.0.33:54310/final/walmart.txt')
ebay_rdd.saveAsTextFile('hdfs://192.168.0.33:54310/final/ebay.txt')

#test = sc.textFile('hdfs://192.168.0.33:54310/final/ebay.txt')
#for x in test.take(9):
#    print x
sc.stop()


# get all books from result set and
# print author and title
'''
for book in api.item_search('Books',Keywords='Java',ResponseGroup='OfferSummary'):
    print '%s: "%s"' % (book.ASIN,
                       vars(book.OfferSummary))
    for similar in api.similarity_lookup(str(book.ASIN)).Items:
        print 'Similar : %s: %s' %(similar.Item.ASIN,similar.Item.ItemAttributes.Title)
        print vars(similar.Item.ItemAttributes)
'''
