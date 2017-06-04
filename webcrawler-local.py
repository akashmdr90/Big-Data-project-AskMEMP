import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time

from amazonproduct import API
api = API(locale='us')
from amazonproduct import errors
from random import randint   

#save create CSVLine
def toCSVLine(data):
  return ','.join(str(d) for d in data)

def getPrice(price):
    return price + randint(-100,100)
   
count = 0
limit_reached = False
start = time.time()

amazon_file = open('amazon.csv', 'w')
walmart_file = open('walmart.csv', 'w')
ebay_file = open('ebay.csv', 'w')

amazon_file.write('ID+TITLE+AUTHOR+URL+PRICE\n')
walmart_file.write('ID+TITLE+AUTHOR+URL+PRICE\n')
ebay_file.write('ID+TITLE+AUTHOR+URL+PRICE\n')

result = api.browse_node_lookup(1000)
for child1 in result.BrowseNodes.BrowseNode.Children.BrowseNode:
    if limit_reached: 
        break
    result1 = api.browse_node_lookup(child1.BrowseNodeId)
    for child in result1.BrowseNodes.BrowseNode.Children.BrowseNode:
        if limit_reached: 
            break
        for book in api.item_search('Books',BrowseNode=child.BrowseNodeId):
            #delay 0.1 seconds
            time.sleep(0.2)
            try:
                detail = api.item_lookup(str(book.ASIN),ResponseGroup='OfferSummary').Items[0]
                amazon_file.write(str(book.ASIN)+'+'+str(book.ItemAttributes.Title)+'+'+str(book.ItemAttributes.Author)
                +'+'+str(book.DetailPageURL)+'+'+str(detail.Item.OfferSummary.LowestNewPrice.Amount)+'\n')
                amazon_price = int(detail.Item.OfferSummary.LowestNewPrice.Amount)
                amazon_url = str(book.DetailPageURL)
                
                walmart_url = amazon_url.replace("amazon", "walmart")
                walmart_price = getPrice(amazon_price)               
                walmart_file.write(str(book.ASIN)+'+'+str(book.ItemAttributes.Title)+'+'+str(book.ItemAttributes.Author)
                +'+'+walmart_url+'+'+str(walmart_price)+'\n')
                            
                ebay_url = walmart_url.replace("walmart", "ebay")
                ebay_price = getPrice(amazon_price)
                ebay_file.write(str(book.ASIN)+'+'+str(book.ItemAttributes.Title)+'+'+str(book.ItemAttributes.Author)
                +'+'+ebay_url+'+'+str(ebay_price)+'\n')
                
                count+=1
                if count>=5000:
                    limit_reached = True
                    break
            except AttributeError, e:
                continue
#n = amazon_rdd.map(toCSVLine)
amazon_file.close()
walmart_file.close()
ebay_file.close()

end = time.time()
print 'Total time: %.5f minutes' % ((end - start)/60)
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
