from amazonproduct import API
api = API(locale='us')

count = 0
result = api.browse_node_lookup(1000)
for child1 in result.BrowseNodes.BrowseNode.Children.BrowseNode:
    if (count >= 500): 
        break
    result1 = api.browse_node_lookup(child1.BrowseNodeId)
    for child in result1.BrowseNodes.BrowseNode.Children.BrowseNode:
        if (count >= 500): 
            break
        count += 1
        print '%s (%s)' % (child.Name, child.BrowseNodeId)

#for book in api.item_search('Books',BrowseNode='3207'):
 #   print '%s: "%s"' % (book.ItemAttributes.Author,
 #                       book.ItemAttributes.Title)
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
