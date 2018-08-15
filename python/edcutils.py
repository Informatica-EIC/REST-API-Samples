'''
Created on Aug 2, 2018

utility functions for processing catalog objects

@author: dwrigley
'''


def getFactValue(item, attrName):
    """
    returns the value of a fact (attribute) from an item
    
    iterates over the "facts" list - looking for a matching attributeId to the paramater attrName
    returns the "value" property or "" 
    """
    # get the value of a specific fact from an item
    value=""
    for facts in item["facts"]:
        if facts.get('attributeId')==attrName:
            value=facts.get('value')
            break
    return value

def exportLineageLink(fromObject, toObject, linkType, csvFile):
    """
    write a custom lineage line to the csv file
    assumptions
      - csvFile is already created
      - csv header is Association,From Connection,To Connection,From Object,To Object
    Association=linkType, From Object=fromObject,To Object=toObject
    From Connection and To Connection will be empty
    """
    #TODO: add error checking
    row=[linkType,"","",fromObject,toObject]
    csvFile.writerow(row)
    return

