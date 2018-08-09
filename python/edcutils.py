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
