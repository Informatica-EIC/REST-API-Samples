'''
Created on Jul 16, 2018

@author: dwrigley

This template can be copied & used to query the catalog
and process each item returned individually (in processAnItem)
it handles the paging model (see pageSize variable)

'''

import requests
from requests.auth import HTTPBasicAuth
import time
import edcutils

start_time = time.time()

# ******************************************************
# change these settings for your catalog service
# ******************************************************
# set variables for connecting to the catalog
# and running a query to get a result-set
# the processItem function will be called for each item
# ******************************************************
catalogServer = 'http://napslxapp01:9085'
uid = 'admin'
# pwd='admin'
pwd = uid
query = " core.allclassTypes:( \
        com.infa.ldm.relational.Column  \
        com.infa.ldm.relational.ViewColumn  \
        com.infa.ldm.file.delimited.DelimitedField  \
        com.infa.ldm.file.xml.XMLFileField  \
        com.infa.ldm.file.json.JSONField  \
        com.infa.ldm.adapter.Field  \
        com.infa.ldm.file.avro.AVROField  \
        com.infa.ldm.file.parquet.PARQUETField \
        ) \
        and core.resourceName:acme_crm \
        "

pageSize = 10   # e.g. 10 objects for each page/chunk - change as needed
# ******************************************************
# end of parameters that should be changed
# ******************************************************

objectsurl = catalogServer + '/access/2/catalog/data/objects'
header = {"Accept": "application/json"}
itemCount = 0


def processItem(anItem, itemCount):
    '''
    put your code here - that does something with the item
    for this example, just print the it and name
    @note python 2.7 does not allow us to specify the parameter type...
    '''
    itemId = anItem["id"]
    itemName = edcutils.getFactValue(anItem, "core.name")
    print("\titem " + str(itemCount) + "=" + itemId + " name=" + itemName)


def main():
    """
    main starts here - run the query processing all items
    note:  this version supports the paging model, to process the results
           in chunks of pageSize
    """
    total = 1000  # initial value - set to > 0 - will replaced on first call
    offset = 0
    page = 0

    print("catalog service=" + catalogServer)
    print("user=" + uid)
    print('query=' + query)
    print("")

    while offset < total:
        page_time = time.time()
        parameters = {'q': query, 'offset': offset, 'pageSize': pageSize}
        page += 1
        resp = requests.get(objectsurl, params=parameters, headers=header,
                            auth=HTTPBasicAuth(uid, pwd))
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            break

        resultJson = resp.json()
        total = resultJson['metadata']['totalCount']
        print(f"objects found: {total} offset: {offset} "
              f"pagesize={pageSize} currentPage={page} "
              f"objects {offset+1} - {offset+pageSize}"
              )

        # for next iteration
        offset += pageSize

        global itemCount
        for foundItem in resultJson["items"]:
            itemCount += 1
            processItem(anItem=foundItem, itemCount=itemCount)

        # end of page processing
        print("\tpage processed - %s seconds ---" % (time.time() - page_time))

    # end of while loop
    print("Finished - run time = %s seconds ---" % (time.time() - start_time))


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
