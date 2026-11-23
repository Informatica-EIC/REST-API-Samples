"""
Created on Jan 7, 2020

@author: dwrigley

usage:
  findDuplicateDomains.py -h to see command-line options

  output written to domain_summary.csv

Note:  requires python 3 (3.6+)
       packages used:
            requests        http library
            python-dotenv   command-line parsing

searches for possible data domain issues where
a single column has BOTH Inferred and Accepted|Rejected data domain references.

Technique used:-
- get a list of data domains that are inferred across the whole catalog

Tested with:  v10.5.x including 10.5.3.0.2,
"""
import requests
import time
import sys
import urllib3
import argparse
import os
from pathlib import PurePath
from edcSessionHelper import EDCSession
import setupConnection
import logging
from datetime import datetime

# from datetime import timezone
import edcutils
import json

urllib3.disable_warnings()


class mem:
    """
    empty class for storing variables & not making them global
    """

    edcSession: EDCSession = EDCSession()
    remove_counter = 0


# setup logging
if not os.path.exists("./log"):
    print("creating new folder: ./log")
    os.makedirs("./log")
logging.basicConfig(
    format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
    level=logging.DEBUG,
    # filename=datetime.now().strftime("./log/lineage_validator_%Y-%m-%d_%H-%M-%S.log"),
    filename=datetime.now().strftime("./log/domain_duplication_%Y-%m-%d_%H-%M-%S.log"),
    filemode="w",
)


page_size = 100  # number of objects for each page/chunk

# setup command-line parser
parser = argparse.ArgumentParser(parents=[mem.edcSession.argparser])
parser.add_argument(
    "--setup",
    required=False,
    action="store_true",
    help=(
        "setup the connection to EDC by creating a .env file"
        " - same as running setupConnection.py"
    ),
)
parser.add_argument(
    "--list",
    required=False,
    action="store_true",
    help=(
        "list all objects that have duplicated domains to file "
        "named items_with_duplicated_domains.txt"
    ),
)
parser.add_argument(
    "--remove",
    required=False,
    action="store_true",
    help=("remove the duplicated inferred domain, using PATCH api "),
)
parser.add_argument(
    "--maxobjects",
    required=False,
    type=int,
    default=10,
    help=(
        "max number of objects to remove, for testing.  only used when --remove is used"
    ),
)


def main():
    """
    start the process to identify possible errors with data domains
    that are both inferred and accepted|rejected
    """
    start_time = time.time()
    p = PurePath(sys.argv[0])
    print(f"{p.name} starting in {os.getcwd()}")

    # read any command-line args passed
    args, unknown = parser.parse_known_args()
    if args.setup:
        # if setup is requested (running standalone)
        # call setupConnection to create a .env file to use next time we run
        print("setup requested..., calling setupConnection & exiting")
        setupConnection.main()
        return
    # initialize http session to EDC, storing the baseurl
    logging.info("process starting")
    logging.info(
        f"args: list={args.list}, remove={args.remove}, maxobjects={args.maxobjects}"
    )
    mem.edcSession.initUrlAndSessionFromEDCSettings()

    domain_list = list_inferred_domains()
    domains_with_errors = count_duplicates_for_domains(domain_list)

    print(f"Domains found with Errors: {len(domains_with_errors)}")
    print()

    if args.list or args.remove:
        print(
            f"listing objecs: {args.list}"
            f" removing refs: {args.remove}, max={args.maxobjects}"
        )
        # createa file to contain a list of object id's with duplicated domains
        # iterate over each domain and call the list function
        out_filename = "items_with_duplicated_domains.txt"
        with open(out_filename, "w") as out_file:
            for domain, errorcount in domains_with_errors.items():
                print_inferred_and_accepted_objects_for_domain(
                    domain,
                    errorcount,
                    out_file,
                    args.list,
                    args.remove,
                    args.maxobjects,
                )
            print(f"list of objects writting to file: {out_filename}")

    print(f"run time = {time.time() - start_time} seconds ---")


def searchSummaryv1(session, resturl, querystring):
    """
    helper script to execute a search
    """
    try:
        resp = session.get(resturl, params=querystring, timeout=3)
        # print(f"api status code={resp.status_code}")
        return resp.status_code, resp.json()
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        # exit if we can't connect
        return 0, None


def list_inferred_domains() -> list:
    """
    for any domain - check to see if there are any instances of
    both inferred and accepted domains - this happenens
    when a patch api object is used to add an inferred domain that
    does not check for a pre-existing accepted/rejected domain

    return a list of domains found

    the process will:-
    - find all domains with a least 1 inferred object

    """
    # format the query parameters for finding all domains (+ rejected) for all resources
    querystring = {
        "q": "com.infa.ldm.profiling.dataDomainsInferred:*",
        "offset": "0",
        "pageSize": "1",
        "hl": "false",
        "related": "false",
        "rootto": "false",
        "facet.field": [
            "core.classType",
            "core.resourceName",
            "com.infa.ldm.profiling.dataDomainsInferred",
        ],
        # "includeRefObjects": "false",
    }

    print(f"\nexecuting search for domains in use q={querystring['q']}")
    logging.info(f"executing search for domains in use q={querystring['q']}")
    print(f"\tusing facets: {querystring['facet.field']}")
    resturl = mem.edcSession.baseUrl + "/access/1/catalog/data/search"
    rc, domainJson = searchSummaryv1(mem.edcSession.session, resturl, querystring)
    print(f"query rc= {rc}")
    if rc != 200:
        print(f"error running query: {rc} {domainJson}")
        print("exiting")
        return

    # itemCount = domainJson["totalCount"]
    print(f"items found={domainJson['totalCount']:,}")
    logging.info(f"items found={domainJson['totalCount']:,}")

    all_facets = domainJson["facets"]["facetFields"]
    # print(f"facet field count = {len(all_facets)}")

    domains_to_check = []

    # check the dataDomainsInferred facet for a count of objects for each domain
    for facet_inst in all_facets:
        if facet_inst["fieldName"] == "com.infa.ldm.profiling.dataDomainsInferred":
            print(f"Domains with Inferred objects: {len(facet_inst['rows'])}")
            for row in facet_inst["rows"]:
                domains_to_check.append(row["value"])
        if facet_inst["fieldName"] == "core.classType":
            print(f"classtypes: {len(facet_inst['rows'])}")
        if facet_inst["fieldName"] == "core.resourceName":
            print(f"resources: {len(facet_inst['rows'])}")

    logging.info(f"domains with inferred objects found={len(domains_to_check)}")
    logging.info(domains_to_check)
    return domains_to_check


def count_duplicates_for_domains(domains_to_check: list) -> dict:
    """
    - for each domain in the domains_to_check list
        - find any items with both inferred and accepted domains using
          com.infa.ldm.profiling.dataDomainsInferred:*  as the search
          and faceting on the com.infa.ldm.profiling.dataDomainsInferred attribute

          this gives us the fastest way to get a list of domains to run the next search

        - for each domain found (with inferred link to dataelements)
            - search for:
              com.infa.ldm.profiling.dataDomainsInferred:{domain} AND
                  (
                  com.infa.ldm.profiling.dataDomainsAccepted:{domain}
                  OR
                  com.infa.ldm.profiling.dataDomainsRejected:{domain}
                  )
              count instances of any matches (candiates for cleanup)


    this will be much faster than just iterating over all objects with an inference
    since we can use search to identify the objects
    """
    print(
        "checking for inferred and accepted|rejected domain errors, "
        f"for {len(domains_to_check)} domains"
    )
    domains_with_errors = {}
    total_errors = 0
    for domain in domains_to_check:
        dup_count = count_inferred_and_accepted_objects_for_domain(domain)
        if dup_count > 0:
            domains_with_errors[domain] = dup_count
            total_errors += dup_count

    print("")
    print(f"objects with errors: {total_errors}")
    logging.info(f"objects with errors: {total_errors}")
    print(
        f"domains with errors: {len(domains_with_errors)} "
        "(duplicate Inferred AND (Accepted OR Rejected)"
    )
    logging.info(
        f"domains with errors: {len(domains_with_errors)} "
        "(duplicate Inferred AND (Accepted OR Rejected)"
    )

    # print a summary of the errors found, by domain
    for domain, errorcount in domains_with_errors.items():
        print(f"\tDomain:{domain}\tduplicate refs found: {domains_with_errors[domain]}")
        logging.info(
            f"\tDomain:{domain}\tduplicate refs found: {domains_with_errors[domain]}"
        )
    #         print_inferred_and_accepted_objects_for_domain(domain)

    return domains_with_errors


def count_inferred_and_accepted_objects_for_domain(domain: str):
    # count objects for the domain passed
    print(".", end="", flush=True)
    search = (
        f"com.infa.ldm.profiling.dataDomainsInferred:{domain} AND "
        f"( com.infa.ldm.profiling.dataDomainsAccepted:{domain} OR "
        f"com.infa.ldm.profiling.dataDomainsRejected:{domain}"
        ")"
    )
    querystring = {
        "q": search,
        "offset": "0",
        "pageSize": "1",
        "hl": "false",
        "related": "false",
        "rootto": "false",
        "facet.field": [
            "core.classType",
            "core.resourceName",
        ],
        # "includeRefObjects": "false",
    }

    # print(f"\nexecuting search for domains in use q={querystring['q']}")
    # print(f"\tusing facets: {querystring['facet.field']}")
    resturl = mem.edcSession.baseUrl + "/access/1/catalog/data/search"
    rc, dupJson = searchSummaryv1(mem.edcSession.session, resturl, querystring)
    # print(f"query rc= {rc}")
    if rc != 200:
        print(f"error running query: {rc} {dupJson}")
        print("exiting")
        return 0

    itemCount = dupJson["totalCount"]
    # print(f"items found={dupJson['totalCount']:,}")
    logging.info(f"items found={itemCount} for domain {domain}")
    return itemCount


def print_inferred_and_accepted_objects_for_domain(
    domain: str,
    errorcount: int,
    file_to_write,
    list_objects: bool,
    remove_objects: bool,
    max_objects_to_clean: int,
):
    # count objects for the domain passed
    # pagination settings (we want to process a few hundred items at a time)
    total = 100  # initial value - set to > 0 - will replaced on first call
    offset = 0
    page = 0
    # page_size set on script init (or prompt from cmd-line?)

    print(
        f"processing: {errorcount} items for domain:{domain} in blocks of {page_size}"
    )
    search = (
        f"com.infa.ldm.profiling.dataDomainsInferred:{domain} AND "
        f"( com.infa.ldm.profiling.dataDomainsAccepted:{domain} OR "
        f"com.infa.ldm.profiling.dataDomainsRejected:{domain}"
        ")"
    )

    queryparms = {
        "q": search,
        "offset": "0",
        "pageSize": page_size,
        "includeRefObjects": "false",
    }

    while offset < total:
        page_time = time.time()
        # update the offset for every iteration (adding the pagesize)
        queryparms["offset"] = offset
        page += 1

        logging.info(
            f"listing objects for domain {domain}: page={page} "
            f"offset={offset} pagesize={page_size} query={search}"
        )
        resturl = mem.edcSession.baseUrl + "/access/2/catalog/data/objects"
        response = mem.edcSession.session.get(resturl, params=queryparms, timeout=3)
        rc = response.status_code
        resp_json = response.json()
        if rc != 200:
            print(f"error running query: {rc} {resp_json}")
            print("exiting")
            break

        if offset == 0:
            # get the total only for the first page
            total = resp_json["metadata"]["totalCount"]
        # page_objects = len(resp_json["items"])
        print(
            f"objects processed: {offset+1}-{offset+page_size} of {total} page={page}"
        )

        # print(f"query rc= {rc}")

        # print(f"items found={dupJson['totalCount']:,}")
        for item in resp_json["items"]:
            # print(f"removing duplicate dd:{domain} from object: {item['id']}")
            file_to_write.write(f"{item['id']}\n")
            mem.remove_counter += 1
            if remove_objects:

                print(
                    f"\nprocessing object {mem.remove_counter} of {total} "
                    f"id={item['id']}"
                )
                remove_duplicate_domainref(item["id"], item, domain)
            # break from item loop, if max reached
            if remove_objects and mem.remove_counter >= max_objects_to_clean:
                print(f"max object removal, reached {mem.remove_counter} ")
                break

        # end of page processing
        print("\t\tpage processed - %s seconds ---" % (time.time() - page_time))

        # for next iteration
        offset += page_size
        # break from item loop, if max reached
        if remove_objects and mem.remove_counter >= max_objects_to_clean:
            print("page iterator break - max reached")
            break


def remove_duplicate_domainref(id: str, object: dict, domain: str):
    """
    process - update the column, removing the duplicated domain inference
    @todo:  read the parent to determine if the DomainTable link should be removed
            since this link is stored only 1 time - it should not be removed by default
            since this is a duplicated reference
            this script does not detect/remove duplicated table > domain relationships
    """
    logging.info(f"ready to start removing inferred domain:{domain} from {id}")
    parent_id = id.rsplit("/", 1)[0]  # parent (table id)

    # look for
    #   attribute 'com.infa.ldm.profiling.DataDomainColumnInferred' = {domain}
    #   attribute ''com.infa.ldm.profiling.DataDomainColumnAccepted' = {domain}
    #   attribute ''com.infa.ldm.profiling.DataDomainColumnRejected' = {domain}
    is_accepted = False
    is_rejected = False
    is_inferred = False
    inf_domain_value = edcutils.getFactValue(
        object, "com.infa.ldm.profiling.infDataDomain"
    )
    if (
        edcutils.getFactValue(object, "com.infa.ldm.profiling.DataDomainColumnInferred")
        == domain
    ):
        is_inferred = True
    if (
        edcutils.getFactValue(object, "com.infa.ldm.profiling.DataDomainColumnAccepted")
        == domain
    ):
        is_accepted = True
    if (
        edcutils.getFactValue(object, "com.infa.ldm.profiling.DataDomainColumnRejected")
        == domain
    ):
        is_rejected = True

    logging.info(
        f"\tobject domain attributes: inferred={is_inferred}, "
        f"accepted={is_accepted}, rejected={is_rejected}"
    )

    template_file = "./template/delete_inferred_domain_duplicate_accepted_template.json"
    if is_inferred and is_accepted:
        # remove the inferred link (keep accapted)
        print("\tremove inferred, leaving accepted")
        logging.info(f"item {id} is inferred and accepted")
    if is_inferred and is_rejected:
        print("\tremove inferred, leaving rejected")
        logging.info(f"item {id} is inferred and rejected")
        print("\trejected domain found. checking all columns in parent")
        domain_usage_count = count_domainrefs_for_dataset(parent_id, domain)
        if domain_usage_count == 1:
            print("\tdelete template used, since only 1 ref found for rejected domain")
            template_file = (
                "./template/delete_inferred_domain_duplicate_rejected_template.json"
            )

    # check for links
    inf_provider_id = "DDPScanner"
    inf_timestamp = ""
    count_all_links = 0
    count_acc_links = 0
    for link in object["srcLinks"]:
        if link["id"] == f"DataDomain://{domain}":
            if link["association"] == "com.infa.ldm.profiling.DataDomainColumnInferred":
                inf_provider_id = link["providerId"]
                print(
                    "\tfound DataDomainColumnInferred link - "
                    f"using providerid: {link['providerId']}"
                )
                # get the timestamp when the attribute was created
                for attr in link["linkProperties"]:
                    # print(attr)
                    if attr["attributeId"] == "com.infa.ldm.profiling.linkCreationTime":
                        inf_timestamp = attr["value"]
                        print(f"\tinferred timestamp is: {inf_timestamp}")
            if link["association"] == "com.infa.ldm.profiling.DataDomainColumnAccepted":
                # this link should be removed if is already rejected
                is_accepted = True
                count_acc_links += 1
            if link["association"] == "com.infa.ldm.profiling.DataDomainColumnAll":
                # this link should be removed if is already rejected
                count_all_links += 1

    print(
        f"\t\tcount all links for domain: {domain}: "
        f"all={count_all_links} accepted={count_acc_links} "
        f"inf_timestamp={inf_timestamp}"
    )
    logging.info(
        f"item: {id} count of com.infa.ldm.profiling.DataDomainColumnAll links for "
        f"{domain}: all={count_all_links} accepted={count_acc_links} "
        f"inf_timestamp={inf_timestamp}"
    )

    # note: read this value from a template file (assumption: the file exists and valid)
    logging.info(f"reading template file for PATCH: {template_file}")
    with open(template_file, "r") as file:
        remove_template = file.read()
    user_id = mem.edcSession.userid
    # print(f"using id={user_id}")
    # replace placeholder values
    remove_template = remove_template.replace("<user>", user_id)
    remove_template = remove_template.replace("<domain>", domain)
    remove_template = remove_template.replace("<domain>", domain)
    remove_template = remove_template.replace("<column_id>", id)
    remove_template = remove_template.replace("<inf_domain_value>", inf_domain_value)
    remove_template = remove_template.replace("<inf_timestamp>", inf_timestamp)
    remove_template = remove_template.replace("<table_id>", parent_id)

    remove_json = json.loads(remove_template)
    # set the provider id (it could be DataDomainScanner, or DDPScanner)
    remove_json["providerId"] = inf_provider_id

    if inf_timestamp == "":
        print("\tno timestamp found in inferred link, removing from payload")
        logging.info("no timestamp found in inferred link, removing from payload")
        # we need to remove the timestamp from the payload
        # since passing "" will fail with a 400
        for updates in remove_json["updates"]:
            for del_links in updates["deleteSourceLinks"]:
                assoc_id = del_links["associationId"]
                for pos, prop in enumerate(del_links["properties"]):
                    if prop["attrUuid"] == "com.infa.ldm.profiling.linkCreationTime":
                        logging.info(
                            f"removing {pos} element from {assoc_id} value was:{prop}"
                        )
                        del_links["properties"].pop(pos)
                        break

    logging.info(
        f"delete domain inference payload\n{json.dumps(remove_json, indent=4)}"
    )
    # execute the PATCH to remove the duplicated domain inference
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    resturl = mem.edcSession.baseUrl + "/access/1/catalog/data/objects"
    print("\tissuing PATCH api request")
    response = mem.edcSession.session.patch(
        resturl, json=remove_json, headers=headers, timeout=3
    )
    rc = response.status_code
    resp_json = response.json()
    if rc != 200:
        print(f"\terror running query: {rc} {resp_json}")
        logging.error(f"error running query: {rc} {resp_json}")
    else:
        print(f"\tPatch API execution succeeded. rc={rc} {resp_json}")
        logging.info(f"PATCH api call succeeded: rc={rc} {resp_json}")


def count_domainrefs_for_dataset(table_id: str, domain_name: str) -> int:
    refs_found = 0
    logging.info(f"counting domain refs for all children of dataset:{table_id}")
    # we will get all objects that are children of the dataset
    # using <dataset_id>/* as the id search
    # and filter for links only on com.infa.ldm.profiling.DataDomainColumnAll
    table_children_search = table_id.replace(":", "\\:")
    querystring = {
        "q": f"id:{table_children_search}/*",
        "includeDstLinks": "false",
        "includeRefObjects": "false",
        "includeSrcLinks": "true",
        "offset": "0",
        "pageSize": "1000",
        "associations": "com.infa.ldm.profiling.DataDomainColumnAll",
    }
    # execute the query
    print(
        f"\tcounting domain={domain_name} refs for all table children for :{table_id}"
    )
    resturl = mem.edcSession.baseUrl + "/access/2/catalog/data/objects"
    response = mem.edcSession.session.get(resturl, params=querystring, timeout=3)
    rc = response.status_code
    resp_json = response.json()
    if rc != 200:
        print(f"error running query: {rc} {resp_json}")
        return 0

    # the query was successful
    total = resp_json["metadata"]["totalCount"]
    print(f"\tchildren of dataset {table_id} = {total}")
    for item in resp_json["items"]:
        # check the dstlinks
        if "srcLinks" not in item:
            continue
        for src_link in item["srcLinks"]:
            if src_link["id"] == f"DataDomain://{domain_name}":
                refs_found += 1
                logging.debug(
                    f"\t\tfound domain ref: {refs_found} for object {item['id']}"
                )

    logging.info(
        f"count_domainrefs_for_dataset: returning {refs_found} for dataset={table_id}"
    )
    return refs_found


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
