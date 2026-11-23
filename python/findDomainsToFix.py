"""
Created on Jan 7, 2020

@author: dwrigley

usage:
  findDomainsToFix.py -h to see command-line options

  output written to domains_to_fix.csv

Note:  requires python 3 (3.6+)
       packages used:
            requests        http library
            python-dotenv   command-line parsing

searches inferred domains created using provierId != DDPScanner


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
import edcutils

# from datetime import timezone


urllib3.disable_warnings()


class mem:
    """
    empty class for storing variables & not making them global
    """

    edcSession: EDCSession = EDCSession()
    items_found = 0
    page_size = 100
    fix_required_count = 0


# setup logging
if not os.path.exists("./log"):
    print("creating new folder: ./log")
    os.makedirs("./log")
logging.basicConfig(
    format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
    level=logging.DEBUG,
    # filename=datetime.now().strftime("./log/domain_finder_%Y-%m-%d_%H-%M-%S.log"),
    filename=datetime.now().strftime("./log/domain_finder.log"),
    filemode="w",
)


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
    "--pagesize",
    required=False,
    type=int,
    default=300,
    help=("pageSize setting to use when executing an objects query, default 300"),
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
    # logging.info(f"args: list={args.list}")
    mem.edcSession.initUrlAndSessionFromEDCSettings()
    mem.page_size = args.pagesize

    print()
    out_filename = "items_with_domain_inf_errors.csv"
    with open(out_filename, "w") as out_file:
        # file header
        out_file.write(
            "id,domain,providerId,infDataDomain attr,modified_by,"
            "is_accepted,is_rejected,fix_required\n"
        )
        find_domains_inference_errors(out_file)

    print(f"\nrun time = {time.time() - start_time:.3f} seconds ---")
    print(f"    results written to file: {out_filename}")
    print(f"items with inferred domains: {mem.items_found}")
    print(f"    count of fixes required: {mem.fix_required_count}")
    print("process complete")


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


def find_domains_inference_errors(file_to_write):
    # count objects for the domain passed
    # pagination settings (we want to process a few hundred items at a time)
    total = 100  # initial value - set to > 0 - will replaced on first call
    offset = 0
    page = 0
    # page_size set on script init (or prompt from cmd-line?)

    search = "com.infa.ldm.profiling.dataDomainsInferred:*"
    print(f"searching for all objects with inferred domains blocks of {mem.page_size}")
    print(f"search for: {search}")

    queryparms = {
        "q": search,
        "associations": [
            "com.infa.ldm.profiling.DataDomainColumnInferred",
            "com.infa.ldm.profiling.DataDomainColumnAccepted",
            "com.infa.ldm.profiling.DataDomainColumnRejected",
        ],
        "offset": "0",
        "pageSize": mem.page_size,
        "includeRefObjects": "false",
        "includeDstLinks": "False",
        "includeSrcLinks": "True",
    }

    while offset < total:
        page_time = time.time()
        errors_in_page = 0
        # update the offset for every iteration (adding the pagesize)
        queryparms["offset"] = offset
        page += 1

        logging.info(
            f"listing inferred domain objects: page={page} "
            f"offset={offset} pagesize={mem.page_size} query={search}"
        )
        resturl = mem.edcSession.baseUrl + "/access/2/catalog/data/objects"
        try:
            response = mem.edcSession.session.get(resturl, params=queryparms, timeout=5)
        except requests.exceptions.ConnectionError as err:
            print(f"Connection timeout, exiting\n: {err}")
            break
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
            f"objects processed: {offset+1}-{offset+mem.page_size} "
            f"of {total} page={page}"
        )

        # print(f"query rc= {rc}")

        # print(f"items found={dupJson['totalCount']:,}")
        for item in resp_json["items"]:
            # print(f"removing duplicate dd:{domain} from object: {item['id']}")
            # file_to_write.write(f"{item['id']}\n")
            requires_fix = check_object_for_errors(item, item["id"], file_to_write)
            if requires_fix:
                mem.fix_required_count += 1
                errors_in_page += 1

        # end of page processing
        print(
            f"\tpage processed - {time.time() - page_time:.1f}s, "
            f"errors this page:{errors_in_page} total={mem.fix_required_count}"
        )

        # for next iteration
        offset += mem.page_size
        # flush output file
        file_to_write.flush()


def check_object_for_errors(item: dict, item_id: str, file_to_write) -> bool:
    # does attribute "com.infa.ldm.profiling.infDataDomain" exist
    requires_fix = False
    inf_dd_attr = edcutils.getFactValue(item, "com.infa.ldm.profiling.infDataDomain")
    if len(inf_dd_attr) > 0:
        inf_dd_attr = '"' + inf_dd_attr + '"'
    # look in for inferred domain
    for link in item["srcLinks"]:
        # if link["id"] == f"DataDomain://{domain}":
        if link["association"] == "com.infa.ldm.profiling.DataDomainColumnInferred":
            modified_by = link["modifiedBy"]
            inf_provider_id = link["providerId"]
            domain_ref = link["id"]
            domain_name = domain_ref.split("://")[1]
            if modified_by != "system":
                mem.items_found += 1
                # print(f"id={item_id}")
                logging.info(f"item found with inferred link: {item_id}")
                # print(
                #     "\tfound DataDomainColumnInferred link - "
                #     f"using providerid: {inf_provider_id} "
                #     f"modified_by:{modified_by}"
                # )
                is_accepted = is_domain_linked(
                    item["srcLinks"],
                    domain_ref,
                    "com.infa.ldm.profiling.DataDomainColumnAccepted",
                )
                is_rejected = is_domain_linked(
                    item["srcLinks"],
                    domain_ref,
                    "com.infa.ldm.profiling.DataDomainColumnRejected",
                )
                if inf_provider_id != "DDPScanner":
                    requires_fix = True
                elif len(inf_dd_attr) == 0:
                    requires_fix = True

                file_to_write.write(
                    f"{item['id']},{domain_name},{inf_provider_id},{inf_dd_attr},"
                    f"{modified_by},{is_accepted},{is_rejected},{requires_fix}\n"
                )

    return requires_fix


def is_domain_linked(links_to_check: list, domain_id: str, link_type: str) -> bool:
    # check if the domain is referenced via link_type
    is_referenced = False

    for src_link in links_to_check:
        # print(f"checking: {src_link['association']}=={link_type}")
        # print(f"checking: {src_link['id']}=={domain_id}")
        if src_link["association"] == link_type and src_link["id"] == domain_id:
            is_referenced = True
            break

    return is_referenced


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
