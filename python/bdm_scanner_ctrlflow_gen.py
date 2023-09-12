"""
Created September 5, 20223

BDM Scanner - lookup controlflow hack
usage:  bdm_scanner_ctrlflow_hack.py  <parms>

@author: dwrigley
"""

import csv
import os
import requests
import sys
import argparse
import edcSessionHelper
import setupConnection
import logging
import edcutils
import urllib3
import time
import platform
from datetime import datetime

urllib3.disable_warnings()

version = "1.0"

if not os.path.exists("./log"):
    print("creating log folder ./log")
    os.makedirs("./log")

logging.basicConfig(
    format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
    level=logging.DEBUG,
    filename=datetime.now().strftime(
        "./log/bdm_scanner_controlflow_gen_%Y_%m_%d_%H_%M_%S_%f.log"
    ),
    # filename=datetime.now().strftime("./log/bdm_scanner_controlflow_gen.log"),
    filemode="w",
)

# create the EDC session helper class
edcHelper = edcSessionHelper.EDCSession()

id_typ_dict = {}
id_name_dict = {}


class mem:
    """
    in memory objects (preferred over global vars.
    (should probably convert this functional script to a class)
    """

    lineage_csv_filename = ""
    links_written = 0

    totalLinks = 0
    totalObjects = 0
    mapping_count = 0
    mapping_total = 0
    lookup_count = 0
    field_ctlflow_links = 0
    dset_unique_links = set()


def setup_cmd_parser():
    # define script command-line parameters (in global scope for gooey/wooey)
    parser = argparse.ArgumentParser(parents=[edcHelper.argparser])

    # check for args overriding the env vars
    # parser = argparse.ArgumentParser()
    # add args specific to this utility (resourceName, resourceType, outDir, force)
    parser.add_argument(
        "-rn",
        "--resourceName",
        required=("--setup" not in sys.argv),
        help="setup the .env file for connecting to EDC",
    )

    parser.add_argument(
        "-o",
        "--outDir",
        required=False,
        default="./out",
        help=(
            "output folder to write results - default = ./out "
            " - will create folder if it does not exist"
        ),
    )

    parser.add_argument(
        "-i",
        "--edcimport",
        default=False,
        # type=bool,
        action="store_true",
        help=(
            "use the rest api to create the custom lineage resource "
            "and start the import process"
        ),
    )

    parser.add_argument(
        "--setup",
        required=False,
        action="store_true",
        help=(
            "setup the connection to EDC by creating a .env file"
            " - same as running setupConnection.py"
        ),
    )

    return parser


def bdm_lookup_ctlflow_links(resource_name: str, out_folder):
    print("ready to find mappings that have lookups..")

    init_files(resource_name, out_folder)

    # find mappings in resource
    try:
        resturl = edcHelper.baseUrl + "/access/2/catalog/data/objects"
        print(f"calling:  #{resturl}#")
        parms = {
            "q": "core.classType:com.infa.ldm.bdm.platform.Mapping",
            "fq": f"core.resourceName:{resource_name}",
        }
        logging.info(f"search for platform mappings using query={parms}")
        resp = edcHelper.session.get(resturl, params=parms, timeout=30)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        return None

    if resp.status_code == 200:
        resJson = resp.json()
        mem.mapping_total = resJson["metadata"]["totalCount"]
    else:
        print(f"non 200 response: {resp.status_code} {resp.text}")

    print(f"found: {mem.mapping_total} mappings to inspect")
    logging.info(f"found: {mem.mapping_total} mappings to inspect")

    for mapping in resJson["items"]:
        # print(f"mapping...{mapping['id']}")
        process_mapping(mapping)
        # print(f"mapping...{edcutils.getFactValue(mapping, 'id')}")

    print("finishing...")
    close_files()


def process_mapping(mapping_obj: dict):
    """
    from the mappiung - inspect dstLinks to get tx objects
    looking for lookup transformations
    we can;t search for these, since they are not 1st class objects
    """
    mem.mapping_count += 1
    print(
        f"\nprocessing mapping: {mem.mapping_count} of {mem.mapping_total} - {mapping_obj['id']}"
    )
    logging.info(
        f"processing mapping: {mem.mapping_count} of {mem.mapping_total} - {mapping_obj['id']}"
    )
    for linked_obj in mapping_obj["dstLinks"]:
        if linked_obj["classType"] == "com.infa.ldm.bdm.platform.LookUpTransformation":
            print(f"\tlookup found: {linked_obj['name']}")
            process_lookup(linked_obj.get("id"))


def process_lookup(lookup_id: str):
    print(f"\treading lookup details id={lookup_id}")
    logging.info(f"processing lookup id={lookup_id}")
    mem.lookup_count += 1
    # get object by id
    try:
        resturl = edcHelper.baseUrl + "/access/2/catalog/data/objects"
        # print(f"calling:  #{resturl}#")
        parms = {"id": lookup_id}
        resp = edcHelper.session.get(resturl, params=parms, timeout=30)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        return None

    if resp.status_code == 200:
        lookup_obj = resp.json()
        lkp_count = lookup_obj["metadata"]["totalCount"]
    else:
        print(f"non 200 response: {resp.status_code} {resp.text}")

    # print(f"found: {lkp_count} lookup to process\n--------\n")
    # if lkp_count != 1 error?

    # print(lookup_obj["items"][0])
    real_obj = lookup_obj["items"][0]
    lookup_condition = edcutils.getFactValue(
        real_obj, "com.infa.ldm.bdm.platform.lookupCondition"
    )
    lookup_id = real_obj["id"]
    print(f"\tlookup condition={lookup_condition}")
    logging.info(f"lookup condition={lookup_condition}")
    # might need to split the condition - if there are multiple, then split left/right from =
    lookup_statements = lookup_condition.split(" AND ")
    print(f"\tlookup statements found = {len(lookup_statements)}")

    lookup_fields = extract_lookup_fields(real_obj)
    # lookup fields should have a dict = name:id  (for all lookup fields)
    # instead of running lineage for all fields downstream - just do it for all fields
    # it should be faster than getting individual fields and determining if in/out or both

    print(f"\t{len(lookup_fields)} lookup fields found: {lookup_fields.keys()}")
    logging.info(f"{len(lookup_fields)} lookup fields found: {lookup_fields.keys()}")

    print(f"\tanalyzing lookup statements: {len(lookup_statements)}")
    for statement in lookup_statements:
        field_refs = statement.split(" = ")
        # should be always 2 objects
        print(f"\tfields compared = {field_refs}")
        left_name = field_refs[0]
        right_name = field_refs[1]
        left_id = ""
        right_id = ""
        # left_obj = {}
        # right_obj = {}

        if left_name in lookup_fields:
            # left_obj = lookup_fields.get(left_name)
            # left_id = left_obj.get("id")
            left_id = lookup_fields.get(left_name)

        if right_name in lookup_fields:
            # right_obj = lookup_fields.get(right_name)
            # right_id = right_obj.get("id")
            right_id = lookup_fields.get(right_name)

        print(f"\tlookup left: {left_id}")
        print(f"\tlookup rght: {right_id}")
        logging.info(f"left lookup ={left_id}")
        logging.info(f"right lookup={right_id}")

        lineage_left_upstream = get_lineage_for_object(left_id, "IN", 1)
        lineage_right_upstream = get_lineage_for_object(right_id, "IN", 10)

        # get upstream lineage from right object (incoming for comparison)
        # get upstream lineage from left objects??? needed???
        all_lineage_downstream = get_lineage_for_object(
            lookup_fields.values(), "OUT", 20
        )
        print(
            f"\t\tall fields downstream lineage: size={len(all_lineage_downstream['items'])}"
        )
        logging.info(
            f"all fields downstream lineage: size={len(all_lineage_downstream['items'])}"
        )

        # get downstream lineage from returned all output objects
        lineage_left_downstream = get_lineage_for_object(left_id, "OUT", 10)
        lineage_right_downstream = get_lineage_for_object(right_id, "OUT", 10)

        print(
            f"\tlineage generated.... {len(lineage_left_upstream['items'])}/"
            f"{len(lineage_right_upstream['items'])}/"
            f"{len(lineage_left_downstream['items'])}/"
            f"{len(lineage_right_downstream['items'])}/"
        )
        logging.info(
            f"lineage generated: {len(lineage_left_upstream['items'])}/"
            f"{len(lineage_right_upstream['items'])}/"
            f"{len(lineage_left_downstream['items'])}/"
            f"{len(lineage_right_downstream['items'])}/"
        )

        left_up_ids = get_lineage_endpoints_upstream(lineage_left_upstream, left_id)
        right_up_ids = get_lineage_endpoints_upstream(lineage_right_upstream, right_id)

        # print("ready to connect the dots...")
        print(f"\t left field - upstream  :: {left_up_ids}")
        print(f"\tright field - upstream  :: {right_up_ids}")
        logging.info(f" left field - upstream  :: {left_up_ids}")
        logging.info(f"right field - upstream  :: {right_up_ids}")

        # for each lineage field id - get the downstream lineage
        down_lineage_endpoints = []
        for lkp_field_id in lookup_fields.values():
            ds_ids = get_lineage_endpoints_downstream(
                all_lineage_downstream, lkp_field_id
            )
            # ds_ids = get_downstream_leaf_from_lineage(
            #     all_lineage_downstream, lkp_field_id
            # )
            if len(ds_ids) > 0:
                # print("exdend...")
                down_lineage_endpoints.extend(ds_ids)
            # print(f"found lineage for id={lkp_field_id} - {ds_ids}")
        print(f"all ds lineage size={len(down_lineage_endpoints)}")
        print(down_lineage_endpoints)

        print(f"\tdownstream fields to link:: {down_lineage_endpoints}")
        logging.info(f"downstream fields to link:: {down_lineage_endpoints}")

        # now iterate over the left/right links
        links_created = 0
        for left in left_up_ids + right_up_ids:
            # for right in right_down_ids + left_down_ids:
            for right in down_lineage_endpoints:
                # print(f"control flow>>{left} ==>>== {right}")
                mem.field_ctlflow_links += 1
                links_created += 1
                mem.connectionlinkWriter.writerow(
                    [
                        "core.DirectionalControlFlow",
                        "",
                        "",
                        left,
                        right,
                        # lookup_id,
                    ]
                )
                logging.info(
                    f"writing lineage: core.DirectionalControlFlow from: {left} to {right}"
                )
                # parent link too (dataset level)
                left_parent = left.rsplit("/", 1)[0]
                right_parent = right.rsplit("/", 1)[0]
                parent_key = left_parent + "::" + right_parent
                if parent_key not in mem.dset_unique_links:
                    mem.dset_unique_links.add(parent_key)
                    links_created += 1
                    mem.connectionlinkWriter.writerow(
                        [
                            "core.DataSetControlFlow",
                            "",
                            "",
                            left_parent,
                            right_parent,
                            # lookup_id,
                        ]
                    )
                    logging.info(
                        "writing lineage: core.DataSetControlFlow from: "
                        f"{left_parent} to {right_parent}"
                    )
        print(f"\tcontrol flow/lineage links created: {links_created}")


def get_lineage_endpoints_upstream(lineage_result: dict, seed_id) -> list:
    """
    recursive function, to get the upstream endpoints
    these are dataelements that are not com.infa.ldm.bdm.platform.Field instances

    we could mix upstream and downstream in a single function, but it is harder to understand
    so keep seperate for now
    """
    # list to return - initialize, for this instance/iteration and append any recursive calls
    endpoint_list = []

    # extract class types from lineage (in memory)
    # probably should call outside of this function - but will leave it for now
    classtypes = collect_classtypes_from_lineage(lineage_result)

    for lineage_hop in lineage_result["items"]:
        # get the out id
        in_id = lineage_hop["inId"]
        out_id = lineage_hop["outId"]

        # only process if the id matches the seed
        # since other lineage could be included
        if in_id == seed_id:
            # print(f"seed match - entry {iterations} ")
            # recursively call this function again???
            class_type = classtypes.get(out_id)
            if class_type != "com.infa.ldm.bdm.platform.Field":
                endpoint_list.append(out_id)
                # return_seeds.append(out_id)
            else:
                # still a platform field - so get the next iteration
                # call this function recursively
                nested_ids = get_lineage_endpoints_upstream(lineage_result, out_id)
                endpoint_list.extend(nested_ids)

    return endpoint_list


def get_lineage_endpoints_downstream(lineage_result: dict, seed_id) -> list:
    """
    recursive function, to get the downstream endpoints
    these are dataelements that are not com.infa.ldm.bdm.platform.Field instances

    we could mix upstream and downstream in a single function, but it is harder to understand
    so keep seperate for now
    """
    # list to return - initialize, for this instance/iteration and append any recursive calls
    endpoint_list = []

    # extract class types from lineage (in memory)
    # probably should call outside of this function - but will leave it for now
    classtypes = collect_classtypes_from_lineage(lineage_result)

    for lineage_hop in lineage_result["items"]:
        # get the out id
        in_id = lineage_hop["inId"]
        out_id = lineage_hop["outId"]

        # only process if the id matches the seed
        # since other lineage could be included
        if out_id == seed_id:
            # print(f"seed match - entry {iterations} ")
            # recursively call this function again???
            class_type = classtypes.get(in_id)
            if class_type != "com.infa.ldm.bdm.platform.Field":
                endpoint_list.append(in_id)
                # return_seeds.append(out_id)
            else:
                # still a platform field - so get the next iteration
                # call this function recursively
                nested_ids = get_lineage_endpoints_downstream(lineage_result, in_id)
                endpoint_list.extend(nested_ids)

    return endpoint_list


def collect_classtypes_from_lineage(lineage_result):
    """
    helper menthod to get the classtype and name from lineage links
    since they are embedded only the first refernce, this is easier than
    figuring out when navigating the lingage resultset itself
    """
    result_dict = {}
    for lineage_hop in lineage_result["items"]:
        in_id = lineage_hop["inId"]
        out_id = lineage_hop["outId"]

        if "inEmbedded" in lineage_hop:
            facts = lineage_hop["inEmbedded"]["facts"]
            for a_fact in facts:
                if a_fact["attributeId"] == "core.classType":
                    class_type = a_fact["value"]
                    result_dict[in_id] = class_type
                    break
        if "outEmbedded" in lineage_hop:
            facts = lineage_hop["outEmbedded"]["facts"]
            for a_fact in facts:
                if a_fact["attributeId"] == "core.classType":
                    class_type = a_fact["value"]
                    result_dict[out_id] = class_type
                    break
    # print(f"returning id/type dict==\n{result_dict}")
    return result_dict


def get_object_using_id(obj_id: str) -> dict:
    """
    get object using objects endpoint - helper method
    should move to common utils
    """
    # object_to_return = {}
    try:
        resturl = edcHelper.baseUrl + "/access/2/catalog/data/objects"
        # print(f"calling:  #{resturl}#")
        parms = {"id": obj_id}
        resp = edcHelper.session.get(resturl, params=parms, timeout=30)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        return {}

    if resp.status_code == 200:
        lookup_obj = resp.json()
        # lkp_count = lookup_obj["metadata"]["totalCount"]
    else:
        print(f"non 200 response: {resp.status_code} {resp.text}")
        return {}

    # print(f"found: {lkp_count} lookup to process\n--------\n")
    # print(lookup_obj["items"][0])
    real_obj = lookup_obj["items"][0]
    return real_obj


def get_lineage_for_object(object_id, direction: str, depth: int) -> dict:
    """
    execute a lineage/relationships call and return the results (json) as a dict
    """
    object_url = f"{edcHelper.baseUrl}/access/2/catalog/data/relationships"
    params = {
        "seed": object_id,
        "association": "com.infa.ldm.etl.DetailedDataFlow",
        "depth": depth,
        "direction": direction,
        "removeDuplicateAggregateLinks": "true",
        "includeTerms": "false",
        "includeAttribute": ["core.name", "core.classType"],
        "includeRefObjects": "true",
    }
    # print(f"relationships api call={object_url}")
    # print(f"\t\trelationships api parms=={params}")
    try:
        resp = edcHelper.session.get(object_url, params=params, timeout=10)
    except Exception as e:
        print("Error executing GET : " + object_url)
        print(e)
        return {}
        # raise HTTPException(status_code=500, detail=e)

    rels_result = resp.json()
    # print(f"\t\tlineage result has {len(rels_result['items'])} entries")
    return rels_result


def extract_lookup_fields(lookup_obj: dict) -> dict:
    """
    get the fields used in a lookup, via the group within the lookup
    """
    field_map = {}
    print("\tgetting lookup fields (via group)")
    for dst_obj in lookup_obj["dstLinks"]:
        # print(dst_obj)
        if dst_obj["classType"] == "com.infa.ldm.bdm.platform.Group":
            # print(f"\tlookup group found: {dst_obj['id']}")
            # get the group children
            logging.info(f"reading lookup fields group: using id={dst_obj['id']}")
            group_obj = get_object_using_id(dst_obj["id"])
            # iterate over the fields...
            print(f"\tlookup group obj has {len(group_obj['dstLinks'])} fields")
            for group_field in group_obj["dstLinks"]:
                name = group_field["name"]
                field_id = group_field["id"]
                field_map[name] = field_id

    logging.info(f"group fields: {field_map}")
    # test - lineage for all lookup fields downstream (seems to work)
    # param for max lineage hops???  20 enough?
    # get_lineage_for_object(field_map.values(), "OUT", 20)
    return field_map


def main():
    """
    description to be added here
    """
    print(f"BDM Scanner control flow lineage generator: version {version} starting")
    logging.info(
        f"BDM Scanner control flow lineage generator: version {version} starting"
    )
    print(f"using Python version: {platform.python_version()}")
    logging.info(f"using Python version: {platform.python_version()}")
    start_time = time.time()
    resourceName = ""
    outFolder = "./out"

    # get the command-line args passed
    cmd_parser = setup_cmd_parser()
    args, unknown = cmd_parser.parse_known_args()
    if args.setup:
        # if setup is requested (running standalone)
        # call setupConnection to create a .env file to use next time we run
        print("setup requested..., calling setupConnection & exiting")
        setupConnection.main()
        return

    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()
    edcHelper.validateConnection()
    print(f"EDC version: {edcHelper.edcversion_str} ## {edcHelper.edcversion}")

    print(f"command-line args parsed = {args} ")
    print()

    # print(type(args))
    if args.resourceName is not None:
        resourceName = args.resourceName
    else:
        print(
            "no resourceName specified - process cannot completed without a BDM Scanner resource"
        )
        return

    if args.outDir is not None:
        outFolder = args.outDir
        print(f"output folder={outFolder}")

    if not os.path.exists(outFolder):
        print(f"creating new output folder: {outFolder}")
        os.makedirs(outFolder)

    print(f"import to EDC: {args.edcimport}")

    bdm_lookup_ctlflow_links(resourceName, outFolder)

    # print a summary of the process
    logging.info("BDM lookup controlflow process completed")
    print(f"lineage file written: {mem.lineage_csv_filename}")
    logging.info(f"lineage file written: {mem.lineage_csv_filename}")
    print(f"Mappings processed: {mem.mapping_count}")
    logging.info(f"Mappings processed: {mem.mapping_count}")
    print(f"Lookups processed: {mem.lookup_count}")
    logging.info(f"Lookups processed: {mem.lookup_count}")
    print(f"element lineage links written: {mem.field_ctlflow_links}")
    logging.info(f"element lineage links written: {mem.field_ctlflow_links}")
    print(f"dataset lineage links written: {len(mem.dset_unique_links)}")
    logging.info(f"dataset lineage links written: {len(mem.dset_unique_links)}")

    print(f"run time = {time.time() - start_time} seconds ---")

    if not args.edcimport:
        print(
            "\ncustom lineage resource will not be created/updated/executed."
            " use -i|-edcimport flag to enable"
        )
        logging.info(
            "custom lineage resource will not be created/updated/executed."
            " use -i|--edcimport flag to enable"
        )
        return

    if mem.field_ctlflow_links == 0:
        print("custom lineage import skipped, 0 objects to import")
        return

    lineage_resource = resourceName + "_controlflow_lineage"
    lineage_fileonly = mem.lineage_csv_filename[
        mem.lineage_csv_filename.rfind("/") + 1 :
    ]
    print(
        "ready to create/update lineage resource..."
        f" {lineage_resource} from {mem.lineage_csv_filename} {lineage_fileonly}"
    )
    logging.info(
        "ready to create/update lineage resource..."
        f" {lineage_resource} from {mem.lineage_csv_filename} {lineage_fileonly}"
    )

    # create/update & start the custom lineage import
    logging.info("creating/updating resource: {lineage_resource} and starting scan")
    edcutils.createOrUpdateAndExecuteResourceUsingSession(
        edcHelper.baseUrl,
        edcHelper.session,
        lineage_resource,
        "template/custom_lineage_template.json",
        lineage_fileonly,
        mem.lineage_csv_filename,
        False,
        "LineageScanner",
    )

    # end of main process
    print("process complete")


def init_files(resourceName: str, outFolder: str):
    """
    open files for output - to be used for writing individual links
    """
    file_to_write = f"{outFolder}/{resourceName}_controlflow_lineage.csv"
    logging.info(f"initializing csv file: {file_to_write}")
    # create the files and store in mem object for reference later
    mem.fConnLinks = open(file_to_write, "w", newline="")
    mem.connectionlinkWriter = csv.writer(mem.fConnLinks)
    mem.connectionlinkWriter.writerow(
        [
            "Association",
            "From Connection",
            "To Connection",
            "From Object",
            "To Object",
            # "com.infa.ldm.etl.ETLContext"
        ]
    )

    mem.lineage_csv_filename = file_to_write


def close_files():
    # mem.allLinks.close()
    mem.fConnLinks.close()


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
