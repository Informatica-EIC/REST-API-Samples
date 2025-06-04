"""
Created September 5, 20223

BDM Scanner - lookup controlflow hack
usage:  bdm_scanner_ctrlflow_hack.py  <parms>

@author: dwrigley
"""

import csv
import os
import requests
from requests.structures import CaseInsensitiveDict
import sys
import argparse
import edcSessionHelper
import setupConnection
import logging
import edcutils
import urllib3
import time
import re

urllib3.disable_warnings()

version = "1.2"

# create log folder if it does not already exist
if not os.path.exists("./log"):
    print("creating log folder ./log")
    os.makedirs("./log")


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
    error_count = 0
    max_hops = 20
    mapping_name = None


class lineage_inspector:
    """
    class to encapsulate lineage traversal
    and to keep track of recursion details
    """

    # class variables
    max_recursion = 0
    # endpoint_list = []
    # classtypes = []
    # ids_processed = []
    # recursion_level = 0
    # seed_ids = []
    # class_types = []

    def __init__(self):
        # instance variables
        self.endpoint_list = []
        self.classtypes = []
        self.ids_processed = []
        self.recursion_level = 0
        self.seed_ids = []
        self.class_types = []

    def get_downstream_endpoints(self, lineage_result: dict, seed_id) -> list:
        """
        external entry point for lineage traversal upstream
        returns the elements connected downstream, that are not platform fields
            will not traverse any links past non-platform fields
        """
        # reset object variables
        self.endpoint_list.clear()
        self.classtypes.clear()
        self.ids_processed.clear()
        self.recursion_level = 0

        # extract the classtypes - for easier reference later
        self.classtypes = self.collect_classtypes_from_lineage(lineage_result)

        logging.debug(
            f"calling _get_lineage_endpoints_downstream: for seed id={seed_id} "
            f"linage size={len(lineage_result)}"
        )

        # call the internal/recursive function, returns id's that are endpoints
        seed_ids = self._get_lineage_endpoints_downstream(lineage_result, seed_id)

        logging.debug(
            f"_get_lineage_endpoints_downstream: returning {len(seed_ids)} objects"
        )
        return seed_ids

    def get_upstream_endpoints(self, lineage_result: dict, seed_id) -> list:
        """
        external entry point for lineage traversal upstream
        returns the elements connected upstream, that are not platform fields
            will not traverse any links past non-platform fields
        """
        # reset object variables
        self.endpoint_list.clear()
        self.classtypes.clear()
        self.ids_processed.clear()
        self.recursion_level = 0

        # extract the classtypes - for easier reference later
        self.classtypes = self.collect_classtypes_from_lineage(lineage_result)

        logging.debug(
            f"calling _get_lineage_endpoints_upstream: for seed id={seed_id} "
            f"linage size={len(lineage_result)}"
        )

        # call the internal/recursive function, returns id's that are endpoints
        seed_ids = self._get_lineage_endpoints_upstream(lineage_result, seed_id)

        logging.debug(
            f"_get_lineage_endpoints_upstream: returning {len(seed_ids)} objects"
        )
        return seed_ids

    def _get_lineage_endpoints_upstream(self, lineage_result: dict, seed_id) -> list:
        """
        recursive function, to get the upstream endpoints
        these are dataelements that are not com.infa.ldm.bdm.platform.Field instances

        we could mix upstream and downstream in a single function, but it is
        harder to understand so keep seperate for now
        """
        endpoint_list = []
        self.recursion_level += 1
        if self.recursion_level > lineage_inspector.max_recursion:
            lineage_inspector.max_recursion = self.recursion_level

        # add this seed, as being processed (for recursion error check)
        self.ids_processed.append(seed_id)

        for lineage_hop in lineage_result["items"]:
            # get the out id
            in_id = lineage_hop["inId"]
            out_id = lineage_hop["outId"]

            # only process if the id matches the seed
            # since other lineage could be included
            if in_id == seed_id:
                # error - if this is a recursive link (to self, skip)
                if in_id == out_id:
                    # print(f"skipping recursive link: {in_id}")
                    logging.info(f"skipping recursive link: {in_id}")
                    continue

                # print(f"seed match - entry {iterations} ")
                # recursively call this function again???
                class_type = self.classtypes.get(out_id)
                if class_type != "com.infa.ldm.bdm.platform.Field":
                    endpoint_list.append(out_id)
                    # return_seeds.append(out_id)
                else:
                    # still a platform field - so get the next iteration
                    # call this function recursively
                    # possible circular reference - so check if already processed
                    if out_id in self.ids_processed:
                        print(f"\tcircular reference found for: {out_id}, skipping")
                        # print("lineage: with issue\n")
                        # print(lineage_result)
                        logging.debug(
                            f"circular reference found for: {out_id}, skipping."
                            # f" lineage={lineage_result}"
                        )
                        continue
                    nested_ids = self._get_lineage_endpoints_upstream(
                        lineage_result, out_id
                    )
                    endpoint_list.extend(nested_ids)

        return endpoint_list

    def _get_lineage_endpoints_downstream(self, lineage_result: dict, seed_id) -> list:
        """
        recursive (internal) function, to get the downstream endpoints
        these are dataelements that are not com.infa.ldm.bdm.platform.Field instances

        we could mix upstream and downstream in a single function, but it is harder
        to understand so keep seperate for now
        """
        # list to return - initialize, for this instance/iteration and append any
        # recursive calls
        endpoint_list = []
        self.recursion_level += 1
        if self.recursion_level > lineage_inspector.max_recursion:
            lineage_inspector.max_recursion = self.recursion_level

        # add this seed, as being processed (for recursion error check)
        self.ids_processed.append(seed_id)

        for lineage_hop in lineage_result["items"]:
            # get the out id
            in_id = lineage_hop["inId"]
            out_id = lineage_hop["outId"]

            # only process if the id matches the seed
            # since other lineage could be included
            if out_id == seed_id:
                if in_id == out_id:
                    logging.info(f"skipping recursive link: {in_id}")
                    continue
                # print(f"seed match - entry {iterations} ")
                # recursively call this function again???
                class_type = self.classtypes.get(in_id)
                if class_type != "com.infa.ldm.bdm.platform.Field":
                    endpoint_list.append(in_id)
                    # return_seeds.append(out_id)
                else:
                    # still a platform field - so get the next iteration
                    # call this function recursively
                    if in_id in self.ids_processed:
                        print("\tcircular reference found for: {in_id}, skipping")
                        # print("lineage: with issue\n")
                        # print(lineage_result)
                        logging.debug(
                            f"circular reference found for: {in_id}, skipping."
                            # f" lineage={lineage_result}"
                        )
                        continue

                    nested_ids = self._get_lineage_endpoints_downstream(
                        lineage_result, in_id
                    )
                    endpoint_list.extend(nested_ids)

        return endpoint_list

    def collect_classtypes_from_lineage(self, lineage_result):
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

    # end of class lineage_inspector


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
        "--mapping",
        help=(
            "search for a specific mapping by name "
            "(will add a fq core.name:<name-entered>)"
        ),
    )

    parser.add_argument(
        "--maxhops",
        required=False,
        type=int,
        default=20,
        help="max lineage hops for relationships api call.  0=everything, default=20",
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
        "--debug", default=False, action="store_true", help="set logging to DEBUG "
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
    """
    main controlling process - find mappings for resource
    and process each one.
    use paging model (100 mappings at a time), until complete
    """
    print("ready to find mappings that have lookups..")

    init_files(resource_name, out_folder)

    # need to process in pages, since there could be large numbers
    # of mappings to read

    total = 1000  # initial value - set to > 0 - will replaced on first call
    offset = 0
    page = 0
    page_size = 100
    query = "core.classType:com.infa.ldm.bdm.platform.Mapping"
    fq = [f"core.resourceName:{resource_name}"]
    if mem.mapping_name is not None:
        fq.append(f"core.name:{mem.mapping_name}")
    item_count = 0

    resturl = edcHelper.baseUrl + "/access/2/catalog/data/objects"
    # print(f"calling:  #{resturl}#")
    print(
        f"search for platform mappings using query={query} fq={fq}, "
        f"pagesize={page_size}"
    )

    logging.info(
        f"search for platform mappings using query={query} fq={fq}, "
        f"pagesize={page_size}"
    )

    # main loop - until all items have been processed (in pages)
    while offset < total:
        page_time = time.time()
        parameters = {"q": query, "fq": fq, "offset": offset, "pageSize": page_size}
        page += 1
        resp = edcHelper.session.get(resturl, params=parameters, timeout=30)
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            break

        resultJson = resp.json()
        total = resultJson["metadata"]["totalCount"]
        mem.mapping_total = total
        print(
            f"objects found: {total} offset: {offset} "
            f"pagesize={page_size} currentPage={page} "
            f"objects {offset+1} - {offset+page_size}"
        )
        logging.info(
            f"objects found: {total} offset: {offset} "
            f"pagesize={page_size} currentPage={page} "
            f"objects {offset+1} - {offset+page_size}"
        )

        for foundItem in resultJson["items"]:
            item_count += 1
            process_mapping(foundItem)

        # end of page processing
        print("\tpage processed - %s seconds ---" % (time.time() - page_time))
        logging.info(
            f"\tpage processed ({offset+1}-{offset+page_size}) - "
            f"{(time.time() - page_time)} seconds ---"
        )

        # for next iteration
        offset += page_size

    print("Mapping processing complete")
    close_files()


def process_mapping(mapping_obj: dict):
    """
    from the mappiung - inspect dstLinks to get tx objects
    looking for lookup transformations
    we can;t search for these, since they are not 1st class objects
    """
    mem.mapping_count += 1
    print(
        f"processing mapping: {mem.mapping_count} of {mem.mapping_total} - "
        f"{mapping_obj['id']}"
    )
    logging.info(
        f"processing mapping: {mem.mapping_count} of {mem.mapping_total} - "
        f"{mapping_obj['id']}"
    )
    for linked_obj in mapping_obj["dstLinks"]:
        if linked_obj["classType"] == "com.infa.ldm.bdm.platform.LookUpTransformation":
            print(f"\tlookup found: {linked_obj['name']}")
            process_lookup(linked_obj.get("id"))


def process_lookup(lookup_id: str):
    """
    process a lookup tx
    TODO: re-factor into smaller methods
    """
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
    if lkp_count != 1:
        print(f"ERROR: lookup count returned should be 1 ({lkp_count}) returned")
        return

    # print(lookup_obj["items"][0])
    real_obj = lookup_obj["items"][0]
    lookup_condition = edcutils.getFactValue(
        real_obj, "com.infa.ldm.bdm.platform.lookupCondition"
    )
    lookup_id = real_obj["id"]
    print(f"\tlookup condition={lookup_condition}")
    logging.info(f"lookup condition={lookup_condition}")

    lookup_statements = lookup_condition.split(" AND ")
    print(f"\tlookup statements found = {len(lookup_statements)}")

    lookup_fields = extract_lookup_fields(real_obj)
    # lookup fields should have a dict = name:id  (for all lookup fields)
    # instead of running lineage for all fields downstream - just do it for all fields
    # it should be faster than getting individual fields and determining if in/out/both

    # workaround: requests.CaseInsensitiveDict returns different structure for keys()
    # use list comprehension to get a list of keys for logging/printing
    lookup_keys = [x for x in lookup_fields.keys()]

    # minimizing logging - don't need tp print the fields here,
    # only do it if there are lookup issues
    print(f"\t{len(lookup_fields)} lookup fields found")
    logging.info(f"{len(lookup_fields)} lookup fields found")

    print(f"\tanalyzing lookup statements: {len(lookup_statements)}")
    for statement in lookup_statements:
        # split on comparison operator (=, !=, etc)
        field_refs = re.split(r"\s{1}(=|!=|>|>=|<|<=)\s{1}", statement)

        # length should be 3 - left, operator, right
        # field_refs = statement.replace("!=", "=").split(" = ")
        # should be always 2 objects
        print(f"\tfields compared = {field_refs}")
        if len(field_refs) != 3:
            print(f"Error: statement split returned <> 3 results: {field_refs}")
            logging.error(f"statement split returned <> 3 results: {field_refs}")
            continue
        left_name = field_refs[0]
        right_name = field_refs[2]
        left_id = ""
        right_id = ""
        # left_obj = {}
        # right_obj = {}

        # note: needs to be case-insensitive key (so just translate to upper case?)
        if left_name.lower() in lookup_fields:
            # left_obj = lookup_fields.get(left_name)
            # left_id = left_obj.get("id")
            left_id = lookup_fields.get(left_name)
        else:
            mem.error_count += 1
            print(
                f"error: cannot find left lookup field with name:{left_name} "
                f"in {lookup_fields}"
            )
            logging.error(
                f"cannot find left lookup field with name:{left_name} "
                f"in {lookup_fields}"
            )

        if right_name in lookup_fields:
            # right_obj = lookup_fields.get(right_name)
            # right_id = right_obj.get("id")
            right_id = lookup_fields.get(right_name)
        else:
            mem.error_count += 1
            print(
                f"error: cannot find right lookup field with name:{right_name} "
                f"in {lookup_keys}, control flow lineage will not be created"
            )
            logging.error(
                f"cannot find right lookup field with name:{right_name} "
                f"in {lookup_keys}, control flow lineage will not be created"
            )

        print(f"\tlookup left: {left_id}")
        print(f"\tlookup rght: {right_id}")
        logging.info(f"left lookup ={left_id}")
        logging.info(f"right lookup={right_id}")

        if left_id == "" or right_id == "":
            # no point processing, of both left and right object
            # for comparison are not found
            continue

        lineage_left_upstream = get_lineage_for_object(left_id, "IN", 1)
        # left_up_count = 0
        left_up_count = (
            len(lineage_left_upstream["items"])
            if "items" in lineage_left_upstream
            else 0
        )
        lineage_right_upstream = get_lineage_for_object(right_id, "IN", mem.max_hops)
        right_up_count = (
            len(lineage_right_upstream["items"])
            if "items" in lineage_right_upstream
            else 0
        )
        # get upstream lineage from right object (incoming for comparison)
        # get upstream lineage from left objects??? needed???
        all_lineage_downstream = get_lineage_for_object(
            lookup_fields.values(), "OUT", mem.max_hops
        )
        downstream_lineage_count = (
            len(all_lineage_downstream["items"])
            if "items" in all_lineage_downstream
            else 0
        )

        print(
            f"\tlineage endpoints found.... {left_up_count}/{right_up_count}"
            f"/{downstream_lineage_count}"
        )
        logging.info(
            f"lineage endpoints found.... {left_up_count}/{right_up_count}"
            f"/{downstream_lineage_count}"
        )

        # if any upstream or downstream links are 0, we have a problem
        # log it and go to next (since will have incomplete links)
        if left_up_count == 0 or right_up_count == 0 or downstream_lineage_count == 0:
            logging.error("error - 0 lineage links up/down")
            print("error - 0 lineage links up/down")
            continue

        # get downstream lineage from returned all output objects
        # this is included in all_lineage downstream so no longer needed
        # lineage_left_downstream = get_lineage_for_object(left_id, "OUT", 10)
        # lineage_right_downstream = get_lineage_for_object(right_id, "OUT", 10)

        lineage_inspector_inst = lineage_inspector()
        left_up_ids = lineage_inspector_inst.get_upstream_endpoints(
            lineage_left_upstream, left_id
        )

        right_up_ids = lineage_inspector_inst.get_upstream_endpoints(
            lineage_right_upstream, right_id
        )

        # print("ready to connect the dots...")
        print(f"\t left field - upstream  :: {left_up_ids}")
        print(f"\tright field - upstream  :: {right_up_ids}")
        logging.info(f" left field - upstream  :: {left_up_ids}")
        logging.info(f"right field - upstream  :: {right_up_ids}")

        # for each lineage field id - get the downstream lineage
        down_lineage_endpoints = []
        for lkp_field_id in lookup_fields.values():
            ds_ids = lineage_inspector_inst.get_downstream_endpoints(
                all_lineage_downstream, lkp_field_id
            )

            if len(ds_ids) > 0:
                # print("exdend...")
                down_lineage_endpoints.extend(ds_ids)
            # print(f"found lineage for id={lkp_field_id} - {ds_ids}")
        print(f"\tall downstream lineage size={len(down_lineage_endpoints)}")
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
                logging.debug(
                    f"writing lineage: core.DirectionalControlFlow from: {left}"
                    f" to {right}"
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
                    logging.debug(
                        "writing lineage: core.DataSetControlFlow from: "
                        f"{left_parent} to {right_parent}"
                    )
        print(f"\tcontrol flow/lineage links created: {links_created}")


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
        status_code = resp.status_code
    except Exception as e:
        print("Error executing GET : " + object_url)
        print(e)
        return {}
        # raise HTTPException(status_code=500, detail=e)

    # check the return code - if not 200 then log and return empty dict
    # status_code = resp.status
    if status_code != 200:
        print(f"rc={status_code} returned from lineage api call {resp.json()}")
        logging.error(f"rc={status_code} returned from lineage api call: {resp.text}")
        mem.error_count += 1
        rels_result = {}
    else:
        rels_result = resp.json()
    # print(f"\t\tlineage result has {len(rels_result['items'])} entries")
    return rels_result


def extract_lookup_fields(lookup_obj: dict) -> CaseInsensitiveDict:
    """
    get the fields used in a lookup, via the group within the lookup
    """
    # test case insensitive dict (bug reported)
    field_map_cis = CaseInsensitiveDict()
    print("\tgetting lookup fields (via group)")
    for dst_obj in lookup_obj["dstLinks"]:
        # print(dst_obj)
        if dst_obj["classType"] == "com.infa.ldm.bdm.platform.Group":
            # print(f"\tlookup group found: {dst_obj['id']}")
            # get the group children
            # logging.info(f"reading lookup fields group: using id={dst_obj['id']}")
            group_obj = get_object_using_id(dst_obj["id"])
            # iterate over the fields...
            print(f"\tlookup group obj has {len(group_obj['dstLinks'])} fields")
            for group_field in group_obj["dstLinks"]:
                name = group_field["name"]
                field_id = group_field["id"]
                field_map_cis[name] = field_id

    # logging.info(f"group fields: {field_map}")
    # logging.info(f"lookup/group fields: {field_map_cis}")

    # return field_map
    return field_map_cis


def main():
    """
    description to be added here
    """
    print(f"BDM Scanner control flow lineage generator: version {version} starting")
    # print(f"using Python version: {platform.python_version()}")
    # logging.info(f"using Python version: {platform.python_version()}")
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

    if args.resourceName is not None:
        resourceName = args.resourceName
    else:
        print(
            "no resourceName specified - "
            "process cannot completed without a BDM Scanner resource"
        )
        return

    log_suffix = ""

    if args.mapping is not None:
        # store mapping name - for search
        print(f"\tstoring mapping={args.mapping} to filter search")
        mem.mapping_name = args.mapping
        log_suffix = "__" + args.mapping

    # default logging level
    logging_level = logging.INFO

    if args.debug:
        print("debug logging enabled")
        logging_level = logging.DEBUG

    log_filename = f"./log/bdm_scanner_ctlflow_{resourceName}{log_suffix}.log"
    print(f"log file: {log_filename}")

    # init logging
    logging.basicConfig(
        format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
        level=logging_level,
        filename=log_filename,
        # filename=datetime.now().strftime("./log/bdm_scanner_controlflow_gen.log"),
        filemode="w",
    )
    logging.info(
        f"BDM Scanner control flow lineage generator: version {version} starting"
    )

    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()
    edcHelper.validateConnection()
    print(f"EDC version: {edcHelper.edcversion_str} ## {edcHelper.edcversion}")

    print(f"command-line args parsed = {args} ")
    print()

    if args.outDir is not None:
        outFolder = args.outDir
        print(f"output folder={outFolder}")

    if not os.path.exists(outFolder):
        print(f"creating new output folder: {outFolder}")
        os.makedirs(outFolder)

    # store max hops
    if args.maxhops != mem.max_hops:
        # overriding default # of hops
        if args.maxhops < 0:
            print(
                f"negative value passed for maxhops {args.maxhops}, "
                f"keeping default: {mem.max_hops}"
            )
        else:
            print(f"overriding lineage max hops from 20, to {args.maxhops}")
            mem.max_hops = args.maxhops

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
    print(f"Errors Found: {mem.error_count}")
    logging.info(f"Errors Found: {mem.error_count}")
    print(f"element lineage links written: {mem.field_ctlflow_links}")
    logging.info(f"element lineage links written: {mem.field_ctlflow_links}")
    print(f"dataset lineage links written: {len(mem.dset_unique_links)}")
    logging.info(f"dataset lineage links written: {len(mem.dset_unique_links)}")
    print(f"max reursion from lineage: {lineage_inspector.max_recursion}")
    logging.info(f"max reursion from lineage: {lineage_inspector.max_recursion}")

    print(f"run time = {time.time() - start_time} seconds ---")

    if not args.edcimport:
        print(
            "\ncustom lineage resource will not be created/updated/executed."
            " use -i|--edcimport flag to enable"
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
    if mem.mapping_name is not None:
        lineage_resource = lineage_resource + "_" + mem.mapping_name
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
    if mem.mapping_name is not None:
        file_to_write = (
            f"{outFolder}/{resourceName}_controlflow_lineage_{mem.mapping_name}.csv"
        )
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
