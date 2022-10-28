"""
Created January, 2022

create custom lineage csv file export from xdocs
this is useful where scanners are not producing correct lineage, or you just need to
get a good idea of the objects that should be linked.

it allows the user to specify regex find/replace strings via config/substitition csv
file (can be multiple)

Note:  this works with EDC 10.5+ only (10.4.x processing removed)

usage:  xdoc_lineage_gen.py  <parms>

@author: dwrigley
"""

import json
import csv
import pprint
import os
import requests
import sys
import argparse
import edcSessionHelper
from zipfile import ZipFile
import setupConnection
import re
import logging
import edcutils
import urllib3
import time

urllib3.disable_warnings()


if not os.path.exists("./log"):
    print("creating log folder ./log")
    os.makedirs("./log")

logging.basicConfig(
    format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
    level=logging.DEBUG,
    filename="./log/xdoc_lineage_gen.log",
    filemode="w",
)

# create the EDC session helper class
edcHelper = edcSessionHelper.EDCSession()


class mem:
    """
    in memory objects (preferred over global vars.
    (should probably convert this functional script to a class)
    """

    assocCounts = {}
    objCounts = {}
    leftRightConnectionObjects = {}
    connectables = set()
    connectionStats = {}
    connectionInstances = {}
    subst_dict = {}
    subst_count = 0

    lineage_csv_filename = ""
    links_written = 0

    lineCount = 0
    data = []
    totalLinks = 0
    totalObjects = 0
    leftExternalCons = 0
    rightExternalCons = 0
    allproperties = 0
    notnull_properties = 0
    propDict = {}
    refObjects = 0

    not_changed_count = 0

    # fConnLinks: File


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
        help="resource name for xdoc download",
    )
    parser.add_argument(
        "-rt",
        "--resourceType",
        required=False,
        help="resource type (provider id) for xdoc download",
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
        "-f",
        "--force",
        action="store_true",
        required=False,
        help=(
            "force overwrite of xdoc json file (if already existing), "
            "otherwise just use the .json file"
        ),
    )

    parser.add_argument(
        "--modifiedonly",
        action="store_true",
        required=False,
        help=("export only modified lineage links (not all external links)"),
    )

    parser.add_argument(
        "-sf",
        "--subst_file",
        required=False,
        help="regex substitition file (header: from_regex,replace_expr)",
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


def main():
    """
    main function - determines if edc needs to be queried to download xdocs
    (if file is not there, or if it is and force=True)
    downloads the xdocs then calls the function to read the xdocs and print a summary
    """

    start_time = time.time()
    resourceName = ""
    resourceType = ""
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
            "no resourceName specified - we can't download xdocs without knowing "
            "what resource name to use. exiting"
        )
        return

    if args.resourceType is None:
        print("we have a resource name - but no type - need to look it up")
        resourceType = getResourceType(resourceName, edcHelper.session)

    else:
        resourceType = args.resourceType

    if args.outDir is not None:
        outFolder = args.outDir
        print(f"output folder={outFolder}")

    if not os.path.exists(outFolder):
        print(f"creating new output folder: {outFolder}")
        os.makedirs(outFolder)

    print(f"force={args.force}")
    print(f"import to EDC: {args.edcimport}")

    if args.subst_file:
        print(f"substitution file referenced in command-line {args.subst_file}")
        mem.subst_dict = read_subst_regex_file(args.subst_file)

    # ready to extract/process xdocs - >=10.5 zip (older 10.4.x not supported))
    if edcHelper.edcversion < 105000:
        print(
            "EDC <= 10.5.0 is not supported for this utility script,"
            f" {edcHelper.edc_build_vers} found.  exiting"
        )
        logging.ERROR(
            "EDC <= 10.5.0 is not supported for this utility script,"
            f" {edcHelper.edc_build_vers} found.  exiting"
        )
        return

    print("EDC 10.5+ found... calling 10.5 exdoc lineage gen")
    # get and process the xdocs
    get_exdocs_zip(resourceName, resourceType, outFolder, args.force)

    logging.info("xdoc_lineage_gen process completed")
    print(f"lineage file written: {mem.lineage_csv_filename}")
    logging.info(f"lineage file written: {mem.lineage_csv_filename}")
    print(f"lineage links written: {mem.links_written}")
    logging.info(f"lineage links written: {mem.links_written}")
    print(f"lineage links skipped (unchanged): {mem.not_changed_count}")
    logging.info(f"lineage links skipped (unchanged): {mem.not_changed_count}")

    print(f"run time = {time.time() - start_time} seconds ---")

    if not args.edcimport:
        print(
            "\ncustom lineage resource will not be created/updated/executed."
            " use -i|-edcimport flag to enable"
        )
        logging.info(
            "custom lineage resource will not be created/updated/executed."
            " use -i|-edcimport flag to enable"
        )
        return ()

    lineage_resource = resourceName + "_lineage"
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


def read_subst_regex_file(subst_filename: str) -> dict:
    """
    read .csv file with 2 columns from_regex,replace_expr
    store values in a dict to return to caller for processing
    """
    print(f"\treading regex substitution file from: {subst_filename}")
    subst_dict = {}
    try:
        with open(subst_filename, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                from_regex = row["from_regex"].strip()
                to_expr = row["replace_expr"].strip()
                print(f"\t\tregex={from_regex} replace-expr={to_expr}")
                subst_dict[from_regex] = to_expr
    except FileNotFoundError:
        print(f"error finding file: {subst_filename}, process aborted.")
        logging.error(f"error finding file: {subst_filename}, process aborted.")
        exit()

    print(f"\t{len(subst_dict)} substitions found")
    return subst_dict


def getResourceType(resourceName, session) -> str:
    """
    get the resource type from EDC, and extract the scannerId (provider id)
    """
    resourceType = None
    try:
        resturl = session.baseUrl + "/access/1/catalog/resources/" + resourceName
        print(f"calling:  #{resturl}#")
        resp = session.get(resturl, timeout=30)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        return None

    if resp.status_code == 200:
        resJson = resp.json()
        resourceType = resJson["scannerConfigurations"][0]["scanner"]["scannerId"]
    else:
        print(f"non 200 response: {resp.status_code} {resp.text}")

    print(f"returning: {resourceType}")
    return resourceType


def get_exdocs_zip(
    resource_name: str, resource_type: str, out_folder: str, force_extract: bool
):
    """
    get the zipped xdocs from EDC (or use from cached file)
    """
    print(f"xdoc analyzer 10.5+ version starting extract for resource={resource_name}")
    xdocurl = f"{edcHelper.baseUrl}/access/1/catalog/data/downloadXdocs"
    parms = {"resourceName": resource_name, "providerId": resource_type}
    print(f"executing xdoc download via endpoint: {xdocurl} with params={parms}")

    xdoc_filename = out_folder + "/" + resource_name + "__" + resource_type + ".zip"

    # only get xdocs .zip file if either new, or forced (use cache otherwise)
    if (not os.path.isfile(xdoc_filename)) or (
        os.path.isfile(xdoc_filename) and force_extract
    ):
        print("generate new xdocs...")
        try:
            resp = edcHelper.session.get(xdocurl, params=parms, timeout=20)
            if resp.status_code == 200:
                logging.info("successfully downloaded xdoc file")
                with open(xdoc_filename, "wb") as outfile:
                    outfile.write(resp.content)
                    print(f"xdoc size={len(resp.content)}")
                    print(f"xdoc files written to {xdoc_filename}")
                    logging.info(
                        f"xdoc files written to {xdoc_filename}"
                        f" size={len(resp.content)}"
                    )
            else:
                print(
                    f"api call returned non 200 status_code: {resp.status_code} "
                    f" {resp.text}"
                )

        except requests.exceptions.RequestException as e:
            print("Exception raised when executing edc query: " + xdocurl)
            print(e)

    init_files(resource_name, out_folder)
    archive = ZipFile(xdoc_filename, "r")
    files = archive.namelist()
    print(f"xdoc files in zip = {len(files)}")
    process_xdocs_zipped(xdoc_filename, resource_name, out_folder)
    close_files()


def process_xdocs_zipped(zipfileName: str, resourceName: str, outFolder: str):
    """
    read a .zip file that was saved/downloaded with all xdocs.
    """
    archive = ZipFile(zipfileName, "r")
    files = archive.namelist()
    info_list = archive.infolist()
    mem.lineCount = len(files)

    with archive as zipped_xdocs:
        for pos, xdfile in enumerate(files):
            with zipped_xdocs.open(xdfile) as xdoc_file:
                print(
                    f"\tprocessing file {pos+1} of {len(files)}:"
                    f" {xdfile} size={info_list[pos].file_size/1024:,.0f} KB"
                )
                logging.info(f"processing xdoc file: {xdfile}")
                # error for very large xdoc files (3gb) - need to research
                try:
                    data = json.loads(xdoc_file.read())
                    process_xdoc_json(data)
                except Exception as e:
                    print("error reading file.")
                    print(e)

    # write results to file(s)
    write_xdoc_results(resourceName, outFolder)


def init_files(resourceName: str, outFolder: str):
    """
    open files for output - to be used for writing individual links
    """
    file_to_write = f"{outFolder}/lineage_{resourceName}_xdoc_generated.csv"
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


def write_xdoc_results(resourceName: str, outFolder: str):
    """
    process has finished - print summary to screen & write some output summaries
    """
    print("totals-------------------------------------------------")
    print("total json files: " + str(mem.lineCount))
    print("total links: " + str(mem.totalLinks))
    print("total connections: " + str(len(mem.connectionStats)))
    print("total upstream linkable objects:" + str(mem.leftExternalCons))
    print("total downstream linkable objects:" + str(mem.rightExternalCons))

    logging.info(f"total json files: {mem.lineCount}")
    logging.info(f"total links: {mem.totalLinks}")
    logging.info(f"total connections: {len(mem.connectionStats)}")
    logging.info(f"total upstream linkable objects: {mem.leftExternalCons}")
    logging.info(f"total downstream linkable objects: {mem.rightExternalCons}")

    pp = pprint.PrettyPrinter(depth=6)
    print("\nconnection stats")
    pp.pprint(mem.connectionStats)

    print(f"regex substitutions completed: {mem.subst_count}")


def process_xdoc_json(data):
    """
    process a single xdoc entry (json doc)
    """
    # print(f"json line length: {len(line)}", end="")
    linkCount, replaced_count = process_xdoc_links(data)
    mem.totalLinks += linkCount

    print(f"\tlinks: {linkCount} replaced={replaced_count}")
    logging.info(f"links: {linkCount} replaced={replaced_count}")


def replace_via_regex(from_string) -> str:
    """
    use the regex substitutions in mem.subst_dict
    replace all values and return the new string
    """
    replaced_val = from_string
    for regex, subst_expr in mem.subst_dict.items():
        # print(f"\treplacing regex {regex} with {subst_expr} in {replaced_val}")
        p = re.compile(regex)
        new_val = p.sub(subst_expr, replaced_val)
        if new_val != replaced_val:
            logging.info(
                f"replaced... \n\tfrom={replaced_val} \n\t"
                f"to  ={new_val} \n\t\tusing regex='{regex}' and subst='{subst_expr}'"
            )
            replaced_val = new_val
            if replaced_val[:1] != "$":
                logging.info(
                    f"probable edge case: {from_string} does not appear to be a "
                    "connection reference, but was substitited"
                )

    return replaced_val


def split_connection_from_id(string_to_split: str):
    con = ""
    id = string_to_split
    if string_to_split.startswith("${"):
        con = string_to_split[2 : string_to_split.rfind("}")]
        id = string_to_split[len(con) + 4 :]

    return con, id


def process_xdoc_links(data):
    """
    process the links within a json doc
    """
    # p = re.compile(r'\#\$\[(\S+)[^\/]*\#')

    linkCount = 0
    replaced_count = 0
    for links in data["links"]:
        linkCount += 1
        is_substituted = False
        to_conn = ""
        fromId = links.get("fromObjectIdentity")
        toId = links.get("toObjectIdentity")
        assoc = links.get("association")

        # extract connection names & new id's
        # get standard/defaults for from/to objects and the connection
        from_conn, from_actual_id = split_connection_from_id(fromId)
        to_conn, to_actual_id = split_connection_from_id(toId)

        # overall association counts
        if assoc in mem.assocCounts.keys():
            mem.assocCounts[assoc] = mem.assocCounts[assoc] + 1
        else:
            mem.assocCounts[assoc] = 1

        has_connection = False

        if links.get("fromObjectIdentity").startswith("${"):
            # it is a connection link
            orig_fromId = links.get("fromObjectIdentity")
            fromId = replace_via_regex(orig_fromId)
            if fromId != orig_fromId:
                mem.subst_count += 1
                replaced_count += 1
                is_substituted = True

            from_conn, from_actual_id = split_connection_from_id(fromId)

            has_connection = True
            if len(from_actual_id) == 0:
                # remove the <conn_name>.  leaving only the schema name
                from_actual_id = from_conn[from_conn.find(".") + 1 :]
                logging.info(
                    f"\tprobably a schema link... {assoc} {from_conn} removing "
                    f"chars left of . for the schema name {from_actual_id}"
                )

            mem.leftExternalCons += 1

            #  connection counts
            cStats = mem.connectionStats.get(from_conn, {})
            connLinkCount = cStats.get(assoc, 0)
            connLinkCount += 1
            cStats[assoc] = connLinkCount
            mem.connectionStats[from_conn] = cStats

        if links.get("toObjectIdentity").startswith("${"):
            orig_toId = links.get("toObjectIdentity")
            toId = replace_via_regex(orig_toId)
            if toId != orig_toId:
                mem.subst_count += 1
                replaced_count += 1
                is_substituted = True

            to_conn, to_actual_id = split_connection_from_id(toId)

            has_connection = True
            mem.rightExternalCons += 1

            #  connection counts
            cStats = mem.connectionStats.get(to_conn, {})
            connLinkCount = cStats.get(assoc, 0)
            connLinkCount += 1
            cStats[assoc] = connLinkCount
            mem.connectionStats[to_conn] = cStats

        if has_connection:
            if is_substituted:
                mem.links_written += 1
                mem.connectionlinkWriter.writerow(
                    [
                        assoc,
                        from_conn,
                        to_conn,
                        from_actual_id,
                        to_actual_id,
                    ]
                )
            else:
                mem.not_changed_count += 1

    return linkCount, replaced_count


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
