"""
Created November, 2022

validate custom lineage files that use direct id's (not connection assignment)
write results to a file that replicates the lineage file, with extra columns
identifying if the left/right objects are valid, if the link is already created
and any messages

usage:  lineage_validator.py  -lf <file>

@author: dwrigley
"""

import json
import csv
import os
import argparse
import edcSessionHelper
import setupConnection
import logging
import urllib3
import time
from datetime import datetime
import re

urllib3.disable_warnings()

resource_map = {}
cs_reversals = []

from_valid_header = "From Valid"
to_valid_header = "To Valid"
comments_header = "Comments"
link_exists_header = "Link Exists"

ref_id_regex = r"([\w\-_]+)\$\$([\w\-_]+)\$\$([\w\-_\/]+):\/\/~proxy~\/([\w\-_\/]+)"

if not os.path.exists("./log"):
    print("creating log folder ./log")
    os.makedirs("./log")

logging.basicConfig(
    format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
    level=logging.DEBUG,
    # filename=datetime.now().strftime("./log/lineage_validator_%Y-%m-%d_%H-%M-%S.log"),
    filename=datetime.now().strftime("./log/lineage_validator.log"),
    filemode="w",
)

# initialize edc helper (used for http session etc)
edcHelper = edcSessionHelper.EDCSession()


def setup_cmd_parser():
    # define script command-line parameters (in global scope for gooey/wooey)
    parser = argparse.ArgumentParser(parents=[edcHelper.argparser])

    parser.add_argument(
        "-lf",
        "--lineage_file",
        required=False,
        help=(
            "Lineage file to check - "
            "results written to same file with _validated.csv suffix"
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

    # get the command-line args passed
    cmd_parser = setup_cmd_parser()
    args, unknown = cmd_parser.parse_known_args()
    if args.setup:
        # if setup is requested (running standalone)
        # call setupConnection to create a .env file to use next time we run
        print("setup requested..., calling setupConnection & exiting")
        setupConnection.main()
        return

    print("Lineage Validator process started.")
    print(f"Log file created: {logging.getLoggerClass().root.handlers[0].baseFilename}")

    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()
    edcHelper.validateConnection()
    print(f"EDC version: {edcHelper.edcversion_str} ## {edcHelper.edcversion}")

    print(f"command-line args parsed = {args} ")
    print()

    # start the lineage check process
    if args.lineage_file:
        print(f"lineage file to check {args.lineage_file}")
        validate_lineage_file(args.lineage_file)

    # print results to console
    print(f"resources referenced: {len(resource_map)}")
    if len(resource_map) > 0:
        print(resource_map)

    print(f"case sensitive search reversals: {len(cs_reversals)}")
    if len(cs_reversals):
        print(cs_reversals)

    logging.info(f"process completed. {time.time() - start_time:.2f} seconds ---")
    print(f"run time = {time.time() - start_time:.2f} seconds ---")


def validate_lineage_file(lineage_filename: str):
    """
    read .csv file with 2 columns from_regex,replace_expr
    store values in a dict to return to caller for processing
    """
    print(f"reading lineage file from: {lineage_filename}")
    validation_filename = os.path.splitext(lineage_filename)[0] + "_validation.csv"

    logging.info(f"initializing csv validation file: {validation_filename}")
    # create the output validation file and write the header
    fValidator = open(validation_filename, "w", newline="")
    validatorWriter = csv.writer(fValidator)
    validatorWriter.writerow(
        [
            "Association",
            "From Connection",
            "To Connection",
            "From Object",
            "To Object",
            from_valid_header,
            to_valid_header,
            link_exists_header,
            comments_header,
        ]
    )

    error_count = 0
    rowCount = 1
    rows_with_errors = []
    try:
        with open(lineage_filename, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rowCount += 1
                logging.info(f"row {rowCount} from csv to validate is: {row}")
                validated_row = validate_lineage_row(row)

                if validated_row[from_valid_header] == "False":
                    error_count += 1
                if validated_row[to_valid_header] == "False":
                    error_count += 1

                if (
                    validated_row[from_valid_header] == "False"
                    or validated_row[to_valid_header] == "False"
                ):
                    rows_with_errors.append(rowCount)
                    logging.error(
                        f"row {rowCount} contains errors - "
                        f"LeftValid={validated_row[from_valid_header]} "
                        f"RightValid={validated_row[to_valid_header]} "
                        f"message={validated_row[comments_header]}"
                    )

                validatorWriter.writerow(
                    [
                        row["Association"],
                        row["From Connection"],
                        row["To Connection"],
                        row["From Object"],
                        row["To Object"],
                        validated_row[from_valid_header],
                        validated_row[to_valid_header],
                        validated_row[link_exists_header],
                        validated_row[comments_header],
                    ]
                )

    except FileNotFoundError:
        print(f"error reading file: {lineage_filename}, process aborted.")
        logging.error(f"error reading file: {lineage_filename}, process aborted.")
        exit()

    print(
        f"\trows processed: {rowCount-1} - {error_count} "
        f"errors found in {len(rows_with_errors)} rows"
    )
    logging.info(
        f"\trows processed: {rowCount-1} - {error_count} "
        f"errors found in {len(rows_with_errors)} rows"
    )
    if error_count > 0:
        print(f"rows with errors: {rows_with_errors}")
        logging.info(f"rows with errors: {rows_with_errors}")
    fValidator.close()
    return


def validate_lineage_row(row_to_validate: dict) -> dict:
    # validate a lineage row
    # pass back the same dict, with new entries for From Valid, To Valid, Comments

    # initialize the return (assume false for valid, until we know otherwise)
    row_to_validate[from_valid_header] = False
    row_to_validate[to_valid_header] = False
    row_to_validate[comments_header] = ""
    row_to_validate[link_exists_header] = False

    leftobj = {}
    rightobj = {}
    is_conn_assign_used = False

    comments = []

    # check for connection assignments (which are not validated)
    if row_to_validate["From Connection"] == "":
        left_valid, ret_message, leftobj = validate_edc_id(
            row_to_validate["From Object"], row_to_validate["Association"]
        )
        if len(ret_message) > 0:
            comments.append("fromId:" + ret_message)
    else:
        logging.info("left object uses connection assignment, id not checked")
        is_conn_assign_used = True
        left_valid = "Unknown"
        comments.append("left object uses connection assignment.")
    if row_to_validate["To Connection"] == "":
        right_valid, ret_message, rightobj = validate_edc_id(
            row_to_validate["To Object"], row_to_validate["Association"]
        )
        # if right_valid:
        if len(ret_message) > 0:
            comments.append("toId:" + ret_message)
    else:
        right_valid = "Unknown"
        is_conn_assign_used = True
        comments.append("right object uses connection assignment.")

    # update the row to return to the caller
    # return string value not boolean
    row_to_validate["From Valid"] = left_valid
    row_to_validate["To Valid"] = right_valid
    row_to_validate[comments_header] = comments

    if is_conn_assign_used:
        row_to_validate[link_exists_header] = "Unknown"

    # if both sides are valid - check to see if the link actually exists
    if left_valid == "True" and right_valid == "True" and not is_conn_assign_used:
        # print("checking for valid link in EDC...")
        validated_link = False
        for linkedobj in leftobj["items"][0]["dstLinks"]:
            # print(f"\tchecking dstlink {linkedobj}")
            # use casefile fir a cis check, incase the id was not a case sensitive match
            if linkedobj["id"].casefold() == row_to_validate["To Object"].casefold():
                # print("\t\tobject link does exist")
                row_to_validate[link_exists_header] = True
                validated_link = True
                break

        if not validated_link:
            comments.append(
                "object link does not exist in EDC"
                " - lineage should be imported again"
            )

    # end of validate lineage row
    return row_to_validate


def validate_edc_id(id: str, link_type: str):
    # validate an id - if it exists return true
    # removed returns -> tuple[str, str, dict] for python3.6
    resource_name = id.split("://")[0]
    is_casesensitive = is_resource_casesenitive(resource_name)
    is_valid = "False"
    message = ""
    if len(id) != len(id.strip()):
        message = message + "id has leading/trailing whitespace. "
    # check for exact id match (regardless of case sensitivity)
    apiURL = edcHelper.baseUrl + "/access/2/catalog/data/objects"
    header = {"Accept": "application/json"}
    tResp = edcHelper.session.get(
        apiURL,
        params={
            "offset": 0,
            "pageSize": 1,
            "id": f"{id}",
            "associations": link_type,
            "includeDstLinks": "true",
            "includeSrcLinks": "true",
            "includeRefObjects": "true",
        },
        headers=header,
    )
    object_count = -1
    if tResp.status_code == 200:
        result = json.loads(tResp.text)
        object_count = result["metadata"]["totalCount"]
        if object_count == 1:
            is_valid = "True"

    # if the object count is 0 and the resource is not case sensitive
    if object_count == 0 and not is_casesensitive:
        print(f"need to check for case insensitive match for {id}")
        last_2 = ("/" + "/".join(id.split("/")[-2:])).upper()
        print(f"searching for core.autoSuggestMatchId={last_2}")
        resp2 = edcHelper.session.get(
            f"{edcHelper.baseUrl}/access/2/catalog/data/objects",
            headers={"Accept": "application/json"},
            params={
                "offset": 0,
                "pageSize": 20,
                "q": f"core.autoSuggestMatchId:{last_2}",
                "fq": f"core.resourceName:{resource_name}",
                "includeDstLinks": "false",
                "includeSrcLinks": "false",
            },
        )
        if resp2.status_code == 200:
            result = json.loads(resp2.text)
            cs_count = result["metadata"]["totalCount"]
            # print(f"cs count={cs_count}")
            if cs_count == 1:
                cs_reversals.append(id)
                actual_id = result["items"][0]["id"]
                print(actual_id)
                message = (
                    message + f"case sensitive id did not match,"
                    f" cis approach matched - actual id={actual_id}"
                )
                return ("True", message, result)
            elif cs_count > 1:
                # multiple found
                message = (
                    message + f"multiple objects found ({cs_count}) "
                    "need to refine the search used for CIS match"
                )

    # if the id is a reference object - add a return message
    if re.match(ref_id_regex, id):
        message = message + "reference object id used. "

    return (is_valid, message, result)


def is_resource_casesenitive(resourceName: str) -> bool:
    """
    check if a resource is defined as case-sensitive
    this will change the way a match is found,
    if an id does not match if the case is not a match
    @todo:  move this to common edc functions
    reference resources - default to True
    """

    # regex pattern for a reference id
    ref_id_regex = r"([\w\-_]+)\$\$([\w\-_]+)\$\$([\w\-_\/]+)"
    if re.match(ref_id_regex, resourceName):
        # print("\treference id found")
        # add to resource map
        resource_map[resourceName] = True
        return True

    default_response = True
    # check the cache first
    if resourceName in resource_map:
        return resource_map[resourceName]

    # resource not yet in cache, so get the resource def
    resp = edcHelper.session.get(
        f"{edcHelper.baseUrl}/access/1/catalog/resources/{resourceName}",
        headers={"Accept": "application/json"},
    )
    # print(resp.status_code)
    if resp.status_code != 200:
        print(
            f"error getting resource case-sensitivity: {resp.status_code} {resp.text}"
        )
        return default_response

    # read the result, as json string
    result = json.loads(resp.text)
    # check "scannerConfigurations"/"configOptions"
    for config_opt in result["scannerConfigurations"][0]["configOptions"]:
        if config_opt["optionId"] == "Case Sensitive":
            resource_map[resourceName] = config_opt["optionValues"][0]
            return config_opt["optionValues"][0]

    return default_response


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
