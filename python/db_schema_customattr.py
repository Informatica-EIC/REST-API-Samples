"""
Created February, 2025

pupulate custom attribute values for relational database objects (tables/views/columns)
storing the schema name that owns these elements.
this should make searching easier for objects that are owned-by a schema

usage: db_schema_customattr.py [-h] [-c EDCURL] [-v ENVFILE] [-a AUTH | -u USER] [-s SSLCERT] [-o OUTDIR] [-pf PARMS_FILE] [-i] [--pagesize PAGESIZE]
                               [--queuesize QUEUESIZE] [--setup]

options:
  -h, --help            show this help message and exit
  -c, --edcurl EDCURL   edc url - including http(s)://<server>:<port>, if not already configured via INFA_EDC_URL environment var
  -v, --envfile ENVFILE
                        .env file with config settings INFA_EDC_URL,INFA_EDC_AUTH etc will over-ride system environment variables. if not specified - '.env'
                        file in current folder will be used
  -a, --auth AUTH       basic authorization encoded string (preferred over -u) if not already configured via INFA_EDC_AUTH environment var
  -u, --user USER       user name - will also prompt for password
  -s, --sslcert SSLCERT
                        ssl certificate (pem format), if not already configured via INFA_EDC_SSL_PEM environment var
  -o, --outDir OUTDIR   output folder to write results - default = ./out - will create folder if it does not exist
  -pf, --parms_file PARMS_FILE
                        varibles file to used to control the process, like custom attribute id & classtypes to check
  -i, --edcimport       execute bulk import processes, if false it is really a test mode
  --pagesize PAGESIZE   pageSize to use for search when returning object contents, default 1000
  --queuesize QUEUESIZE
                        max number of jobs for the bulk import queue, default 10. process will pause until queue size is less than max
  --setup               setup the connection to EDC by creating a .env file - same as running setupConnection.py and is useful if packaged as a stand-alone
                        executable


@author: dwrigley
"""

import json
import csv
import requests
import argparse
import edcSessionHelper
import setupConnection
import logging
import edcutils
import urllib3
import time
import os
import sys
import pathlib
from dotenv import load_dotenv
from datetime import datetime

urllib3.disable_warnings()


if not os.path.exists("./log"):
    print("creating log folder ./log")
    os.makedirs("./log")

logging.basicConfig(
    format="%(asctime)s:%(levelname)-8s:%(module)s:%(message)s",
    level=logging.DEBUG,
    filename="./log/db_schema_customattr.log",
    filemode="w",
)

# create the EDC session helper class
edcHelper = edcSessionHelper.EDCSession()


def main():
    """
    main function - entry point.  setup instance of this process and execute
    """

    start_time = time.time()

    outFolder = "./out"

    custom_process = db_schema_customattr()
    custom_process.start()

    elapsed_seconds = time.time() - start_time
    # print(f"run time = {elapsed_seconds:.2f} seconds ---")
    print(f"run time: {time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))}")


class db_schema_customattr:

    # variables for class instances
    out_folder = "./out"
    # args = []
    # args_parser = argparse

    def __init__(self):
        # add initialization code here
        print("initializing process...")
        self.args_parser = self.setup_cmd_parser()
        self.class_types = "com.infa.ldm.relational.Table com.infa.ldm.relational.View com.infa.ldm.relational.Column com.infa.ldm.relational.ViewColumn"
        # empty list of resources to process (since we process 1 at a time)
        self.resources_to_process = []
        self.schema_map = {}  # schema id: schema name
        self.resource_map = {}  # resource name: count of found objects
        self.resource_files = {}  # file full_path: filename
        self.pagesize = 1000  # default pagesize, override in args
        self.bulk_queuesize = 10  # default queue size to limit bulk imports
        self.sleep_seconds = 2  # seconds to sleep, to check queue again
        self.out_folder = "./bulk"  # default folder for csv files created
        self.schema_lookup_errors = 0  # counter for any errors finding sch name
        self.schema_error_list = []  # list of resources with schema lookup errors
        self.schema_alt_found = 0  # counter for external schema matches
        self.files_created = []  # list of the actual files created
        self.files_imported = []  # list of the actual files imported
        self.rows_exported = 0  # counter for total rows written to all csv files
        self.query_filters = []  # list of additional query filters from parm file

    def start(self):
        print("process starts here...")
        # check for command-line args
        self.args, unknown = self.args_parser.parse_known_args()
        # if setup is requested - then call the setup process and finish
        if self.args.setup:
            # if setup is requested (running standalone)
            # call setupConnection to create a .env file to use next time we run
            print("setup requested..., calling setupConnection & exiting")
            setupConnection.main()
            return

        # start the process
        # 1 - count objects to be processed (gets resource list and total objects)
        rc = self.store_args()
        if not rc:
            return
        if not self.validate_edc_connection():
            print("edc connection not valid, exiting")
            return
        if not self.validate_custom_attr(self.schema_cust_attr_id):
            print("schema custom attribtue not valid, exiting")
            return

        # initial search to find objects that need to be updated
        # and get the resources that contain these objects
        self.count_objects_to_process()

        # assuming there are objects and resources to process, then proceed
        if len(self.resources_to_process) == 0:
            print("no objects or resources to process, exiting")
            return

        for resource_name in self.resources_to_process:
            # start the process for the resource
            self.process_resource(resource_name)
        # 2 - get schema names and id's (for lookup)
        # 3 - find/export objects that need custom attribute values

        self.print_summary()

    def print_summary(self):
        print("process ended")
        print(f"\t    resources processed: {len(self.resources_to_process)}")
        print(f"\t          files created: {len(self.files_created)}")
        print(f"\t         total csv rows: {self.rows_exported}")
        print(f"\t bulk imports submitted: {len(self.files_imported)}")

        print(f"\t   schema lookup errors: {self.schema_lookup_errors}")
        if len(self.schema_error_list) > 0:
            print(f"\t #resources with errors: {len(self.schema_error_list)}")
            print(f"\t  resources with errors: {self.schema_error_list}")
        print(f"\texternal schema matches: {self.schema_alt_found}")
        print("")

    def store_args(self) -> bool:
        # extract args as individial vars (& print for context)
        if self.args.outDir != self.out_folder:
            self.out_folder = self.args.outDir
            print(f"\toutput folder set to {self.out_folder}")

        # create output folder if it does not already exist
        if self.out_folder != "":
            if not os.path.exists(self.out_folder):
                print(f"\tcreating folder: {self.out_folder}")
                os.makedirs(self.out_folder)

        if self.args.edcimport:
            # execute bulk import switch (default off)
            print("bulk import mode configured --edcimport")
        else:
            print("test mode, no bulk import will happen")

        if self.args.pagesize:
            # store the pagesize
            if self.args.pagesize != self.pagesize:
                self.pagesize = self.args.pagesize
                print(f"setting pagesize to {self.pagesize}")

        # overriding queue size
        if self.args.queuesize <= 0 or self.args.queuesize > 100:
            # invalid
            print(f"quesize arg has invalid value: {self.args.queuesize} disregrding")
        # elif self.args.queuesize > 100:
        #     print(f"quesize arg has invalid value: {self.args.queuesize} disregrding")
        else:
            if self.args.queuesize != self.bulk_queuesize:
                print(f"storing queuesize={self.args.queuesize}")
                self.bulk_queuesize = self.args.queuesize

        if self.args.parms_file:
            print(f"storing vars file={self.args.parms_file}")
            # read the parms file - using dotenv

            # check if the file exists
            # envfullpath = (pathlib.Path(".").cwd() / args.envfile)
            print(f"ready to check .env file {self.args.parms_file}")
            if pathlib.Path(self.args.parms_file).is_file():
                print(f"\tloading from .env file {self.args.parms_file}")
                # envfullpath = f"{Path('.').cwd()}\\{args.envfile}"
                # override - ensure we read settings from <envfile> vs vars
                load_dotenv(
                    dotenv_path=(pathlib.Path(self.args.parms_file)),
                    verbose=True,
                    override=True,
                )
                # check the settings from the .env file
                # print(os.getenv("INFA_EDC_URL"))
                cust_attr_id = os.getenv("schema_custom_attr_id")
                print(
                    f"\tcuston attr id value={cust_attr_id} from variable:'schema_custom_attr_id'"
                )
                if cust_attr_id == None:
                    print(
                        "Error:  no value found for schema custom attribute in 'schema_custom_attr_id', exiting"
                    )
                    return False
                self.schema_cust_attr_id = cust_attr_id

                class_types = os.getenv("class_types")
                print(f"\tclass types to update={class_types}")
                if class_types != None:
                    # check if different from default
                    if class_types != self.class_types:
                        print(
                            f"\tupdateing class types to process from parm file to : {class_types}"
                        )
                        self.class_types = class_types

                # check for query_filter - up to 5??
                for qf_suffix in range(1, 6, 1):
                    filter_name = "query_filter" + str(qf_suffix)
                    # print(f"\tchecking query filter: {filter_name}")
                    fq = os.getenv(filter_name)
                    if fq != None:
                        if len(fq) > 0:
                            # self.query_filter = fq
                            self.query_filters.append(fq)
                            print(
                                f"\tstoring additional query filter {filter_name} = {fq}"
                            )
                        else:
                            print(f"\tquery filter has no value, disregarding")

                print(
                    f"\t{len(self.query_filters)} query filters stored from parameter file"
                )

                return True
            else:
                print(f"parameter file not found: {self.args.parms_file}, exiting")
                return False

    def validate_edc_connection(self) -> bool:
        edcHelper.initUrlAndSessionFromEDCSettings()
        rc, version_info = edcHelper.validateConnection()
        if rc != 200:
            # can'r validate edc connecting (calling productInformation)
            return False

        print(
            f"EDC connection validated, version: {edcHelper.edcversion_str} ## {edcHelper.edcversion}"
        )
        return True

    def validate_custom_attr(self, attr_id: str) -> bool:
        # check that the custom attribute id exists (and what classtypes it is valid for?)
        print(f"validating custom attribute exists: id={attr_id}")

        resturl = edcHelper.baseUrl + f"/access/2/catalog/models/attributes/{attr_id}"

        # execute catalog rest call, for a page of results
        try:
            resp = edcHelper.session.get(resturl, timeout=3)
        except requests.exceptions.RequestException as e:
            print("Error connecting to : " + resturl)
            print(e)
            # exit if we can't connect
            return False

            # no execption rasied - so we can check the status/return-code
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            # since we are in a loop to get pages of objects - break will exit
            # break
            # instead of break - exit this script
            return False

        resultJson = resp.json()
        # store the total, so we know when the last page of results is read
        # total = resultJson["metadata"]["totalCount"]
        # print(resultJson)
        # get the attribute name
        print(f"\tattribute is valid id={self.schema_cust_attr_id}")
        self.schema_attr_name = resultJson["name"]
        print(f"\tattribute name={self.schema_attr_name}")
        # inspect the classes
        schema_classes = resultJson["classes"]
        print(f"\tclasses referenced by attr: {len(schema_classes)}")

        return True

    def count_objects_to_process(self):
        print("1 - counting objects to process")
        parms = {
            "q": f"core.classType:({self.class_types})",
            "fq": [f"NOT {self.schema_cust_attr_id}:[* TO *]"],
            "defaultFacets": "false",
            "facet": "true",
            "facetId": ["core.resourceType", "core.resourceName", "core.classType"],
            "offset": 0,
            "pageSize": 1,
        }

        # if additional query filter is specified add it
        if len(self.query_filters) > 0:
            current_fq = parms.get("fq")
            current_fq.extend(self.query_filters)
            print(f"\t\tsearch fq is now: {current_fq}")

        status, resultJson = self.search_edc(parms)
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resultJson))
            return

        total = resultJson["metadata"]["totalCount"]
        print(f"\tsearch successful: {total:,} objects found to update")

        # read the resoureceName facet - gets a unique list of resources and the count of objects
        all_facets = resultJson["facetResults"]
        for facet_result in all_facets:
            if facet_result["facetId"] == "core.resourceName":
                # resource facet here
                buckets = facet_result["buckets"]
                print(f"\tresource buckets = {len(buckets)}")
                for resource_facet in buckets:
                    res_name = resource_facet["value"]
                    res_count = resource_facet["count"]
                    print(f"\t\tresource:{res_name} count={res_count:,}")
                    self.resource_map[res_name] = res_count
                    self.resources_to_process.append(res_name)
        return

    def setup_cmd_parser(self):
        # define script command-line parameters
        print("setting up cmd args...")
        args_parser = argparse.ArgumentParser(parents=[edcHelper.argparser])

        # check for args overriding the env vars
        # parser = argparse.ArgumentParser()
        # add args specific to this utility (resourceName, resourceType, outDir, force)

        args_parser.add_argument(
            "-o",
            "--outDir",
            required=False,
            default="./bulk",
            help=(
                "folder for writing csv bulk import files. default = ./bulk "
                " - will create folder if it does not exist"
            ),
        )

        args_parser.add_argument(
            "-pf",
            "--parms_file",
            default="./db_schema_vars.txt",
            required=False,
            help="varibles file to used to control the process, like custom attribute id & classtypes to check",
        )

        args_parser.add_argument(
            "-i",
            "--edcimport",
            required=False,
            # type=bool,
            action="store_true",
            help=("execute bulk import processes, if false it is really a test mode "),
        )

        args_parser.add_argument(
            "--pagesize",
            required=False,
            type=int,
            help="pageSize to use for search when returning object contents, default 1000",
        )

        args_parser.add_argument(
            "--queuesize",
            required=False,
            default=10,
            type=int,
            help="max number of jobs for the bulk import queue, default 10.  process will pause until queue size is less than max",
        )

        args_parser.add_argument(
            "--setup",
            required=False,
            action="store_true",
            help=(
                "setup the connection to EDC by creating a .env file"
                " - same as running setupConnection.py and is useful if packaged as a stand-alone executable"
            ),
        )

        return args_parser

    def process_resource(self, resource_name: str):
        # find objects in the resource that are missing the custom attribute contents
        # first extract the schema names, so we know what value to add to the attribute
        # process_resource_objects will execute the search and call bulk import

        print(f"processing resource {resource_name}")
        # get/create schema id to name mapping
        self.extract_schema_names(resource_name)

        self.process_resource_objects(resource_name)

    def extract_schema_names(self, resource_name: str):
        print(f"\textracting schema names for resource: {resource_name}")
        # reset schema name mapping
        self.schema_map.clear()

        # use id:<resource_name>:* to ensure only that resource objects are returned
        # using core.resourceName:<name> will also get resources with extra suffixes
        params = {
            "q": f"id:{resource_name}" + r"\:*",
            "fq": "core.classType:(com.infa.ldm.relational.Schema com.infa.ldm.google.bigquery.Dataset com.infa.ldm.relational.ExternalSchema)",
            "fl": "core.name",
            "pageSize": 1000,
            "offset": 0,
        }
        status, resultJson = self.search_edc(params)

        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resultJson))
            return

        total = resultJson["metadata"]["totalCount"]
        print(f"\tschema search successful: {total:,} found")

        # store schema id:name in dict, for lookup when processing objects
        for schema_hit in resultJson["hits"]:
            schema_id = schema_hit["id"]
            schema_name = schema_hit["values"][0]["value"]
            print(f"\t\tschema={schema_name} id={schema_id}")
            self.schema_map[schema_id] = schema_name

        return  # end of extract schema names for resource

    def process_resource_objects(self, resource_name):
        # run the actual search & iterate over the results
        # get pagesize from args...
        page_size = self.pagesize
        offset = 0
        expected_count = self.resource_map[resource_name]
        print(
            f"searching for objects to update in resource: {resource_name}, should find {expected_count}"
        )

        #   id:WideWorldImporters_SQLServer\:*
        parms = {
            "q": f"id:{resource_name}" + r"\:*",
            "fq": [
                f"NOT {self.schema_cust_attr_id}:[* TO *]",
                f"core.classType:({self.class_types})",
            ],
            "defaultFacets": "false",
            "fl": ["core.name", "core.classType"],
            # "facet": "true",
            # "facetId": ["core.resourceType", "core.resourceName", "core.classType"],
            "offset": offset,
            "pageSize": page_size,
        }

        while offset < expected_count:
            # override offset
            parms["offset"] = offset
            print(f"\tready to query page with offset: {offset}")

            # execute serach - iterate results and write to file then send bulk import
            # execute catalog rest call, for a page of results
            status, resultJson = self.search_edc(parms)
            if status != 200:
                # some error - e.g. catalog not running, or bad credentials
                print("error! " + str(status) + str(resultJson))
                # if there is an error with the search, break the iteration
                break
                # return

            # total = resultJson["metadata"]["totalCount"]
            print(
                f"\tsearch successful: {len(resultJson['hits']):,} objects found to update"
            )

            # create bulk import file
            self.create_bulk_file(resource_name)

            for obj_found in resultJson["hits"]:
                obj_id = obj_found["id"]
                schema_id = "/".join(obj_id.split("/", 4)[:4])
                if schema_id not in self.schema_map:
                    # problem - schema not found, skip the object

                    # this could be because of external schema objects.  so check
                    # all schema id's for a match.
                    is_found = False
                    for sch_id_to_check in self.schema_map.keys():
                        if obj_id.startswith(sch_id_to_check + "/"):
                            # match
                            # set schema_id.
                            schema_id = sch_id_to_check
                            is_found = True
                            self.schema_alt_found += 1
                            # print(
                            #     f"\tschema found using alternate, probably via external schema object {schema_id} "
                            # )
                            break  # match found, so we can continue
                    if not is_found:
                        # error, schema not found - so skip this item
                        print(
                            f"\tschema id not found: {schema_id} skipping - obj={obj_id}"
                        )
                        self.schema_lookup_errors += 1
                        if resource_name not in self.schema_error_list:
                            self.schema_error_list.append(resource_name)
                        continue
                schema_name = self.schema_map[schema_id]
                # get the classype
                class_type = obj_found["values"][0]["value"]
                abbrev_type = class_type.rsplit(".", 1)[-1]
                obj_name = obj_found["values"][1]["value"]
                # print(f"\t\tobject to update: {obj_id} \n\t\t\twith value={schema_name} type={abbrev_type} name={obj_name}")

                # write entry to bulk import file
                csv_row = [obj_id, obj_name, abbrev_type, schema_name]
                self.bulk_writer.writerow(csv_row)
                self.rows_exported += 1

            # call bulk import for file (after closing)
            try:
                self.fcsvbulk.close()
                # print(f"bulk file closed...{self.bulk_file_name}")
                # self.start_edc_bulk_import(self.bulk_file_name, self.bulk_file_fqdn)

            except:
                print("error closing csv bulk file")

            # for next iteration
            offset += page_size

        # end of process for a resource...  now call bulk import for all files
        # need to do it this way, as if we call bulk import for each file, it will finish
        # very fast and skew the next results
        print(
            f"resource: {resource_name} complete - importing {len(self.resource_files)} files"
        )
        if self.args.edcimport:
            for file_full_path, file_name in self.resource_files.items():
                print(f"\tbulk importing: {file_full_path}")
                self.start_edc_bulk_import(file_name, file_full_path)
        else:
            print("\tuse --edcimport cmdline switch to enable bulk import")

        # clear out resource files - since they are all now processed
        self.resource_files.clear()

        return  # process resource objects

    def create_bulk_file(self, resource_name: str) -> str:
        # create a new file - <resourceName>_<timestamp>.csv
        # with header for bulk import
        # returning the file name
        now = datetime.now()
        timestamp_ms = now.strftime("%Y%m%d_%H%M%S%f")

        self.bulk_file_name = f"{resource_name}_{timestamp_ms}.csv"
        self.bulk_file_fqdn = f"{self.out_folder}/{self.bulk_file_name}"

        # add to map - for bulk import after the resource finished
        self.resource_files[self.bulk_file_fqdn] = self.bulk_file_name

        print(f"File to create={self.bulk_file_fqdn}")

        self.fcsvbulk = open(self.bulk_file_fqdn, "w", newline="", encoding="utf8")
        self.bulk_writer = csv.writer(self.fcsvbulk)
        header1 = ["id", "core.name", "core.classType", self.schema_cust_attr_id]
        header2 = ["id", "name", "classType", self.schema_attr_name]

        self.bulk_writer.writerow(header1)
        self.bulk_writer.writerow(header2)
        self.files_created.append(self.bulk_file_fqdn)

        return  # init bulk file

    def start_edc_bulk_import(self, fileName: str, fullPath: str):
        """
        start the bulk import process bu uploading the csv file
        we need to make sure we add the com.infa.appmodels.ldm package, or it will fail with a weird message (csv tampered with)
        """
        apiURL = edcHelper.baseUrl + "/access/2/catalog/jobs/objectImports"

        # print("\turl=" + apiURL)
        params = {"packages": "com.infa.appmodels.ldm,com.infa.ldm.ootb.enrichments"}

        # print("\t" + str(params))
        file = {"file": (fileName, open(fullPath, "rt"), "text/csv")}

        # before submitting - we want to make sure the bulk import queue is not overloaded
        queued_job_count = self.get_bulk_queue_count()
        print(f"\tjobs currently queued for reset={queued_job_count}")
        while queued_job_count >= self.bulk_queuesize:
            print(
                f"\tcurrent queue ({queued_job_count}) >= threshold ({self.bulk_queuesize}), waiting..."
            )
            time.sleep(1)
            queued_job_count = self.get_bulk_queue_count()

        # print(f"\t{file}")
        uploadResp = edcHelper.session.post(
            apiURL,
            data=params,
            files=file,
        )
        print("\tresponse=" + str(uploadResp.status_code))
        if uploadResp.status_code == 200:
            # valid - return the json
            self.files_imported.append(fullPath)

            return uploadResp.status_code
        else:
            # not valid
            print("\tbulk import process (file upload) failed")
            print("\t" + str(uploadResp))
            print("\t" + str(uploadResp.text))
            return uploadResp.status_code

    def search_edc(self, search_parms: dict) -> tuple[int, dict]:
        # execute search using parms passed
        # return status code (e.g. 200) & json results
        resturl = edcHelper.baseUrl + f"/access/2/catalog/data/search"

        try:
            resp = edcHelper.session.get(resturl, params=search_parms, timeout=3)
        except requests.exceptions.RequestException as e:
            print("Error connecting to : " + resturl)
            print(e)
            # exit if we can't connect
            return False, None

        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, network error
            print("error! " + str(status) + str(resp.json()))

        resultJson = resp.json()
        return status, resultJson

    def get_bulk_queue_count(self):
        """
        query the catalog to see how many bulk imports are currently queue'd
        """
        apiURL = edcHelper.baseUrl + "/access/2/catalog/jobs/objectImports"

        # print("\turl=" + apiURL)
        params = {"jobStatus": "SUBMITTED"}
        print(f"parms>>{params}")

        # print(f"\t{file}")
        bulkjob_resp = edcHelper.session.get(
            apiURL,
            params=params,
        )
        print("\tresponse=" + str(bulkjob_resp.status_code))
        if bulkjob_resp.status_code == 200:
            # valid - return the json
            resultJson = bulkjob_resp.json()
            total = resultJson["metadata"]["totalCount"]
            print(f"\tqueued bulk import jobs={total}")
            return total
        else:
            # not valid
            print("\tcount of queued bulk imports failed")
            return 0


if __name__ == "__main__":
    # entry point
    main()
