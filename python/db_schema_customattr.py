"""
Created February, 2025

pupulate custom attribute values for relational database objects (tables/views/columns) 
storing the schema name that owns these elements.
this should make searching easier for objects that are owned-by a schema

usage:  xdoc_lineage_gen.py  <parms>

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

    print("process ended")


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
        self.resoource_files = {}  # file fqdn: filename

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

    def store_args(self) -> bool:
        # extract args as individial vars (& print for context)
        if self.args.outDir != self.out_folder:
            print("storing new out folder here")
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

                return True

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
            sys.exit(1)

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
            "fq": f"NOT {self.schema_cust_attr_id}:[* TO *]",
            "defaultFacets": "false",
            "facet": "true",
            "facetId": ["core.resourceType", "core.resourceName", "core.classType"],
            "offset": 0,
            "pageSize": 1,
        }

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
            default="./out",
            help=(
                "output folder to write results - default = ./out "
                " - will create folder if it does not exist"
            ),
        )

        args_parser.add_argument(
            "-pf",
            "--parms_file",
            required=False,
            help="varibles file to use for instance of the process",
        )

        # args_parser.add_argument(
        #     "-i",
        #     "--edcimport",
        #     default=False,
        #     # type=bool,
        #     action="store_true",
        #     help=(
        #         "execute bulk import processes, if false it is really a test mode "
        #     ),
        # )

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
        print(f"processing resource {resource_name}")
        # get/create schema id to name mapping
        self.extract_schema_names(resource_name)

        # extract schema nam
        print(self.schema_map)

        self.process_resource_objects(resource_name)
        # search for all objects in resource and match schema

    def extract_schema_names(self, resource_name: str):
        # use
        print(f"\textracting schema names for resource: {resource_name}")

        # use id:<resource_name>:* to ensure only that resource objects are returned
        # using core.resourceName:<name> will also get resources with extra sufixes
        params = {
            "q": f"id:{resource_name}\:*",
            "fq": "core.classType:com.infa.ldm.relational.Schema",
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
        # reset schema name mapping
        self.schema_map.clear()

        for schema_hit in resultJson["hits"]:
            schema_id = schema_hit["id"]
            schema_name = schema_hit["values"][0]["value"]
            print(f"\t\tschema={schema_name} id={schema_id}")
            self.schema_map[schema_id] = schema_name

        return  # end of extract schema names for resource

    def process_resource_objects(self, resource_name):
        # run the actual search & iterate over the results
        page_size = 500
        offset = 0
        expected_count = self.resource_map[resource_name]
        print(
            f"searching for objects to update in resource: {resource_name}, should find {expected_count}"
        )

        #   id:WideWorldImporters_SQLServer\:*
        parms = {
            "q": f"id:{resource_name}\:*",
            "fq": [
                f"NOT {self.schema_cust_attr_id}:[* TO *]",
                f"core.classType:({self.class_types})",
            ],
            "defaultFacets": "false",
            "fl": ["core.name", "core.classType"],
            "facet": "true",
            "facetId": ["core.resourceType", "core.resourceName", "core.classType"],
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
                schema_name = self.schema_map[schema_id]
                # get the classype
                class_type = obj_found["values"][0]["value"]
                abbrev_type = class_type.rsplit(".", 1)[-1]
                obj_name = obj_found["values"][1]["value"]
                # print(f"\t\tobject to update: {obj_id} \n\t\t\twith value={schema_name} type={abbrev_type} name={obj_name}")

                # write entry to bulk import file
                csv_row = [obj_id, obj_name, abbrev_type, schema_name]
                self.bulk_writer.writerow(csv_row)

            # call bulk import for file (after closing)
            try:
                self.fcsvbulk.close()
                print(f"bulk file closed...{self.bulk_file_name}")
                # self.start_edc_bulk_import(self.bulk_file_name, self.bulk_file_fqdn)

            except:
                print("error closing csv bulk file")

            # for next iteration
            offset += page_size

        # end of process for a resource...  now call bulk import for all files
        # need to do it this way, as if we call bulk import for each file, it will finish
        # very fast and skew the next results
        print(f"resource: {resource_name} complete - imppring files")
        for file_full_path, file_name in self.resoource_files.items():
            print(f"\tbulk importing: {file_full_path}")
            self.start_edc_bulk_import(file_name, file_full_path)

        # clear out resource files - since they are all now processed
        self.resoource_files.clear()

        return  # process resource objects

    def create_bulk_file(self, resource_name: str) -> str:
        # create a new file - <resourceName>_<timestamp>.csv
        # with header for bulk import
        # returning the file name
        now = datetime.now()
        timestamp_ms = now.strftime("%Y%m%d_%H%M%S%f")

        self.bulk_file_name = f"{resource_name}_{timestamp_ms}.csv"
        self.bulk_file_fqdn = f"./bulk/{self.bulk_file_name}"

        # add to map - for bulk import after the resource finished
        self.resoource_files[self.bulk_file_fqdn] = self.bulk_file_name

        print(f"File to create={self.bulk_file_fqdn}")

        self.fcsvbulk = open(self.bulk_file_fqdn, "w", newline="", encoding="utf8")
        self.bulk_writer = csv.writer(self.fcsvbulk)
        header1 = ["id", "core.name", "core.classType", self.schema_cust_attr_id]
        header2 = ["id", "name", "classType", self.schema_attr_name]

        self.bulk_writer.writerow(header1)
        self.bulk_writer.writerow(header2)

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
        # print(f"\t{file}")
        uploadResp = edcHelper.session.post(
            apiURL,
            data=params,
            files=file,
        )
        print("\tresponse=" + str(uploadResp.status_code))
        if uploadResp.status_code == 200:
            # valid - return the json
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


if __name__ == "__main__":
    # entry point
    main()
