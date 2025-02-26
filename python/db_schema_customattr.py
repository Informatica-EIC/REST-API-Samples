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
        self.count_objects_to_process()
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
                print(f"\tcuston attr id value={cust_attr_id} from variable:'schema_custom_attr_id'")
                if cust_attr_id == None:
                    print("Error:  no value found for schema custom attribute in 'schema_custom_attr_id', exiting")
                    return False
                self.schema_cust_attr_id = cust_attr_id
                self.validate_custom_attr(cust_attr_id)

                class_types = os.getenv("class_types")
                print(f"\tclass types to update={class_types}")
                if class_types != None:
                    # check if different from default
                    if class_types != self.class_types:
                        print(f"\tupdateing class types to process from parm file to : {class_types}")
                        self.class_types = class_types

                return True

    def validate_custom_attr(self, attr_id: str):
        # check that the custom attribute id exists (and what classtypes it is valid for?)
        print(f"validating custom attribute exists: id={attr_id}")
        edcHelper.initUrlAndSessionFromEDCSettings()
        edcHelper.validateConnection()
        print(f"EDC version: {edcHelper.edcversion_str} ## {edcHelper.edcversion}")

        resturl = edcHelper.baseUrl + f"/access/2/catalog/models/attributes/{attr_id}"

        # execute catalog rest call, for a page of results
        try:
            resp = edcHelper.session.get(resturl, timeout=3)
        except requests.exceptions.RequestException as e:
            print("Error connecting to : " + resturl)
            print(e)
            # exit if we can't connect
            sys.exit(1)

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
        print(resultJson)
        # get the attribute name
        self.schema_attr_name = resultJson['name']
        print(f"attribute name={self.schema_attr_name}")
        # inspect the classes
        schema_classes = resultJson['classes']
        print(f"classes referenced by attr: {len(schema_classes)}")




    def count_objects_to_process(self):
        print("1 - counting objects to process")
        
        


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




    



if __name__ == "__main__":
    # entry point
    main()