"""
created by:  dwrigley

composite domain - any N of total script

for a given composite domain - allow the user to specify any N values
to use for a valid composite combination

eg  if there are 4 domains used - and you want to configure the domain rule
    for any 3 of the 4

    Name, Address, Email, Gender

    combinations of any 3 would be
        Name, Address, Email
        Name, Address, Gender
        Name, Email, Gender
        Address, Email, Gender

    8 out of 10 would produce 45 combinations (too hard/time consuming to do manually)

see internal infa notes:
    https://infawiki.informatica.com/display/NBDS/Composite+Domain+any+N+of+total
"""
import requests
import time
import sys
import urllib3
from itertools import combinations
import argparse
from edcSessionHelper import EDCSession
import json

# global var declaration (with type hinting)
# edcSession: EDCSession = None
urllib3.disable_warnings()

start_time = time.time()
# initialize http header - as a dict
auth = None

edcSession: EDCSession = EDCSession()
parser = argparse.ArgumentParser(parents=[edcSession.argparser])
parser.add_argument("-cd", "--composite_domain", help="composite domain to update")


def main():
    """
    main function
        - connect to edc
        - find domain (if specifiied, or promot user)
        - call function to get existing contents (get count)
        - prompt user for # values to use
        - format the new composite domain
        - write to edc

    """
    args, unknown = parser.parse_known_args()
    # initialize http session to EDC, storeing the baseurl
    edcSession.initUrlAndSessionFromEDCSettings()
    print(
        f"args from cmdline/env vars: url={edcSession.baseUrl}"
        f"  session={edcSession.session}"
    )

    # test the connection - see if the version is 10.4.0 or later
    rc, json_resp = edcSession.validateConnection()
    print(f"validated connection: {rc} {json_resp}")

    # check the composite-domain arg - if not entered, prompt the user
    if args.composite_domain is None:
        print("no composite domain entered via cmd-paramter, prompting...")
        args.composite_domain = input("Enter composite domain to modify: ")

    print(f"checking cd: {args.composite_domain}")
    rc, domain_json = get_cd(edcSession.session, args.composite_domain)

    # if domain_json is None:
    if rc != 200:
        print(f"unable to get composite domain content, exiting - reason={domain_json}")
        return

    print("parse the domain...")
    unique_elements = getDomainComponents(domain_json)

    print(f"\n{len(unique_elements)} found - how many objects per set?: ")
    any_n_val = input_valid_number(len(unique_elements))
    # any_n_val = int(input(f"enter a number between 1 and {len(unique_elements)-1}: "))
    print(any_n_val)

    domain_combinations = dict()
    domain_combinations = list(combinations(unique_elements, any_n_val))
    print(f"combinations found={len(domain_combinations)}")
    for a_combination in domain_combinations:
        print(a_combination)

    new_combinations = assemble_combinations(domain_combinations, unique_elements)
    domain_json["inferenceRules"] = new_combinations

    # ask the user if they want to update the existing domain
    # (Y = do the update, N=prompt for a new name)
    update_existing = input(
        f"\tupdate the exting domain (Y|y=yes, anything else = prompt for new name : "
    )
    if update_existing.lower() != "y":
        new_cd_name = input_name_nospace("enter new composite-domain name to create")
        if len(new_cd_name.strip()) == 0:
            print("no name entered, no composite domain created.")
            return

        # check if domain already exists - then create if not (better error than just generic 500)
        exists_rc, exists_json = get_cd(edcSession.session, new_cd_name)
        if exists_rc == 200:
            print(f"composite domain {new_cd_name} already exists, exiting")
            return

        # create the new composite domain
        domain_json["domainName"] = new_cd_name
        domain_json["domainId"] = ""
        domain_json["domainType"] = "CDD"
        print(f"ready to create new cd: {new_cd_name}")
        resturl = f"{edcSession.baseUrl}/access/1/catalog/compositedomains"
        header = {"Content-Type": "application/json"}
        rc = edcSession.session.post(
            resturl, data=json.dumps(domain_json), headers=header
        )

        if rc.status_code == 200:
            print(f"Success - status_code={rc.status_code} composite domain: {new_cd_name} was created")

        # print(rc)

    else:
        domain_json["domainType"] = "CDD"
        print(f"ready to update existing cd: {args.composite_domain}")
        resturl = (
            f"{edcSession.baseUrl}/access/1/catalog/"
            f"compositedomains/{args.composite_domain}"
        )
        header = {"Content-Type": "application/json"}
        rc = edcSession.session.put(
            resturl, data=json.dumps(domain_json), headers=header
        )
        if rc.status_code == 200:
            print(f"Success - status_code={rc.status_code} composite domain: {args.composite_domain} was updated")
        # print(rc)

    if rc.status_code != 200:
        print(f"operation failed: status_code={rc.status_code} reason={rc.reason}")

    # out_json = json.dumps(domain_json)
    # print(json.dumps(domain_json, indent=4))


def input_valid_number(max: int) -> int:
    """
    input a valid number from 1 through max
    anything else is rejected and asked for again
    """
    while True:
        try:
            nbr = int(input(f"Enter the number of counted domains : (1-{max}): "))
        except ValueError:
            print(f"Sorry, i need an integer between 1 and {max}")
            continue

        if nbr < 1:
            print("Sorry, your response must not less than 1.")
            continue
        if nbr > max:
            print(f"Sorry, your response must not be more than {max}")
            continue
        else:
            # valid number entered
            break
    return nbr


def input_name_nospace(prompt_message: str) -> str:
    """
    input a name - that cannot have spaces - empty string returned is ok
    """
    while True:
        entered_name = input(f"{prompt_message}: ")
        # print(f"value entered >>>{entered_name}<<<")
        if ' ' in entered_name or '\t' in entered_name:
            print("\tname cannot contain whitespace characters, please try again...")
            continue
        break
    return entered_name


def assemble_combinations(domain_combinations: list, domain_objects: dict):
    all_combinations = list()
    for domain_combination in domain_combinations:
        combi_list = list()
        combi_entry = dict()
        for a_domain in sorted(domain_combination):
            # domain_object = domain_objects[a_domain]
            combi_list.append(domain_objects[a_domain])
        combi_entry["compositeDomainRuleElements"] = combi_list
        combi_entry["ruleOperation"] = "AND"
        combi_entry["valid"] = "true"
        # separator = " AND "
        # test = separator.join(elems for elems in domain_combinations)
        combi_entry["inferenceCondition"] = " AND ".join(sorted(domain_combination))
        all_combinations.append(combi_entry)

    return all_combinations


def getDomainComponents(domain_def: str) -> dict:
    unique_objects = dict()
    obj_count = 0
    for inf_set in domain_def["inferenceRules"]:
        for obj_ref in inf_set["compositeDomainRuleElements"]:
            obj_count += 1
            obj_name = obj_ref["cDomainElementName"]
            unique_objects[obj_name] = obj_ref

    print(
        f"unique objects found {len(unique_objects)} out of {obj_count} total objects"
    )
    print(f"keys={sorted(unique_objects.keys())}")
    return unique_objects


def get_cd(session, cd_name):
    """
    given a composite domain - return the status code and json
    """
    resturl = f"{edcSession.baseUrl}/access/1/catalog/compositedomains/{cd_name}"
    print(f"\tfinding composite domain {cd_name} using {resturl}")
    try:
        resp = edcSession.session.get(
            resturl, headers={"accept": "application/json"}, timeout=3
        )
        # print(resp.status_code)
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to : {resturl}")
        print(e)
        # exit if we can't connect
        sys.exit(1)

    if resp.status_code != 200:
        # some error - e.g. catalog not running, or bad credentials
        # print(f"\terror! {resp.status_code} reason={resp.reason}")
        return resp.status_code, resp.reason

    # must be 200
    # print(resp.status_code, resp.json())
    return resp.status_code, resp.json()


if __name__ == "__main__":
    main()
