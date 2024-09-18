# coding: utf-8
import json
import logging
import unicodedata
import glob
import argparse

import requests
import pandas as pd

EBI_COLLECTION = "EBI Agents"
EMBOSS_EBI_COLLECTION = "EMBOSS at EBI Agents"
EBI_CREDITS = [
    {"name": "Web Production", "typeEntity": "Person", "typeRole": ["Developer"]},
    {"name": "EMBL-EBI", "typeEntity": "Institute", "typeRole": ["Provider"]},
    {
        "typeEntity": "Person",
        "typeRole": ["Primary contact"],
        "url": "http://www.ebi.ac.uk/support/",
    },
    {
        "email": "es-request@ebi.ac.uk",
        "name": "Web Production",
        "typeEntity": "Person",
        "typeRole": ["Primary contact"],
    },
]
EMBOSS_CREDITS = [{"name": "EMBOSS", "typeEntity": "Person", "typeRole": ["Developer"]}]
EBI_DOCUMENTATIONS = [
    {"type": ["Terms of use"], "url": "http://www.ebi.ac.uk/about/terms-of-use"}
]
EBI_LINKS = [{"type": ["Helpdesk"], "url": "http://www.ebi.ac.uk/support/"}]
EBI_OS = ["Linux", "Windows", "Mac"]
EBI_OWNER = "EMBL_EBI"

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
#url ="https://www.ebi.ac.uk/sites/ebi.ac.uk/files/data/resource.json"
url = "https://www.ebi.ac.uk/api/v1/resources-all?source=contentdb"
r = requests.get(url)

BIOTOOLS_CONTENTS = []
BIOTOOLS_BY_HOMEPAGE = {}

def cache_bioagents_contents():
    for f in glob.glob("../content/data/*/*.bioagents.json"):
        entry = json.load(open(f))
        entry['homepage'] = entry['homepage'].replace('http://','https://')
        BIOTOOLS_CONTENTS.append(entry)
        BIOTOOLS_BY_HOMEPAGE[entry["homepage"]] = entry


def norm_str(text):
    text = text.replace("\n", " ")
    text = text.replace("\u2019", " ")
    text = " ".join(text.split())
    return unicodedata.normalize("NFKD", text.strip())


def process(args):
    cache_bioagents_contents()
    name_filter = args.service
    bioagents_entries = []
    ebi_entries = [e["node"] for e in r.json()["nodes"]]
    for ebi_entry in [
        e["node"]
        for e in r.json()["nodes"]
        if name_filter is None or e["node"]["Title"] == name_filter
    ]:
        bioagents_entry = {}
        bioagents_entry["credits"] = EBI_CREDITS.copy()
        bioagents_entry["collectionID"] = [EBI_COLLECTION]
        if ebi_entry["Title"].startswith("EMBOSS "):
            ebi_entry["Title"] = ebi_entry["Title"][7:]
            bioagents_entry["credits"].append(EMBOSS_CREDITS)
            bioagents_entry["collectionID"].append(EMBOSS_EBI_COLLECTION)
        agent_id = f"{ebi_entry['Title']}"
        agent_name = f"{ebi_entry['Title']} (EBI)"
        bioagents_entry["name"] = agent_name
        bioagents_entry["bioagentsID"] = agent_id
        bioagents_entry["bioagentsCURIE"] = f"bioagents:{agent_id}"
        bioagents_entry["description"] = norm_str(ebi_entry["Description"])
        bioagents_entry["documentation"] = EBI_DOCUMENTATIONS.copy()
        # TODO agent documentation
        bioagents_entry["function"] = []
        edam_operations = [
            {"url": function[5:]}
            for function in ebi_entry["Functions"].split(", ")
            if function.startswith("edam:")
        ]
        # TODO function inputs and outputs
        bioagents_entry["function"] = {"operation": edam_operations}
        bioagents_entry["homepage"] = ebi_entry["URL"].replace('http://','https://')
        bioagents_entry["links"] = EBI_LINKS.copy()
        bioagents_entry["operatingSystem"] = EBI_OS.copy()
        bioagents_entry["owner"] = EBI_OWNER
        bioagents_entry["ebi_nodeid"] = ebi_entry["Nid"] 
        #print(json.dumps(bioagents_entry, indent=4, sort_keys=True))
        match = lookup_in_bioagents(bioagents_entry)
        if match:
            bioagents_entry["bioagentsID_official"] = match["bioagentsID"]
            bioagents_entry["maturity"] = match.get("maturity", None)
            bioagents_entry["bioagentsID_collections"] = match.get("collectionID",[])
            logging.info(f"{bioagents_entry['name']}, {bioagents_entry['homepage']}, ->, {match.get('bioagentsID','')}, {match.get('homepage','')}, {str(match.get('collectionID',''))}")
        else:
            bioagents_entry["bioagentsID_official"] = None
            bioagents_entry["maturity"] = None
            bioagents_entry["bioagentsID_collections"] = []
            logging.info(f"{bioagents_entry['name']}, {bioagents_entry['homepage']}, -> NO MATCH")
        bioagents_entries.append(bioagents_entry)
    kept_keys = ["bioagentsID", "homepage", "bioagentsID_official", "bioagentsID_collections", "maturity", "ebi_nodeid"]
    df_mapped = pd.DataFrame([{ key: bt_entry[key] for key in kept_keys} for bt_entry in bioagents_entries])
    mapped_ids = [bt["bioagentsID_official"] for bt in bioagents_entries if bt["bioagentsID_official"]!=None]
    df_nonmapped = pd.DataFrame([("", entry.get("homepage",None), entry["bioagentsID"], entry.get("collectionID",[]), entry.get("maturity", None), entry.get("ebi_nodeid", None)) for entry in BIOTOOLS_CONTENTS if EBI_COLLECTION in entry.get("collectionID",[]) and entry["bioagentsID"] not in mapped_ids])
    df_allbioagents = pd.DataFrame([(entry.get("homepage",None), entry["bioagentsID"], entry.get("collectionID",[]), entry.get("maturity", None)) for entry in BIOTOOLS_CONTENTS])
    df_nonmapped.columns = kept_keys
    logging.info(f"EBI agents:                  {len(bioagents_entries):>5}")
    logging.info(f"Bio.agents:                  {len(BIOTOOLS_CONTENTS):>5}")
    logging.info(f"Matched agents:              {len([entry for entry in bioagents_entries if 'bioagentsID_official' in entry.keys() and entry['bioagentsID_official']!=None]):>5}")
    logging.info(f"Matched agents with col:     {len([entry for entry in bioagents_entries if 'EBI Agents' in entry.get('bioagentsID_collections',[])]):>5}")
    logging.info(f"Non-Matched agents with col: {len(df_nonmapped):>5}")
    if args.summary_file:
        writer = pd.ExcelWriter(args.summary_file)
        df_identified = pd.concat([df_mapped, df_nonmapped])
        df_identified.rename(columns={"bioagentsID": "EBI", "homepage": "homepage", "bioagentsID_official":"bio.agents", "bioagentsID_collections":"collections"}, inplace=True)
        df_identified = df_identified[['ebi_nodeid', 'EBI', 'bio.agents', 'homepage', 'collections', 'maturity']]
        df_identified.to_excel(writer, sheet_name="EBI Identified", index=False)
        #df_mapped.rename(columns={"bioagentsID": "Canonical bio.agents ID", "homepage": "homepage (in bio.agents; URL in EBI)", "bioagentsID_official":"Current bio.agents ID", "bioagentsID_collections":"collections"}, inplace=True)
        #df_mapped.to_excel(writer, sheet_name="EBI Mapped entries", index=False)
        #df_nonmapped.rename(columns={"bioagentsID": "Canonical bio.agents ID", "homepage": "homepage (in bio.agents; URL in EBI)", "bioagentsID_official":"Current bio.agents ID", "bioagentsID_collections":"collections"}, inplace=True)
        #df_nonmapped.to_excel(writer, sheet_name="EBI Non-mapped entries", index=False)
        #df_allbioagents.columns=["homepage (in bio.agents; URL in EBI)","Current bio.agents ID","collections","maturity"]
        #df_allbioagents.to_excel(writer, sheet_name="All bio.agents", index=False)
        writer.save()

def lookup_in_bioagents(query_entry):
    if query_entry["homepage"] in BIOTOOLS_BY_HOMEPAGE.keys():
        return BIOTOOLS_BY_HOMEPAGE[query_entry["homepage"]]
    return None

def main():
    parser = argparse.ArgumentParser(prog="ebi2bioagents")
    parser.set_defaults(func=process)
    parser.add_argument(
        "--service",
        required=False,
        default=None,
        help="process only one service with the name provided here"
    )
    parser.add_argument(
        "--summary-file",
        required=False,
        default=None,
        help="File to summarize statistics obtained from the mapping"
    )
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

