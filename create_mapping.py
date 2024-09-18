# coding: utf-8
import json
import logging
import unicodedata
import glob
import argparse

import requests
import pandas as pd

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
url = "https://www.ebi.ac.uk/api/v1/resources-all?source=contentdb"
r = requests.get(url)

BIOTOOLS_CONTENTS = []
BIOTOOLS_BY_HOMEPAGE = {}
EBI_COLLECTION = "EBI Agents"


def cache_bioagents_contents():
    for f in glob.glob("../content/data/*/*.bioagents.json"):
        entry = json.load(open(f))
        entry["homepage"] = entry["homepage"].replace("http://", "https://")
        BIOTOOLS_CONTENTS.append(entry)
        BIOTOOLS_BY_HOMEPAGE[entry["homepage"]] = entry


def norm_str(text):
    text = text.replace("\n", " ")
    text = text.replace("\u2019", " ")
    text = " ".join(text.split())
    return unicodedata.normalize("NFKD", text.strip())


def lookup_in_bioagents(query_entry):
    if query_entry["URL"] in BIOTOOLS_BY_HOMEPAGE.keys():
        return BIOTOOLS_BY_HOMEPAGE[query_entry["URL"]]
    return None


def process(args):
    cache_bioagents_contents()
    ebi_entries_mapped = []
    ebi_entries = [e["node"] for e in r.json()["nodes"]]
    for ebi_entry in [
        e["node"] for e in r.json()["nodes"] if e["node"]["Domain"] != "Project Website"
    ]:
        ebi_entry["URL"] = ebi_entry["URL"].replace("http://", "https://")
        ebi_entry["Description"] = norm_str(ebi_entry["Description"])
        ebi_entry["Short description"] = norm_str(ebi_entry["Short description"])
        ebi_entry["Logo"] = ebi_entry["Logo"]["src"]
        ebi_entry["Logo-thumbnail"] = ebi_entry["Logo-thumbnail"]["src"]
        del ebi_entry["short_description"]
        match = lookup_in_bioagents(ebi_entry)
        if match:
            ebi_entry["bio.agents ID"] = match["bioagentsID"]
            ebi_entry["bio.agents maturity"] = match.get("maturity", None)
            ebi_entry["bio.agents collections"] = match.get("collectionID", [])
        else:
            ebi_entry["bio.agents ID"] = None
            ebi_entry["bio.agents maturity"] = None
            ebi_entry["bio.agents collections"] = None
        ebi_entries_mapped.append(ebi_entry)
    df_mapped = pd.DataFrame(ebi_entries_mapped)

    mapped_ids = [
        bt["bio.agents ID"] for bt in ebi_entries_mapped if bt["bio.agents ID"] != None
    ]
    df_nonmapped = pd.DataFrame(
        [
            {
                "bio.agents ID": entry["bioagentsID"],
                "bio.agents collections": entry.get("collectionID", []),
                "bio.agents maturity": entry.get("maturity", None),
                "Category": None,
                "Description": None,
                "Domain": None,
                "Email": None,
                "Functions": None,
                "Keywords": None,
                "Logo": None,
                "Logo-thumbnail": None,
                "Maintainer": None,
                "Nid": None,
                "Popular": None,
                "Primary contact": None,
                "Short description": None,
                "Short name": None,
                "Title": None,
                "URL": entry["homepage"],
                "Weight": None,
                "data_licence_type": None,
                "maturity": None,
                "resource_api_compliant": None,
                "resource_out_of_ebi_ctrl": None,
                "resource_rest_landing_page": None,
            }
            for entry in BIOTOOLS_CONTENTS
            if EBI_COLLECTION in entry.get("collectionID", [])
            and entry["bioagentsID"] not in mapped_ids
        ]
    )
    logging.info(f"EBI agents:                  {len(ebi_entries):>5}")
    logging.info(f"Bio.agents:                  {len(BIOTOOLS_CONTENTS):>5}")
    logging.info(
        f"Matched agents:              {len([entry for entry in ebi_entries_mapped if 'bio.agents ID' in entry.keys() and entry['bio.agents ID']!=None]):>5}"
    )
    logging.info(f"Non-Matched agents with col: {len(df_nonmapped):>5}")
    if args.summary_file:
        writer = pd.ExcelWriter(args.summary_file, engine='xlsxwriter')

        workbook  = writer.book

        instructions_sheet = workbook.add_worksheet("Instructions")
        instructions_sheet.set_row(0, 150)
        instructions_sheet.set_column('A:A', 2500)
        instructions_format = workbook.add_format()
        instructions_format.set_text_wrap()
        bold = workbook.add_format({'bold': True})
        text = [bold,
                "Goal of this workbook\n",
                "Link the existing entries of bio.agents entries to the contents database, to make sure we can synchronize the non-extinct EBI services between the EBI Contents DB and bio.agents\n",
                bold,
                "Where do the data of this workbook come from?\n",
                f"The entries in the EBI-bio.agents worksheet were retrieved and merged from bio.agents and contents DB. They are a union of all contents DB entries (sometimes automatically mapped to a bio.agents ID using the URL for the service), and the bio.agents entries identified as EBI because they belonged to the \"{EBI_COLLECTION}\" collection even though no corresponding entry in contents DB was identified.\n",
                bold,
                "What to do with these lines?\n",
                "- if a content DB entry has an bio.agents ID, do not do anything unless you judge it invalid.\n",
                "- if a content DB entry does not have a bio.agents ID, decide or not whether we want to create one, and type requested ID in bold in column A\n",
                "- if a bio.agents entry is not mapped to a content DB entry, suggest an existing content DB Nid in column B or specify other action in column Z"
        ]
        rc = instructions_sheet.write_rich_string("A1", *text, instructions_format)



        df_identified = pd.concat([df_mapped, df_nonmapped])
        df_identified = df_identified[
            [
                "bio.agents ID",
                "bio.agents collections",
                "bio.agents maturity",
                "Nid",
                "Title",
                "URL",
                "Category",
                "Description",
                "Domain",
                "Email",
                "Functions",
                "Keywords",
                "Maintainer",
                "Popular",
                "Primary contact",
                "Short description",
                "Short name",
                "Weight",
                "data_licence_type",
                "maturity",
                "resource_api_compliant",
                "resource_out_of_ebi_ctrl",
                "resource_rest_landing_page",
                "Logo",
                "Logo-thumbnail",
            ]
        ]
        df_identified.to_excel(writer, sheet_name="EBI-bio.agents", index=False)
        # add conditional formatting to the worksheet
        worksheet = writer.sheets["EBI-bio.agents"]
        research_format = workbook.add_format({'font_color': '#045D5D'})
        worksheet.conditional_format('A2:Z1048576', {'type': 'formula',
                                          'criteria': '=LEFT($I2, 8)="Research"',
                                          'format': research_format})
        mapped_format = workbook.add_format({'bg_color': '#5cb85c'})
        worksheet.conditional_format('A2:Z1048576', {'type': 'formula',
                                          'criteria': '=AND($A2<>"",$D2<>"")',
                                          'format': mapped_format})
        ebi_unmapped_format = workbook.add_format({'bg_color': '#f0ad4e'})
        worksheet.conditional_format('A2:Z1048576', {'type': 'formula',
                                          'criteria': '=AND($A2="",$D2<>"")',
                                          'format': ebi_unmapped_format})
        bioagents_unmapped_format = workbook.add_format({'bg_color': '#5bc0de'})
        worksheet.conditional_format('A2:Z1048576', {'type': 'formula',
                                          'criteria': '=AND($A2<>"",$D2="")',
                                          'format': bioagents_unmapped_format})
        writer.save()


def main():
    parser = argparse.ArgumentParser(prog="ebi2bioagents")
    parser.set_defaults(func=process)
    parser.add_argument(
        "--service",
        required=False,
        default=None,
        help="process only one service with the name provided here",
    )
    parser.add_argument(
        "--summary-file",
        required=False,
        default=None,
        help="File to summarize statistics obtained from the mapping",
    )
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
