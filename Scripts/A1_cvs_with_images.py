import os, sys, re
import json

#-------------- NESTED PATH CORRECTION --------------------------------#
# For all script files, we add the parent directory to the system path
import numpy as np
import pandas as pd

cwd = re.sub(r"[\\]", "/", os.getcwd())
cwd_list = cwd.split("/")
path = re.sub(r"[\\]", "/", sys.argv[0])
path_list = path.split("/")

# either the entire filepath is entered as command i python
if cwd_list[0:3] == path_list[0:3]:
    full_path = path
# or a relative path is entered, in which case we append the path to the cwd_path
else:
    full_path = cwd + "/" + path
# remove the overlap
root_dir = re.search(r"(^.+gender_in_academia)", full_path).group(1)
sys.path.append(root_dir)
#----------------------------------------------------------------------#

import Functions.sql_functions as sql
import Functions.document_extraction as dxtr
from Functions.StringComp import StringComp
from multiprocessing import Pool
from functools import partial

# get all of the researchers


def get_missing_app_ids():
    all_app_ids = sql.select('applications', ['app_id'])
    inserted_app_ids = sql.select('docx_rows', ['app_id']).drop_duplicates()
    inserted_app_ids['complete'] = 1

    df = all_app_ids.merge(inserted_app_ids, how="left", on="app_id")
    incomplete = df.loc[(df['complete'] != 1), :].app_id.values

    print(f"Length of all app_ids is {len(all_app_ids)}")
    print(f"length of inserted app_ids is {len(inserted_app_ids)}")
    print(f"length of incomplete is, {len(incomplete)}")
    return incomplete


def extract_and_upload(table_name, app_id):
    docx_rows = dxtr.extract_docx(app_id)
    sql.upload_to_table(table_name, docx_rows)
    print(f"completed {app_id}")

def insert_docx_rows(app_ids, multiprocessing=True):

    app_ids_with_file = [x[:-5] for x in os.listdir(f"{root_dir}/Data/docx_files")]

    app_ids = set(app_ids).intersection(app_ids_with_file)

    # Create SQL table if it is missing
    # create the progress table
    cols_tups = [('app_id', 'TEXT'), ('row_id', 'INT'), ('text', 'TEXT'), ('style', 'TEXT')]

    if not sql.check_table_exists('docx_rows'):
        sql.create_table('docx_rows', cols_tups, [('app_id', 12), 'row_id'])

    if not multiprocessing:
        for app_id in app_ids:
            docx_rows = dxtr.extract_docx(app_id)
            sql.upload_to_table('docx_rows', docx_rows)
    else:
        p = Pool(8)
        p.map(partial(extract_and_upload, 'docx_rows'), list(app_ids))
        p.close()
        p.join()


def get_app_ids_with_images():
    df = sql.select('docx_rows', ['app_id'], [('style', 'Novalue')]).drop_duplicates()
    app_ids = list(df.app_id.values)

    rows = sql.select("docx_rows", where=[("app_id", app_ids)])
    print(app_ids)
    print(len(app_ids))
    print(rows)

if __name__  == "__main__":
    # sample = dxtr.extract_docx("2014-04192")
    # for rid, row in sample.iterrows():
    #    print(row)
    # print(sample)
    #
    # get_app_ids_with_images()
    # counter = 10
    # while counter > 0:
    #     app_ids = get_missing_app_ids()
    #     try:
    #         insert_docx_rows(app_ids, False)
    #     except:
    #         continue
    #     counter = counter - 1

    app_ids = get_missing_app_ids()
    insert_docx_rows(app_ids, False)