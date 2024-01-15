import os
import numpy as np
import spacy
from spacy import displacy
from spacy.matcher import Matcher

from Functions.j_functions import *
from Functions.functions import *

import Models.MsWordModel as WM

import Functions.functions as f
import Functions.sql_functions as sql
from multiprocessing import Pool
from functools import partial

from tqdm import tqdm

data_dir = "D:/LocalStore/Gender_in_Academia"


def get_extraction(app_id, ocr_table_name):
    return sql.select(ocr_table_name, ["*"], [('app_id', app_id)])


def setup(table_name):

    # create table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('row_id', 'INT'),
                 ('nlp', 'TEXT')]
    if not sql.check_table_exists(table_name):
        sql.create_table(table_name, cols_tups,['app_id', 'row_id'], verbose=False)

    # create status table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('success', 'INT'),
                 ('time', 'DATETIME', 'DEFAULT CURRENT_TIMESTAMP')]
    if not sql.check_table_exists(f'status_{table_name}'):
        sql.create_table(f'status_{table_name}', cols_tups, 'app_id', verbose=False)



def drop_tables(table_name):
    # drop the table if it exists
    sql.drop_table(table_name)
    sql.drop_table(f"status_{table_name}")




def execute_nlp(app_id, nlp, ocr_table_name, nlp_table_name):

    def get_ents(x):
        doc = nlp(x)
        ents = [(x.start_char, x.end_char, x.label_) for x in doc.ents]
        return json.dumps(ents)

    df = get_extraction(app_id, ocr_table_name)

    if df.empty:
        return 0

    df["nlp"] = df.text.apply(get_ents)

    df = df[["app_id", "row_id", "nlp"]]

    # here we do the uploading
    sql.upload_to_table(nlp_table_name, df)
    return 1


def try_execute_nlp(app_id, nlp, ocr_table_name, nlp_table_name):
    try:
        success = execute_nlp(app_id, nlp, ocr_table_name, nlp_table_name)
        #print("completed", app_id)
    except:
        success = 0

    sql.insert(f'status_{nlp_table_name}', [('app_id', app_id), ('success', success)], verbose=False)


if __name__ == '__main__':

    ocr_table_name = "ocr_extractions_final"
    nlp_table_name = "nlp_final"

    # If we are restarting, drop tables
    # drop_tables(nlp_table_name)

    # first we set up
    setup(nlp_table_name)

    # Let us run the model on each row
    all_app_ids = get_applications(format="df").app_id.to_list()

    # filter out the completed applications
    completed = sql.select(f"status_{nlp_table_name}", ["app_id"], ['success', 1]).app_id.to_list()
    remaining = list(np.setdiff1d(all_app_ids, completed))

    print(f"{len(remaining)} app_ids remaining")

    nlp_model = spacy.load("en_core_web_trf")

    # Assuming try_execute_nlp is a function you've defined, and nlp_model and remaining are already set up
    g = partial(try_execute_nlp, nlp=nlp_model, ocr_table_name=ocr_table_name, nlp_table_name=nlp_table_name)

    with Pool(processes=16) as pool:
        # Use pool.imap_unordered to get the iterator
        # Use tqdm to create a progress bar
        results = []
        for result in tqdm(pool.imap_unordered(g, remaining), total=len(remaining), desc='Processing NLP', unit='Document'):
            results.append(result)  # Collect results here
            # You can process results individually as they come if needed

    print("All NLP Completed")