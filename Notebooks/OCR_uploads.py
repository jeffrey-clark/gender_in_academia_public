from Functions.j_functions import *
from Functions.functions import *

from pdf2image import convert_from_path
import pytesseract
import pandas as pd


import Functions.functions as f
import Functions.sql_functions as sql
from multiprocessing import Pool
from functools import partial

from tqdm import tqdm

data_dir = "D:/LocalStore/Gender_in_Academia"

data_dir = "D:/LocalStore/Gender_in_Academia"
cv_2012_2014 = f"{data_dir}/pdf_files/2012_2014"
cv_2015_2016 = f"{data_dir}/pdf_files/2015_2016"
cv_fps = [f"{cv_2012_2014}/{x}" for x in os.listdir(cv_2012_2014)] + [f"{cv_2015_2016}/{x}" for x in os.listdir(cv_2015_2016)]


def setup():
    # set table name
    table_name = "ocr_extractions"
    # create table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('row_id', 'INT'),
                 ('page', 'INT'),
                 ('text', 'TEXT')]
    if not sql.check_table_exists(table_name):
        sql.create_table(table_name, cols_tups,['app_id', 'row_id'], verbose=False)

    # create status table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('success', 'INT'),
                 ('time', 'DATETIME', 'DEFAULT CURRENT_TIMESTAMP')]
    if not sql.check_table_exists(f'status_{table_name}'):
        sql.create_table(f'status_{table_name}', cols_tups, 'app_id', verbose=False)



def drop_tables():
    table_name = "ocr_extractions"
    # drop the table if it exists
    sql.drop_table(table_name)
    sql.drop_table(f"status_{table_name}")


def execute_ocr(app_id):
    year = int(app_id[:4])
    if year < 2015:
        fp = f'{cv_2012_2014}/{app_id}.pdf'
    else:
        fp = f'{cv_2015_2016}/{app_id}.pdf'


    # Convert PDF to list of images
    images = convert_from_path(fp)

    data = []
    row_id = 1
    for page, img in enumerate(images, start=1):
        text = pytesseract.image_to_string(img, lang="swe")
        #print(f"Extracted Text from Page {page}:")
        rows = text.split("\n")
        for row in rows:
            # if row.strip():  # This will ignore empty rows

            data.append({"app_id": app_id, "row_id": row_id, "page": page, "text": row})
            row_id += 1

    # Create a DataFrame
    df = pd.DataFrame(data)

    sql.upload_to_table("ocr_extractions", df)


def try_execute_ocr(app_id):
    try:
        execute_ocr(app_id)
        success = 1
        print("completed", app_id)
    except:
        success = 0

    sql.insert('status_ocr_extractions', [('app_id', app_id), ('success', success)], verbose=False)


if __name__ == '__main__':

    #drop_tables()

    setup()

    # Let us run the model on each row
    all_app_ids = get_applications(format="df").app_id.to_list()

    # filter out the completed applications
    completed = sql.select("status_ocr_extractions", ["app_id"], ['success', 1]).app_id.to_list()
    remaining = list(np.setdiff1d(all_app_ids, completed))

    print(f"{len(remaining)} app_ids remaining")

    #execute_ocr("2012-00222")

    with Pool(processes=14) as pool:
        result = pool.map(try_execute_ocr, remaining)
        print(result)