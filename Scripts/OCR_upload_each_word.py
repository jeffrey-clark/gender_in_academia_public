from Functions.j_functions import *
from Functions.functions import *


import Functions.functions as f
import Functions.sql_functions as sql
from multiprocessing import Pool
from functools import partial

from tqdm import tqdm

from pdf2image import convert_from_path
import pytesseract

import pandas as pd
import numpy as np
import string
from statistics import median
from PIL import Image, ImageDraw, ImageColor
import random

data_dir = "D:/LocalStore/Gender_in_Academia"
cv_2012_2014 = f"{data_dir}/pdf_files/2012_2014"
cv_2015_2016 = f"{data_dir}/pdf_files/2015_2016"
cv_fps = [f"{cv_2012_2014}/{x}" for x in os.listdir(cv_2012_2014)] + [f"{cv_2015_2016}/{x}" for x in os.listdir(cv_2015_2016)]


def setup(table_name):
    # create table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('word_id', 'INT'),
                 ('page', 'INT'),
                 ('word', 'TEXT'),
                 ('word_length', 'INT'),
                 ('left', 'INT'),
                 ('top', 'INT'),
                 ('right', 'INT'),
                 ('bottom', 'INT'),
                 ('height', 'INT'),
                 ('width', 'INT')
                 ]

    if not sql.check_table_exists(table_name):
        sql.create_table(table_name, cols_tups,['app_id', 'word_id'], verbose=False)

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


def execute_ocr(app_id, table_name=None):
    # Define paths for PDFs based on year
    year = int(app_id[:4])
    if year < 2015:
        fp = f'{cv_2012_2014}/{app_id}.pdf'
    else:
        fp = f'{cv_2015_2016}/{app_id}.pdf'

    images = convert_from_path(fp, dpi=300)
    data = []
    page = 1

    # tesseract config
    t_config = r'-l swe'

    word_id = 1

    for img in images:
        details = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT, config=t_config)

        for i, word in enumerate(details['text']):
            word_stripped = word.strip()
            if word_stripped:  # Check if the word is not just whitespace
                top, left, width, height = details['top'][i], details['left'][i], details['width'][i], details['height'][i]
                bottom = top + height
                right = left + width

                # Append line data to the list
                data.append({
                    "app_id": app_id,
                    "word_id": word_id,
                    "page": page,
                    "word": word_stripped,
                    "word_length": len(word_stripped),
                    "left": left,
                    "top": top,
                    "right": right,
                    "bottom": bottom,
                    "height": height,
                    "width": width,
                    # "font_size": font_size,  # Placeholder
                    # "font_weight": font_weight  # Placeholder
                })
                word_id += 1

                    # Increment the page counter once all OCR details for the current page have been processed
        page += 1

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Upload
    if table_name is not None:
        sql.upload_to_table(table_name, df)
    else:
        return df


def try_execute_ocr(app_id, table_name=None):
    try:
        execute_ocr(app_id, table_name)
        success = 1
        #print("completed", app_id)
    except:
        success = 0

    sql.insert(f'status_{table_name}', [('app_id', app_id), ('success', success)], verbose=False)


if __name__ == '__main__':

    # # #df = execute_ocr("2012-00230")  # has columns
    # df = execute_ocr("2012-00202", table_name="ocr_extractions_words")
    # df.to_excel(f"{data_dir}/debug/df.xlsx", index=False)
    # raise ValueError('aa')



    table_name = "ocr_extractions_words"

    # drop_tables(table_name)

    setup(table_name)

    # Let us run the model on each row
    all_app_ids = get_applications(format="df").app_id.to_list()

    # filter out the completed applications
    completed = sql.select(f"status_{table_name}", ["app_id"], ['success', 1]).app_id.to_list()
    remaining = list(np.setdiff1d(all_app_ids, completed))

    print(f"{len(remaining)} app_ids remaining")

    #execute_ocr("2012-00222")

    # with Pool(processes=14) as pool:
    #     result = pool.map(try_execute_ocr, remaining)
    #     print(result)

    # Prepare the pool and the tqdm progress bar
    with Pool(processes=16) as pool:
        # Here we use `tqdm` to create a progress bar that tracks the completion of `try_execute_ocr` calls.
        # `total=len(remaining)` informs `tqdm` of the total number of items to track.
        g = partial(try_execute_ocr, table_name=table_name)
        for _ in tqdm(pool.imap_unordered(g, remaining), total=len(remaining), desc='OCR SCANNING', unit='Documents'):
            pass  # You can do something with each result if you need to, or just pass.

    print("All applications processed.")