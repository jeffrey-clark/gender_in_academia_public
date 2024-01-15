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
                 ('row_id', 'INT'),
                 ('page', 'INT'),
                 ('text', 'TEXT'),
                 ('text_length', 'INT'),
                 ('left', 'INT'),
                 ('top', 'INT'),
                 ('right', 'INT'),
                 ('bottom', 'INT'),
                 ('height', 'INT'),
                 ('width', 'INT'),
                 ('dist', 'INT'),
                 ]

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



def add_line_to_data(line_data, data, app_id, row_id, page):
    # Unpack line data
    left, top, right, bottom, words = line_data

    height = bottom - top
    width = right - left

    # Calculate line level attributes if needed, e.g. font size, font weight
    # For now, let's just set them to None or some default value
    font_size, font_weight = None, None  # These would need to be calculated based on your requirements

    # Combine words into a single text string
    text = ' '.join(words)
    text_length = len(text)

    # Append line data to the list
    data.append({
        "app_id": app_id,
        "row_id": row_id,
        "page": page,
        "text": text.strip(),
        "text_length": text_length,
        "left": left,
        "top": top,
        "right": right,
        "bottom": bottom,
        "height": height,
        "width": width,
        # "font_size": font_size,  # Placeholder
        # "font_weight": font_weight  # Placeholder
    })

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

    row_id = 1

    for img in images:
        details = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT, config=t_config)

        line_data = []
        prev_left = None  # Initialize previous left position

        for i, word in enumerate(details['text']):
            print(word)
            if word.strip():  # Check if the word is not just whitespace
                top, left, width, height = details['top'][i], details['left'][i], details['width'][i], details['height'][i]
                bottom = top + height
                right = left + width

                # If there's no previous left, or current left is less than previous,
                # or the current top is more than 200 pixels above the previous bottom, it's a new line
                is_new_line = prev_left is None or left < prev_left or (prev_bottom is not None and top < prev_bottom - 200)
                if is_new_line:
                    # If we have line data, it means we have reached a new line; add existing line data to the list
                    if line_data:
                        add_line_to_data(line_data, data, app_id, row_id, page)
                        row_id += 1

                    # Start new line data with the current word
                    line_data = [left, top, right, bottom, [word]]
                else:
                    # Continue the existing line by extending its boundaries and appending the word
                    line_data[2] = max(line_data[2], right)  # Update right boundary if needed
                    line_data[3] = max(line_data[3], bottom)  # Update bottom boundary if needed
                    line_data[4].append(word)  # Append the word to the current line

                prev_left = right  # Update previous left to the right boundary of the current word
                prev_bottom = bottom  # Update previous bottom to the bottom boundary of the current word

            # After processing each word, check if we're at the end of the OCR details for the current page
            if i == len(details['text']) - 1 and line_data:
                # Add the last line data to the list
                add_line_to_data(line_data, data, app_id, row_id, page)

        # Increment the page counter once all OCR details for the current page have been processed
        page += 1

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # POST PROCESSING

    # Calculate distance
    df["dist"] = df.top - df.bottom.shift()

    # Determine the first row_id for each page
    first_row_ids = df.groupby('page')['row_id'].transform('min')

    # Set 'dist' to NaN where the row_id is the first row_id of the respective page
    df.loc[df['row_id'] == first_row_ids, 'dist'] = np.nan

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

    # #df = execute_ocr("2012-00230")  # has columns
    df = execute_ocr("2012-04184")
    df.to_excel(f"{data_dir}/debug/df.xlsx", index=False)
    raise ValueError('aa')



    table_name = "ocr_extractions_final"

    #drop_tables(table_name)

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
    with Pool(processes=14) as pool:
        # Here we use `tqdm` to create a progress bar that tracks the completion of `try_execute_ocr` calls.
        # `total=len(remaining)` informs `tqdm` of the total number of items to track.
        g = partial(try_execute_ocr, table_name=table_name)
        for _ in tqdm(pool.imap_unordered(g, remaining), total=len(remaining), desc='OCR SCANNING', unit='Documents'):
            pass  # You can do something with each result if you need to, or just pass.

    print("All applications processed.")