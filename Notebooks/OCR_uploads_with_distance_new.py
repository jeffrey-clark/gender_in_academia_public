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

data_dir = "D:/LocalStore/Gender_in_Academia"
cv_2012_2014 = f"{data_dir}/pdf_files/2012_2014"
cv_2015_2016 = f"{data_dir}/pdf_files/2015_2016"
cv_fps = [f"{cv_2012_2014}/{x}" for x in os.listdir(cv_2012_2014)] + [f"{cv_2015_2016}/{x}" for x in os.listdir(cv_2015_2016)]


def setup():
    # set table name
    table_name = "ocr_extractions_w_distance"
    # create table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('row_id', 'INT'),
                 ('page', 'INT'),
                 ('text', 'TEXT'),
                 ('distance_to_prev_line', "DOUBLE"),
                 ('font_size', "DOUBLE"),
                 ('font_weight', "DOUBLE")
    ]

    if not sql.check_table_exists(table_name):
        sql.create_table(table_name, cols_tups,['app_id', 'row_id'], verbose=False)

    # create status table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('success', 'INT'),
                 ('time', 'DATETIME', 'DEFAULT CURRENT_TIMESTAMP')]
    if not sql.check_table_exists(f'status_{table_name}'):
        sql.create_table(f'status_{table_name}', cols_tups, 'app_id', verbose=False)



def drop_tables():
    table_name = "ocr_extractions_w_distance"
    # drop the table if it exists
    sql.drop_table(table_name)
    sql.drop_table(f"status_{table_name}")

def draw_distance_lines(img, details, line_threshold=10):
    draw = ImageDraw.Draw(img)
    last_bottom = 0  # Bottom position of the last drawn line
    line_positions = []  # List to store the positions of lines drawn

    # Sort details by the 'top' position
    sorted_details = sorted(zip(details['text'], details['top'], details['height']), key=lambda x: x[1])

    for text, top, height in sorted_details:
        text = text.strip()
        bottom = top + height

        # If the text isn't empty and the bottom of the current text block is significantly lower than the last
        if text and (bottom > last_bottom + line_threshold):
            draw.line((0, bottom, img.width, bottom), fill=128, width=1)
            last_bottom = bottom  # Update the position of the last drawn line
            line_positions.append(bottom)  # Store the line position

    return img, line_positions


def find_closest_line_position(word_bottom, line_positions):
    closest_position = None
    min_distance = float('inf')
    for line_pos in line_positions:
        distance = abs(line_pos - word_bottom)
        if distance < min_distance:
            min_distance = distance
            closest_position = line_pos
    return closest_position


# Helper function to calculate font weight
def calculate_font_weight(image, x, y, width, height, threshold=128):
    """
    Calculate the font weight based on the density of black pixels in the bounding box,
    considering only the columns that contain at least one black pixel.

    :param image: PIL.Image object.
    :param x: The x-coordinate of the top-left corner of the bounding box.
    :param y: The y-coordinate of the top-left corner of the bounding box.
    :param width: The width of the bounding box.
    :param height: The height of the bounding box.
    :param threshold: Pixel value threshold to consider as 'black' (0-255).
    :return: Font weight as a ratio of black pixel density.
    """
    # Crop the bounding box from the image
    bbox = image.crop((x, y, x + width, y + height))
    # Convert the image to grayscale and then to a NumPy array
    gray = np.array(bbox.convert('L'))
    # Apply the threshold to get a binary image (1 for black pixels)
    binary_image = gray < threshold
    # Count the black pixels in columns that have at least one black pixel
    black_pixels = np.sum(binary_image, axis=0)
    black_pixels_in_columns = black_pixels[black_pixels > 0]
    total_black_pixels = np.sum(black_pixels_in_columns)
    # Calculate the total number of pixels in the considered columns
    total_pixels = height * len(black_pixels_in_columns)
    return total_black_pixels / total_pixels if total_pixels else 0


def calculate_median_font_size(details, start_index, end_index):
    """
    Calculate the median font size for a line of text by finding the median height
    of non-space and non-punctuation characters.

    :param details: Dictionary containing Tesseract OCR output details.
    :param start_index: The start index of the line in the details dictionary.
    :param end_index: The end index of the line in the details dictionary.
    :return: Median font size of the line.
    """
    char_heights = [
        details['height'][i]
        for i in range(start_index, end_index + 1)
        if details['text'][i].strip() and details['text'][i] not in string.punctuation
    ]
    return median(char_heights) if char_heights else None

    # Convert PDF to list of images
    images = convert_from_path(fp)





def execute_ocr(app_id):
    year = int(app_id[:4])
    if year < 2015:
        fp = f'{cv_2012_2014}/{app_id}.pdf'
    else:
        fp = f'{cv_2015_2016}/{app_id}.pdf'

    images = convert_from_path(fp)

    # Initialize data list
    data = []
    page = 1  # Initialize page number
    row_id = 0  # Initialize row ID

    for img in images:
        details = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        img, line_positions = draw_distance_lines(img, details)

        img.save(f"{data_dir}/debug/{app_id}_page{page}.png")
        print(f"SAVED {data_dir}/debug/{app_id}_page{page}.png")

        prev_line_position = None
        current_line_text = ""
        current_line_top = None
        current_line_height = None
        current_line_num = None
        line_start_index = 0

        for i in range(len(details['text'])):
            text = details['text'][i]
            top = details['top'][i]
            height = details['height'][i]

            if current_line_top is None or top < current_line_top:
                current_line_top = top
            if current_line_height is None or height > current_line_height:
                current_line_height = height

            if current_line_num is not None and details['line_num'][i] != current_line_num:
                # Process the current line if it has text
                if current_line_text.strip():
                    font_size = calculate_median_font_size(details, line_start_index, i - 1)
                    font_weight = calculate_font_weight(img, details['left'][i-1], current_line_top, details['width'][i-1], current_line_height)
                    current_line_bottom = current_line_top + current_line_height
                    closest_line_pos = find_closest_line_position(current_line_bottom, line_positions)
                    distance_to_prev_drawn_line = closest_line_pos - current_line_bottom if closest_line_pos is not None else None

                    # Append line data to the list
                    data.append({
                        "app_id": app_id,
                        "row_id": row_id,
                        "page": page,
                        "text": current_line_text.strip(),
                        "distance_to_prev_drawn_line": distance_to_prev_drawn_line,
                        "font_size": font_size,
                        "font_weight": font_weight
                    })

                    # Update prev_line_position for the next line
                    prev_line_position = current_line_bottom

                # Reset the current line variables
                current_line_text = ""
                current_line_top = None
                current_line_height = None
                row_id += 1
                line_start_index = i

            current_line_num = details['line_num'][i]
            current_line_text += f"{text} " if text.strip() != "" else ""

        # Process the last line if there is text
        if current_line_text.strip():
            font_size = calculate_median_font_size(details, line_start_index, len(details['text']) - 1)
            font_weight = calculate_font_weight(img, details['left'][-1], current_line_top, details['width'][-1], current_line_height)
            current_line_bottom = current_line_top + current_line_height
            closest_line_pos = find_closest_line_position(current_line_bottom, line_positions)
            distance_to_prev_drawn_line = closest_line_pos - current_line_bottom if closest_line_pos is not None else None

            data.append({
                "app_id": app_id,
                "row_id": row_id,
                "page": page,
                "text": current_line_text.strip(),
                "distance_to_prev_drawn_line": distance_to_prev_drawn_line,
                "font_size": font_size,
                "font_weight": font_weight
            })

        page += 1

    df = pd.DataFrame(data)
    return df
    #sql.upload_to_table("ocr_extractions_w_distance", df)


def try_execute_ocr(app_id):
    try:
        execute_ocr(app_id)
        success = 1
        #print("completed", app_id)
    except:
        success = 0

    #sql.insert('status_ocr_extractions_w_distance', [('app_id', app_id), ('success', success)], verbose=False)


if __name__ == '__main__':


    df = execute_ocr("2012-00230")

    df["median_dist"] = df.distance_to_prev_drawn_line.median()
    df["new_entry"] =  (df.distance_to_prev_drawn_line) > 1.5*df.distance_to_prev_drawn_line.median()

    df.to_excel(f"{data_dir}/debug/df.xlsx")



    raise ValueError('aa')

    #drop_tables()

    setup()

    # Let us run the model on each row
    all_app_ids = get_applications(format="df").app_id.to_list()

    # filter out the completed applications
    completed = sql.select("status_ocr_extractions_w_distance", ["app_id"], ['success', 1]).app_id.to_list()
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
        for _ in tqdm(pool.imap_unordered(try_execute_ocr, remaining), total=len(remaining), desc='Processing', unit='app'):
            pass  # You can do something with each result if you need to, or just pass.

    print("All applications processed.")