from Functions.j_functions import *
from Functions.functions import *


import Functions.functions as f
import Functions.sql_functions as sql
from multiprocessing import Pool
from functools import partial

from tqdm import tqdm

from pdf2image import convert_from_path
import pytesseract
from PIL import Image
import pandas as pd
import numpy as np
import string
from statistics import median

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
    row_id = 0
    page = 1

    # Process each image
    for img in images:
        # Use pytesseract to get OCR output as a dict
        details = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)

        # Initialize variables for tracking text and bounding box info
        prev_non_empty_bottom = None
        current_line_text = ""
        current_line_top = None
        current_line_height = None
        current_line_num = None

        # Line start and end indices for font size calculation
        line_start_index = 0

        for i in range(len(details['text'])):
            text = details['text'][i]
            if current_line_num is not None and details['line_num'][i] != current_line_num:
                # Line has changed
                if current_line_text.strip():
                    # There's text in the current line
                    distance = (current_line_top - prev_non_empty_bottom) if prev_non_empty_bottom is not None else None
                    prev_non_empty_bottom = current_line_top + current_line_height

                    # Calculate median font size
                    font_size = calculate_median_font_size(details, line_start_index, i - 1)

                    # Calculate font weight using previously defined function
                    font_weight = calculate_font_weight(
                        img,
                        details['left'][i-1],
                        current_line_top,
                        details['width'][i-1],
                        current_line_height
                    )
                else:
                    # Current line is empty
                    distance = None
                    font_size = None
                    font_weight = None

                # Append line data to the list
                data.append({
                    "app_id": app_id,
                    "row_id": row_id,
                    "page": page,
                    "text": current_line_text.strip(),
                    "distance_to_prev_line": distance,
                    "font_size": font_size,
                    "font_weight": font_weight
                })

                # Reset variables for the next line
                current_line_text = ""
                current_line_top = None
                current_line_height = None
                row_id += 1

                # Set new line start index
                line_start_index = i

            # Update current line info
            current_line_num = details['line_num'][i]
            current_line_text += f"{text} " if text.strip() != "" else ""
            top = details['top'][i]
            height = details['height'][i]

            # Update the bounding box
            if current_line_top is None or top < current_line_top:
                current_line_top = top
            if current_line_height is None or height > current_line_height:
                current_line_height = height

        # Process the last line if there's any text
        if current_line_text.strip():
            distance = (current_line_top - prev_non_empty_bottom) if prev_non_empty_bottom is not None else None
            font_size = calculate_median_font_size(details, line_start_index, len(details['text']) - 1)
            font_weight = calculate_font_weight(
                img,
                details['left'][i],
                current_line_top,
                details['width'][i],
                current_line_height
            )
            data.append({
                "app_id": app_id,
                "row_id": row_id,
                "page": page,
                "text": current_line_text.strip(),
                "distance_to_prev_line": distance,
                "font_size": font_size,
                "font_weight": font_weight
            })

        # Increment the page number
        page += 1

    # Convert the data list into a DataFrame
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

    sql.insert('status_ocr_extractions_w_distance', [('app_id', app_id), ('success', success)], verbose=False)


if __name__ == '__main__':

    df = execute_ocr("2012-00230")

    df["median_dist"] = df.distance_to_prev_line.median()
    df["new_entry"] =  (df.distance_to_prev_line) > 1.5*df.distance_to_prev_line.median()

    df.to_excel(f"{data_dir}/debug/df_old.xlsx")

    raise ValueError('aaa')

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