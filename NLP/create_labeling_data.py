import os

import pandas as pd

import Models.MsWordModel as WM
from Functions.j_functions import *
from Functions.functions import *
import random

data_dir = "D:/LocalStore/Gender_in_Academia"

def extract_docx(app_id, output_format="string"):
    # let us load the cv with the WordModel
    cv_filepath = f"{data_dir}/docx_files/{app_id}.docx"
    cv_filepath_pdf = f"{data_dir}/pdf_files/{app_id}.pdf"

    # make sure that the filepath exists
    if not os.path.exists(cv_filepath):
        # researcher.warnings.append("Missing CV")
        # return researcher
        return None

    # process the docx file and extract text as df
    doc = WM.Docx_df(cv_filepath)
    df = doc.df_rows

    # make modifications to df
    df['app_id'] = app_id
    df['row_id'] = df.index

    # calculate the text length
    df['text_length'] = df.text.str.len()
    df = df.reindex(columns=['app_id', 'row_id', 'text', 'style', 'text_length'])
    #print(df)

    if output_format == "df":
        return df
    elif output_format == "string":
        # join all strings
        s = " ".join(df.text.to_list())
        return s

    else:
        raise SyntaxError("Invalid output format")


def export_sample():

    all_app_ids = get_applications(format="df").app_id.to_list()
    random.seed(23500)
    app_ids = random.sample(all_app_ids, 20)
    print(app_ids)

    for index, a in enumerate(app_ids, 1):
        # Create the desired data structure
        data = {
            "data":  {
                "app_id": a,
                "text": extract_docx(a)
            }
        }

        # Define the filename based on the index (or any other naming scheme you prefer)
        filename = f"{data_dir}/training/files for labeling/{a}.json"

        # Write the data to a JSON file
        with open(filename, 'w') as file:
            json.dump(data, file, ensure_ascii=False)

def format_train_data():
    fp = f"{data_dir}/training/label_data.json"
    with open(fp, 'r') as file:
        data = json.load(file)

    train_data = []
    for dic in data:
        print([x[0] for x in dic.items()])
        text = dic['data']['text']
        annotations = dic["annotations"][0]['result']
        ents = []
        for a in annotations:
            #print(a)
            start = a["value"]["start"]
            end = a["value"]["end"]
            lab = a["value"]["labels"][0]
            ents.append((start, end, lab))

        train_data.append((text, {'entities': ents}))

        # Write the data to a JSON file
        fp = f"{data_dir}/training/train_data.json"
        with open(fp, 'w') as file:
            json.dump(train_data, file, ensure_ascii=False)




if __name__ == "__main__":
    export_sample()
    #format_train_data()
    pass