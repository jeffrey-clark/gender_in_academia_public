import os, sys, re

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
import Resources.filepaths as fps
#----------------------------------------------------------------------#

import Functions.functions as f
import Functions.sql_functions as sql

# drop the table if it exists
#sql.drop_table('completed_api_files')
#sql.drop_table('clarivate_data')

# create the progress table
cols_tups = [('id', 'INT', 'AUTO_INCREMENT'),
        ('fp', 'TEXT', 120),
        ('time', 'DATETIME', 'DEFAULT CURRENT_TIMESTAMP')]
if not sql.check_table_exists('completed_api_files'):
    sql.create_table('completed_api_files', cols_tups, 'id', 'fp')


folders = ['Bigfiles', 'Discrepancies', 'Hyphens', 'Initial_copies', 'Name_copies']

for folder in folders:
    dir = f"{fps.api_dir}/{folder}"
    files = os.listdir(dir)
    for file in files:
        if file[-5:] != ".xlsx":
            continue

        fp = f"{dir}/{file}"
        # make sure that the fp is a valid file
        if not os.path.isfile(fp):
            continue
        # check if the fp has been reported as completed
        x = sql.count_rows('completed_api_files', where=['fp', fp])
        if x > 0:
            print(f"Skipping: {fp}")
            continue
        # import the df
        print(f'\nReading: {fp}')
        df = pd.read_excel(fp, 'api_output')

        # filter the df to include only relevant columns (skip the percentile and raw data)
        df = df.loc[:, 'app_id':'jnci']

        # convert relevant columns to numerics
        integer_cols = ["pub_year"]
        df[integer_cols] = df[integer_cols].astype(np.int64)

        if "times_cites" in list(df.columns):
            df.rename(columns={"times_cites": "times_cited"}, inplace=True)

        float_cols = ["impact_factor", "journal_expected_citations", "jnci", "is_international_collab", "times_cited",
                      "open_access"]
        df[float_cols] = df[float_cols].astype(np.float64)

        # check that the table exists
        table_exists = sql.check_table_exists('clarivate_data')
        if not table_exists:
            sql.create_table_from_df('clarivate_data', df, [('app_id', 20), ('api_id', 30)])

        # here we do the uploading
        sql.upload_to_table('clarivate_data', df)

        # mark the file as complete
        sql.insert('completed_api_files', [('fp', fp)])



#tup_list = [('fp', 'thiggdds is the test fp')]





# def main():
#     tabl = 'applications'
#     sql.drop_table(tabl)
#     df = f.load_vr()
#     sql.create_table(tabl, df, 'id')
#     sql.upload_to_table(tabl, df)


if __name__ == "__main__":
    pass


# SELECT * FROM `clarivate_data` WHERE `app_id` = "2012-00364"   ORDER BY `app_id` ASC, `api_id` ASC