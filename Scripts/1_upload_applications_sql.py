import os, sys, re

#-------------- NESTED PATH CORRECTION --------------------------------#
# For all script files, we add the parent directory to the system path
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

import Functions.functions as f
import Functions.sql_functions as sql

def main():
    tabl = 'applications'
    sql.drop_table(tabl)
    df = f.load_vr()
    sql.create_table_from_df(tabl, df, 'id')
    sql.upload_to_table(tabl, df)


if __name__ == "__main__":
    main()


