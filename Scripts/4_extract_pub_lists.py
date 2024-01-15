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
import Scripts.docxtractor_old as dxtr
from Functions.StringComp import StringComp

# get all of the researchers
df = sql.select('applications')

# for row_id, researcher in df.iterrows():
#     if row_id < 21782:
#         continue
#     x = dxtr.extract(researcher, save=True)
#     print(row_id)



# Create SQL table if it is missing
# create the progress table
cols_tups = [('app_id', 'TEXT'), ('pub_id', 'INT'), ('vr_doctype', 'TEXT'), ('vr_publication', 'TEXT'),
             ('vr_year', 'TEXT'), ('api_match', 'INT'), ('api_id', 'TEXT')]
if not sql.check_table_exists('publications'):
    sql.create_table('publications', cols_tups, [('app_id', 12), 'pub_id'])



for row_id, researcher in df.iterrows():
    print(researcher.name)
    changes_made_excel = False

    vr_pubs = dxtr.extract(researcher, save=True)
    clarivate_pubs = sql.select('clarivate_data', where=[('app_id', researcher.app_id)])


    # declare the matching function to be mapped in matching process
    def match(cl_string, vr_string):
        x = StringComp(cl_string, vr_string, 2, 0)
        # print(x.print_analysis())
        return x


    for pub_id, pub in vr_pubs.iterrows():

        # check if the SQL row has already been inserted
        check = sql.select('publications', where=[('app_id', pub.app_id), ('pub_id', pub.pub_id)])
        if not check.empty:
            top_match = sql.select('clarivate_data', where=[('app_id', researcher.app_id), ('api_id', check.api_id.values[0])])

        else:
            changes_made_excel = True
            # match against all clarivate pubs
            x = clarivate_pubs.title.apply(lambda x: match(x, pub.VR_publication))
            clarivate_pubs['word_match'] = x.apply(lambda x: x.max_percent_words_matched)
            clarivate_pubs['letter_match'] = x.apply(lambda x: x.max_percent_letters_matched)
            clarivate_pubs = clarivate_pubs.sort_values(['word_match', 'letter_match'], ascending=[False, False])

            top_match = clarivate_pubs.iloc[0]
            print(top_match)
            if top_match.word_match >= 90 and top_match.letter_match >= 90:
                pub['api_match'] = 1
                atts = ['api_id', 'title', 'doctype', 'journal', 'issue', 'volume', 'pub_date',
                           'pub_year', 'authors', 'keywords', 'is_international_collab', 'times_cited', 'impact_factor',
                           'journal_expected_citations', 'open_access']
                for a in atts:
                    pub[a] = top_match[a]

            else:
                pub['api_match'] = 0
                pub['api_id'] = None

            # update the original vr publication list
            vr_pubs.iloc[pub_id] = pub

            # upload to SQL
            sql.insert('publications', [('app_id', pub.app_id), ('pub_id', pub.pub_id), ('vr_doctype', pub.VR_doctype),
                                        ('vr_publication', pub.VR_publication), ('vr_year', pub.VR_year),
                                        ('api_match', pub.api_match), ('api_id', pub.api_id)], verbose=True)

        #break
        print(f"completed {pub.pub_id}")


    # overwrite the vr publicate sheet in workbook
    def overwrite_excel_sheet(filename, sheetname, dataframe):
        options = {}
        options['strings_to_formulas'] = False
        options['strings_to_urls'] = False
        with pd.ExcelWriter(filename, engine='openpyxl', mode='a', options=options) as writer:
            workBook = writer.book
            try:
                workBook.remove(workBook[sheetname])
            except:
                print("Worksheet does not exist")
            finally:
                dataframe.to_excel(writer, sheet_name=sheetname, index=False)
                writer.save()
                workBook.close()

    if changes_made_excel:
        fp = f"{root_dir}/Data/docx_extractions/{researcher.app_id}.xlsx"
        overwrite_excel_sheet(fp, 'Publications', vr_pubs)






