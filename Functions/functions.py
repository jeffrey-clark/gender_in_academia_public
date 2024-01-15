import pandas as pd
import numpy as np
import sys
import math

import Functions.sql_functions as sql
from Models.ResearcherModel import Application
import Resources.filepaths as fps

# progress bar
def progress_bar(i, N):
    if isinstance(N, list):
        index = N.index(i) + 1
        total = len(N)
    elif isinstance(N, int):
        index = i
        total = N

    progress = round(100*index/total)
    no_of_dashes = math.floor(progress/5)
    no_of_blanks = 20 - no_of_dashes

    beg = "|" + "-"*no_of_dashes
    end = " " *no_of_blanks + "| " + str(progress) + "%"

    sys.stdout.write('\r'+beg+end)
    sys.stdout.flush()

    if total == index:
        print("\n")


def load_vr():
    fp = f"{fps.data_dir}/VR-ansökningar 2012-2016.xlsx"
    print(f'Reading: {fp}')
    # read the excel file
    df = pd.read_excel(fp, "data 12-16")
    # remove supurflous columns
    df = df.loc[:, 'Dnr':'Nyckelord']
    # Translate the columns
    index_swe = ['Dnr', 'Ärendeår', 'Efternamn', 'Förnamn', 'Kön', 'Medelsförvaltare', 'Beslut', 'Klustrad bidragsform',
                 'Bidragsform', 'Inriktning', 'Tematisk inriktning/fritt', 'Stödform (fr om 2016)','Beslutsorgan (ÄRK)',
                 'Ämnesområde', 'HuvudBG', 'Alla SCBkoder', 'TotalBevBelopp', 'Antal Beviljade År', 'ProjekttitelSv',
                 'ProjekttitelEn', 'Nyckelord']
    index_eng = ['app_id', 'year', 'surname', 'name', 'gender', 'financier', 'decision', 'clustered_grant_type',
                 'grant_type', 'specialization', 'thematic_specialization', 'support_type (2016)', 'deciding_body (ÄRK)',
                 'field', 'assessing_group', 'all_scb_codes', 'amount_granted', 'years_granted', 'project_title_swe',
                 'project_title_eng', 'keywords']
    df.columns = index_eng
    # insert the ID column
    df = pd.concat([pd.Series(range(0, len(df)), index=df.index, name='id'), df], axis=1)
    return df


def get_applications(app_ids=None, format="obj"):

    if app_ids in ['all', '*', None]:
        df = sql.select('applications')
    else:
        df = sql.select('applications', where=[('app_id', app_ids)])

    if df.empty:
        return False

    # maiden name cleaning
    df['surname_clean'] = df.surname
    df['has_maiden_name'] = df.surname.str.contains("\(", regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r"\(tid[^\s]*\s", "", case=False, regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r"\(prev[^\s]*\s", "", case=False, regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r"\(also ", "", case=False, regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r"\(maiden ", "", case=False, regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r"\(f.*d.* ", "", case=False, regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r"[\(\)]", "", case=False, regex=True)
    df['surname_clean'] = df.surname_clean.str.replace(r" - ", "-", case=False, regex=True)

    if format.lower() in ['obj', 'object', 'model']:
        application_list = df.apply(lambda x: Application(x), axis=1).values

        if len(application_list) == 1:
            return application_list[0]  # type: Application
        else:
            return application_list

    elif format.lower() in ['df']:
        return df

    else:
        raise SyntaxError("Incorrect Format Specification")



if __name__ == "__main__":
    list = get_applications(["2016-05576", "2012-00202"])
    list = get_applications()
    for x in list:   # type: Application
        print(x.name, x.surname)
