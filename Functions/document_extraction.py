import os
import sys
import re

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

#from Models.initialize import *
import Models.MsWordModel as WM
from Functions.StringComp import *
from Models.DXT_doc_row import *
import Functions.DXT_functions as DXTF
from Functions.j_functions import *
from Functions.functions import *


import codecs
# using Textract
import textract

pd.set_option('display.expand_frame_repr', False)

# -------------------------------------------------------------------------------------
#  The purpose of this code is to effectively parse publication lists in docx format.
# -------------------------------------------------------------------------------------

# EXCEL COLUMNS RAW
cols_raw = ['app_id', 'row_id', 'text', 'style', 'blank', 'section', 'year_heading', 'author_match',
            'author_occ',
            'personnummer', 'year', 'impact_factor', 'split_flag', 'ending_flag', 'ending_deduced',
            'merge_below',
            'skip_row', 'auth_list_loc', 'auth_list_loc_weak', 'asterisk_flag', 'deduce_reason', 'is_appendage']

# EXCEL COLUMNS PUB
cols_pubs = ['app_id', 'pub_id', 'VR_doctype', 'publication', 'year', 'api_match', 'api_id', 'title', 'doctype',
             'journal', 'issue', 'volume', 'pub_date', 'pub_year', 'authors', 'keywords',
             'is_international_collab',
             'times_cited', 'impact_factor', 'journal_expected_citations', 'open_access', 'api_raw']


def extract_docx(identifier, researcher=None):

    if type(identifier) == Application:
        researcher = identifier
        app_id = researcher.app_id
    elif re.match(r"\d{4}-\d{5}", identifier):
        if researcher is None:
            researcher = get_applications(identifier)
        app_id = researcher.app_id
    else:
        raise SyntaxError("Invalid identifier for Document Extraction")

    # let us load the cv with the WordModel
    cv_filepath = f"{fps.docx_dir}/{app_id}.docx"
    #cv_filepath_pdf = f"{root_dir}/Data/pdf_files/{app_id}.pdf"

    # make sure that the filepath exists
    if not os.path.exists(cv_filepath):
        researcher.warnings.append("Missing CV")
        return False

    # process the docx file and extract text as df
    doc = WM.Docx_df(cv_filepath)


    print(doc)
    print(dir(doc))

    raise ValueError("ajaj")

    df = doc.df_rows

    # make modifications to df
    df['app_id'] = app_id
    df['row_id'] = df.index
    df = df.reindex(columns=['app_id', 'row_id', 'text', 'style'])
    #print(df)
    return df


def check_personnummer(string):
    '''
    Returns boolean if a personnummer is present in a given string
    :param string: input string
    :return: Boolean
    '''
    pnummer = re.search(r"(1*9*[3-9]\d[0-1]\d{3}[-]*\d{4})\b", string)
    if pnummer:
        return pnummer.group(1)
    else:
        return False


def extract_personnummer(string):
    '''
    Generate df with all personnummer present in a string
    :param string: input string
    :return: dataframe with all personnummer as start/end indices.
    '''
    q = re.finditer(r"(1*9*[3-9]\d[0-1]\d{3}[-]*\d{4})\b", string, re.IGNORECASE)
    pnum_positions = [(m.start(0), m.end(0), string[m.start(0):m.end(0)]) for m in q]
    df = pd.DataFrame(pnum_positions, columns=['start','end', 'value'])
    return df



def extract_author_occurrence(string, author_surname):

    # 1. Find the indices of all LISTS OF AUTHORS
    # an author list looks something like this "Surname1, Initial1., Surname2, Initial2, and Surname3 Initial3."

    # create a df with start and end index of all commas
    q = re.finditer(r",|\s\w[\.\b]", string, re.IGNORECASE)
    comma_positions = [(m.start(0), m.end(0)) for m in q]
    df = pd.DataFrame(comma_positions, columns=['start', 'end'])

    df['diff'] = df.start - df.shift().end
    group_count = len(df.loc[(df['diff']>30) | (pd.isnull(df['diff'])), :].index)
    df.loc[(df['diff']>30) | (pd.isnull(df['diff'])), 'group_id'] = range(group_count)
    df.group_id = df.group_id.interpolate(method='pad')

    # define commas as in same group is less than 30 characters away from previous comma
    df['same_group'] = (df.start - df.shift().start < 30) | (df.shift(-1).start - df.start < 30)

    # indicator when same_group bool switches
    df['group_break'] = (df.same_group != df.shift().same_group)

    # id for each group
    number_of_groups = len(df.loc[(df.same_group != df.shift().same_group), :].index)
    df.loc[(df.same_group != df.shift().same_group), 'group_id'] = range(number_of_groups)
    df.group_id = df.group_id.interpolate(method='pad')

    # aggregate the group comma-group start and end
    df = pd.DataFrame(df.groupby(['group_id']).agg({'start': ['min'], 'end': ['max']}))
    df.columns = df.columns.droplevel(level=1)

    # extend the start and end to capture entire Author string
    def split_substring(index, position):
        if position == "start":
            start_index = index - 30
            end_index = index
            start_index = max(0, start_index)
        elif position == "end":
            start_index = index
            end_index = index+30
            end_index = min(len(string), end_index)
        else:
            raise SyntaxError("invalid argument passed for position")

        substring = string[start_index:end_index]
        split_list = re.split(r"[\.\d]", substring)
        if position == "start":
            return index - len(split_list[-1])
        else:
            return index + len(split_list[0].strip()) + 1

    if not df.empty:
        df['start'] = df.apply(lambda x: split_substring(x['start'], 'start'), axis=1)
        df['end'] = df.apply(lambda x: split_substring(x['end'], 'end'), axis=1)

        author_list_indices = df.copy()
    else:
        author_list_indices = pd.DataFrame([], columns=["start", "end"], index=["group_id"])
        #print(author_list_indices)

    # 2. Find all occurrences of Author surname in string

    # split the surnames
    split_list = re.split(r'[\s-]', author_surname, 10)
    # drop short surnames
    skip_list = ['de', 'la', 'von', 'van', 'di']
    surname_list = [x for x in split_list if x not in skip_list]

    matches = []
    for snm in surname_list:
        q = re.finditer(re.escape(snm) + r"\b", string, re.IGNORECASE)
        for m in q:
            matches.append((m.start(0), m.end(0), snm))
    df = pd.DataFrame(matches, columns=['match_start', 'match_end', 'surname']).sort_values('match_start')

    # define commas as in same group is less than 30 characters away from previous comma
    df['diff'] = df.match_start - df.shift().match_end
    group_count = len(df.loc[(df['diff']>30) | (pd.isnull(df['diff'])), :].index)
    df.loc[(df['diff']>30) | (pd.isnull(df['diff'])), 'group_id'] = range(group_count)
    df.group_id = df.group_id.interpolate(method='pad')

    # # aggregate the group comma-group start and end
    df = pd.DataFrame(df.groupby(['group_id']).agg({'match_start': ['min'], 'match_end': ['max']}))
    df.columns = df.columns.droplevel(level=1)

    # 3.  Now check if there are author matches outside the detected
    def match_row(row):
        m = author_list_indices.loc[(author_list_indices.start <= row.match_start) & (author_list_indices.end >= row.match_end), :]
        #row.author_list_start = m.start
        if len(m) == 1:
            return m.iloc[0][['start', 'end']]
        else:
            return pd.Series(index=['start', 'end'], dtype='object')


    df[['author_list_start', 'author_list_end']] = df.apply(lambda x: match_row(x), axis=1)

    # Note that if there is a coauthor with the same surname in the autor list, keep only the first author match
    df = df.drop_duplicates(subset=['author_list_start', 'author_list_end'])

    if df.empty:
        author_match = False
        author_occurrences = 0
    else:
        author_match = True
        author_occurrences = len(df)
    #
    # for r_id, r in df.iterrows():
    #     print(string[r.author_list_start:r.author_list_end])
    author_matches = df.copy()

    return author_match, author_occurrences, df


def check_if_subset(mydf, row):
    '''
    Return boolean if a pd series is a subset of a pd dataframe
    :param mydf: input dataframe
    :param row: input row
    :return: True if the row is found in the dataframe
    '''
    check_df = mydf.loc[((mydf['start'] <= row.start) & (mydf['end'] >= row.end)) & ((mydf['start'] != row.start) | (mydf['end'] != row.end)), :]
    return len(check_df) > 0


def extract_all_section_flags(identifier):
    '''

    :param identifier:
    :return:
    '''

    t = type(identifier)
    if t == pd.Series:
        row = identifier
    elif t == str:
        row = pd.Series([identifier, None], ['text', 'style'])
    else:
        raise SyntaxError(f"pd Series or String should be passed as identifier argument. Got {t}")

    criteria = {
        'peer-reviewed': [r'peer.{,5}review', r'referee.{,5}review', r'peer.{,5}refereed',
                          r'bedömda', r'fackgranskade'],
        'publication': [r'publications*', r'articles*', r'papers*', r'original work',
                        r'artiklar', r'publikation'],   # Taken out: r'journals*'
        'conference': [r'conference', r'konferensbidrag'],
        'monographs': [r'monographs*', r'manuscripts*', r'monografi', r'manuskript'],
        'popular_science': [r"popular.{,5}scien", r"populärvetenskap"],
        'patent': [r'patent'],
        'other': [r"other", r"report", r"regional", r"national"],
        'presentation': [r'presentation'],
        'not': [r'\bnon\b', r'\bnot\b', r'\bej\b', r'\bicke\b']
    }

    section_types = {
        'Peer-reviewed publication':
            [   # criterion 1
                criteria['peer-reviewed'],
                # criterion 2
                criteria['publication'] ],
        'Non Peer-reviewed publication':
            [   # criterion 1
                criteria['not'],
                # criterion 2
                criteria['peer-reviewed'],
                # criterion 3
                criteria['publication'] ],
        'Publication':
            [   # criterion 1
                criteria['publication'] ],
        'Peer-reviewed conference contribution':
            [   # criterion 1
                criteria['peer-reviewed'],
                # criterion 2
                criteria['conference'] ],
        'Conference contribution':
            [   # criterion 1
                criteria['conference'] ],
        'Monograph':
            [   # criterion 1
                criteria['monographs'] ],
        'Patent':
            [   # criterion 1
                criteria['patent'] ],
        'Developed software':
            [   # criterion 1
                [r"software", r"programvara"] ],
        'Popular science publication':
            [   # criterion 1
                criteria['popular_science'],
                # criterion 2
                criteria['publication'] ],
        'Popular science presentation':
            [   # criterion 1
                criteria['popular_science'],
                # criterion 2
                criteria['presentation'], ],
        'Other publication':
            [   # criterion 1
                criteria['other'],
                # criterion 2
                criteria['publication'] ],
        'Book/Chapter':
            [   # criterion 1
                [r"books*", r"bok"],
                # criterion 2
                [r'chapters*', r'kapitel', r'letter'] ],
        'Review article':
            [   # criterion 1
                [r"reviews", r"review articles*", r'research review'] ],
        'Computer Program':
            [   #criterion 1
                [r"open access", r"developed"],
                #criterion 2
                [r"computer programs*", r"databases*"] ]
    }

    section_ranking =  {
        'Peer-reviewed publication': 1,
        'Publication': 2,
        'Peer-reviewed conference contribution': 1,
        'Conference contribution': 1,
        'Monograph': 1,
        'Patent': 1,
        'Developed software': 1,
        'Popular science publication': 1,
        'Popular science presentation': 1,
        'Other publication': 1,
        'Book/Chapter': 1,
        'Review article': 1,
        'Computer Program': 1
    }


    section_type_keys = list(section_types.keys())
    section_count = len(section_type_keys)
    #print(section_count)
    section_criteria_counts = [len(section_types[key]) for key in section_type_keys]
    section_total_criteria = dict(zip(section_type_keys, section_criteria_counts))

    # fix for computing the required_criteria_score
    total_criteria_key = {1: 1, 2: 3, 3: 6, 4: 10, 5: 15, 6: 21, 7: 28, 8: 36, 9: 45, 10: 55}

    dfs = []

    for s in section_type_keys:
        section_criteria = section_types[s]
        for index_c, c in enumerate(section_criteria):
            for pattern in c:
                q = re.finditer(r"(" + pattern + r")", row['text'], re.IGNORECASE)
                flag_positions = [(m.start(0), m.end(0), m.group(1), s, (index_c+1)) for m in q]
                dfs.append(pd.DataFrame(flag_positions, columns=['start','end', 'match', 'section', 'criterion']))
    df = pd.concat(dfs).sort_values(['section', 'start'], ascending=[True, True], ignore_index=True)
    # drop duplicate section flags
    df = df.drop_duplicates()

    # add the required criteria count
    df['req_criteria_score'] = df.section.apply(lambda x: total_criteria_key[section_total_criteria[x]])

    # let us restrict the distance between criteria matches 12 characters
    max_char = 12
    df.loc[(df['section'] == df.shift()['section']), 'delta_prev'] = df['start'] - df.shift()['end']
    df.loc[(df['section'] == df.shift(-1)['section']), 'delta_next'] = df.shift(-1)['start'] - df['end']

    index_to_drop = df.loc[(
                             (
                               ((df['delta_prev'] > max_char ) & (df['delta_next'] > max_char )) |
                               ((df['delta_prev'].isnull()) & (df['delta_next'] > max_char )) |
                               ((df['delta_prev'] > max_char ) & (df['delta_next'].isnull()))
                             ) &
                               (df['req_criteria_score'] > 1)
                            ), :].index
    #df.loc[index_to_drop, 'drop'] = 1
    df = df.drop(index_to_drop)

    # compute if all criteria have matched in the match_score_df and merge into df
    match_score_df = df[['section', 'criterion']].drop_duplicates().groupby('section').sum(numeric_only=False).reset_index()
    match_score_df.rename(columns = {'criterion':'criteria_score'}, inplace = True)
    df = df.merge(match_score_df, 'left', 'section')

    if df.empty:
        return pd.DataFrame([], columns=['start', 'end', 'match', 'section'])

    # drop all rows that do not meet the match score requirement
    df = df.loc[df['criteria_score'] == df['req_criteria_score'], :].reset_index()

    # group flags that belong together
    group_count = len(df.loc[((df['delta_prev'].isnull()) | (df['delta_prev'] > max_char))].index)
    df.loc[((df['delta_prev'].isnull()) | (df['delta_prev'] > max_char)), 'flag_group'] = range(1, group_count+1)
    df.flag_group = df.flag_group.interpolate(method='pad')

    # merge the groups into single rows
    #     flag_combos = complete_rows.groupby('row').flag.apply(lambda x:str(list(x))).value_counts().reset_index()
    def combine_strings(x):
        return str(list(x))

    section_df = df.groupby('flag_group').agg({'start': np.min, 'end': np.max, 'match': combine_strings, 'section': 'first'}).reset_index()

    # drop subsets
    subset_bools = section_df.apply(lambda x: check_if_subset(section_df, x), axis=1)
    if not subset_bools.empty:
        section_df = section_df.loc[~subset_bools, :]

    # Rename non-peer-reviewed publications to just publications
    section_df.loc[section_df['section'] == "Non Peer-reviewed publication", "section"] = "Publication"
    section_df = section_df.sort_values('start')

    # finally let us handle subsets
    section_df['end_prev_row'] = section_df.shift().end
    section_df['overlap_flag'] = section_df.end_prev_row > section_df.start
    section_count = len(section_df.loc[~section_df.overlap_flag, :])
    section_df.loc[~section_df.overlap_flag, 'section_group'] = range(1, section_count+1)
    section_df.section_group = section_df.section_group.interpolate(method='pad')

    # let us create rankings
    section_df['ranking'] = section_df.section.apply(lambda x: section_ranking[x])
    section_df = section_df.sort_values(['section_group', 'ranking'], ascending=[True, True])
    section_df = section_df.groupby('section_group').agg({'start': np.min, 'end': np.max, 'match': 'first', 'section': 'first'}).reset_index()

    return section_df




def extract_ending_flags(string):

    ending_flags = [
        r"p{1,2}\.*\s*\d+[\s\-\–\.\d]*",        # Page numbers: p.5-66 , p123
        r"\b\d+\s*p{1,2}\b",                    # Page numbers: 15 pp.
        r"\bno\.*\s*\d{1,4}",                   # Journal number: No .13
        r"[\)\(\d:\s-]{6,}\b",                  # Volume numbers: 12(2) 4:22
        r"vol\w{,4}\s*\.*\d+\b",                # Volume explicit: vol 14
        r"number of citations:*\s*[\d]+\b",     # number of citations 4
        r"\d{,3}\(\d{,3}\)",                    # 1(12)
        r"[\w]{3,8}-[\w]{2,8}"              # EP41B-0699.
    ]

    false_flags = [
        r"[\D]{3,8}-[\D]{2,8}"
    ]

    dfs = []
    # check for flags
    for pattern in ending_flags:
        q = re.finditer(r"(" + pattern + r")", string, re.IGNORECASE)
        flag_positions = [(m.group(1), m.start(0), m.end(0)) for m in q]
        dfs.append(pd.DataFrame(flag_positions, columns=['span', 'start','end']))
    df = pd.concat(dfs).sort_values('start', ascending=True)

    # check for false flags and remove from dfs
    def check_false_flag(string):
        result_bool = False
        for pattern in false_flags:
            if re.search(pattern, string):
                result_bool = True
                break
        return result_bool

    false_flag_inx = df.apply(lambda x:check_false_flag(x.span), axis=1)

    if not false_flag_inx.empty:
        df = df.loc[~false_flag_inx, :]

    # for pattern in false_flags:
    #     q = re.finditer(r"(" + pattern + r")", string, re.IGNORECASE)
    #     flag_positions = [(m.group(1), m.start(0), m.end(0)) for m in q]
    #     dfs.append(pd.DataFrame(flag_positions, columns=['span', 'start','end']))

    if df.empty:
        x = pd.DataFrame({'start': [], 'end': []}, dtype='object')
        return x

    # a) remove subset matches
    subset_bools = df.apply(lambda x: check_if_subset(df, x), axis=1)
    if not subset_bools.empty:
        df = df.loc[~subset_bools, :]
        df = df.drop_duplicates(subset=['start', 'end'])

        # drop any identified endings that,when trimmed are just years
        def filter_years(string):
            pattern = r"[^-]([12]\d{3})[^-]|^([12]\d{3})[^-]|[^-]([12]\d{3})$"
            q = re.search(pattern, str(string).strip())
            if q is None:
                return False
            else:
                year_str_clean = int(re.sub(r"\D", "", q.group(0)))
                return year_str_clean >= 1950 and year_str_clean < 2020

        subset_bools = df.apply(lambda x: filter_years(x), axis=1)
        #df['year_to_drop'] = subset_bools
        df = df.loc[~subset_bools, :]

    # merge identified ending sections to larger ending chunks
    df['diff'] = df.start - df.shift().end
    group_count = len(df.loc[(df['diff']>10) | (pd.isnull(df['diff'])), :].index)
    df.loc[(df['diff']>10) | (pd.isnull(df['diff'])), 'group_id'] = range(group_count)
    df.group_id = df.group_id.interpolate(method='pad')

    # # aggregate the group comma-group start and end
    df = pd.DataFrame(df.groupby(['group_id']).agg({'start': ['min'], 'end': ['max']}))
    df.columns = df.columns.droplevel(level=1)


    def get_value(row):
        return string[int(row["start"]):int(row["end"])]
    if not df.empty:
        # extract the ending flag value
        df["value"] = df.apply(get_value, axis=1)

    return df



def extract_year_flags(string):

    # capture all four-digit combinations without dashes before or after
    pattern = r"[^-\d]([12]\d{3})[^-\d]|^([12]\d{3})[^-\d]|[^-\d]([12]\d{3})$"
    year_positions = []
    q = re.finditer(pattern, string, re.IGNORECASE)  # allow for one letter after the year e.g. 2012a
    for m in q:
        group_list = [x for x in [1, 2, 3] if m.group(x) is not None ]
        if len(group_list) == 0:
            continue
        i_group = group_list[0]
        year_positions.append((m.start(i_group), m.end(i_group), int(m.group(i_group))))

    #year_positions = [(m.start(0), m.end(0), int(m.group(1))) for m in q]
    year_df = pd.DataFrame(year_positions, columns=['start','end', 'year'])

    # drop any years that are unrealistic for the pub list
    year_df = year_df.loc[((year_df['year'] >= 1950) & (year_df['year'] < 2020))]


    # ALSO CONTROL FOR INCORRECTLY TYPES YEARS, BY LOOKING FOR MONTHS
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    patterns = [re.escape(x) + r"\D{,10}(\d{4})" for x in months]
    year_positions = []
    for p in patterns:
        q = re.finditer(p, string, re.IGNORECASE)  # allow for one letter after the year e.g. 2012a
        for m in q:
            year_positions.append((m.start(1), m.end(1), int(m.group(1))))

    explicit_year_from_month_df = pd.DataFrame(year_positions,  columns=['start','end', 'value'])
    year_df = pd.concat([year_df, explicit_year_from_month_df]).drop_duplicates()

    # remove subset year-matches
    #subset_bools = year_df.apply(lambda x: check_if_subset(year_df, x), axis=1)
    if not year_df.empty:
        #year_df = year_df.loc[~subset_bools, :]
        year_df = year_df.drop_duplicates(subset=['start', 'end'])

    def get_value(row):
        return string[int(row["start"]):int(row["end"])]
    # extract the ending flag value
    if not year_df.empty:
        year_df["value"] = year_df.apply(get_value, axis=1)

    return year_df



def find_all_flags(string, surname):

    author_list_df = extract_author_occurrence(string, surname)[2]
    # merger the author match and author_list match into single columns
    author_list_df['start'] = author_list_df.min(axis=1)
    author_list_df['end'] = author_list_df.max(axis=1)
    author_collapsed_df = author_list_df[['start', 'end']].copy()
    author_collapsed_df['flag'] = "author"

    pnum_df = extract_personnummer(string)
    pnum_df['flag'] = 'personnummer'

    year_df = extract_year_flags(string).loc[:, ['start', 'end', "value"]]
    year_df['flag'] = "year"

    ending_df = extract_ending_flags(string)
    ending_df['flag'] = 'ending'


    if string == "":
        blank_df = pd.DataFrame({'start': [np.NaN], 'end': [np.NaN], 'flag': ['blank']})
    else:
        blank_df = pd.DataFrame({'start': [], 'end': [], 'flag': []})

    flag_df = pd.concat([author_collapsed_df, pnum_df, year_df, ending_df, blank_df], ignore_index=True).sort_values('start', ignore_index=True)

    # if we have two consecutive ending flags, with less than 100 characters inbetween, then drop the first one
    flag_df['next_flag'] = flag_df.shift(-1).flag
    flag_df['delta'] = flag_df.shift(-1).start - flag_df.end
    condition = (flag_df['flag'] == "ending") & (flag_df['next_flag'] == "ending") & (flag_df['delta'] < 100)
    endings_to_drop = flag_df.loc[condition, :].index
    flag_df = flag_df.drop(endings_to_drop)

    # if we have a detected year flag, which is part of a personnummer, then drop it
    subset_pnum_years_indx = flag_df.loc[((flag_df['flag'] == 'personnummer') & (flag_df['next_flag'] == 'year') &
                                     (flag_df['delta'] <= 0))].index + 1
    flag_df = flag_df.drop(index=subset_pnum_years_indx)

    # try to deduce groups based off of ending flags
    ending_count = len(flag_df.loc[flag_df['flag'] == "ending", :].index)
    flag_df.loc[flag_df['flag'] == 'ending', 'group'] = range(ending_count)
    flag_df['group'] = flag_df.group.interpolate(method='backfill')

    return flag_df


def get_row_content(df, row_id, start=None, end=None):
    row = df.iloc[row_id]
    text = row.text
    if start in [None, np.NaN]:
        start = 0
    if end in [None, np.NaN]:
        end = len(text)
    substring = text[int(start):int(end)]
    return substring


def create_flag_df(df, researcher):

    ### GET ALL FLAGS (EXCEPT SECTION) #######
    def find_all_flags_mapped(string, reseracher_surname, row_id):
        mydf = find_all_flags(string, reseracher_surname)
        mydf['row'] = row_id
        return mydf.loc[:, ["row", "start", "end", "flag"]]

    print("DF IS")
    print(df)

    flag_df = pd.concat(df.apply(lambda x: find_all_flags_mapped(x.text, researcher.surname_clean, x.row_id), axis=1).values, ignore_index=True)

    # make the flag_df pretty
    #flag_df = flag_df.loc[:, ["row", "start", "end", "flag"]]

    flag_df['content'] = flag_df.apply(lambda x: get_row_content(df, x.row, x.start, x.end), axis=1)

    ### GET SECTION FLAGS  #######
    def find_all_section_flags_mapped(string, row_id):
        mydf = extract_all_section_flags(string).rename(columns = {'section':'content'})
        mydf['row'] = row_id
        mydf['flag'] = "section"
        return mydf.loc[:, ["row", "start", "end", "flag", "content"]]

    section_flags = pd.concat(df.apply(lambda x: find_all_section_flags_mapped(x.text, x.row_id), axis=1).values, ignore_index=True)

    ### APPEND TO ALL FLAG DATAFRAME #######

    flag_df = pd.concat([flag_df, section_flags]).sort_values(['row', 'start', 'end'])

    # HERE WE MAKE DEDUCTION #################################################

    # 1 check for perfect entries between blanks
    #blank_indices = flag_df.loc[flag_df.flag == "blank", :].index
    #print(blank_indices)

    # save the row_count of the flag_df
    last_row_id = flag_df.iloc[-1].row

    def identify_complete_rows(sub_df):

        val_counts = sub_df.value_counts('flag')

        for att in ['author', 'ending', 'year']:
            if att not in val_counts.index:
                val_counts[att] = np.NaN

        row_id = sub_df.row.iloc[0]

        if row_id == 0:
            prev_flags = pd.DataFrame({'start': [np.NaN], 'end': [np.NaN], 'flag': ["blank"]})
            #prev_flags = pd.DataFrame(columns=['start', 'end', 'flag'])
        else:
            prev_flags = flag_df.loc[flag_df['row'] <= (row_id-1), :]

        if row_id == last_row_id:
            next_flags = pd.DataFrame({'start': [np.NaN], 'end': [np.NaN], 'flag': ["blank"]})
            #next_flags = pd.DataFrame(columns=['start', 'end', 'flag'])
        else:
            next_flags = flag_df.loc[flag_df['row'] >= (row_id+1), :]


        if val_counts.author == 1 and val_counts.ending == 1 and val_counts.year == 1:
            #if this complete row is surrounded by blanks

            if prev_flags.iloc[-1].flag == "blank" and next_flags.iloc[0].flag == 'blank':
                return "Perfect"

            return "Complete"

        # elif val_counts.author >= 1 and val_counts.ending >= 1 and val_counts.year >= 1:
        #     print(sub_df)
        #
        #     pass


    complete_col = flag_df.groupby('row').apply(lambda x: identify_complete_rows(x)).reset_index().rename({0: "status"}, axis=1)

    if not complete_col.empty:
        flag_df = flag_df.merge(complete_col, on='row')
    else:
        flag_df['status'] = None

    # at this point all completed groups belong to the same extracted row. Thus we create a group variable, assigning the row_id
    flag_df['group'] = flag_df['row']
    flag_df.loc[flag_df['status'].isnull(),'group'] = np.NaN

    # from the perfect rows, can we deduce the most common order? e.g. AUTHOR - YEAR - ENDING
    complete_rows = flag_df.loc[~flag_df['status'].isnull(), :]
    if not complete_rows.empty:
        flag_combos = complete_rows.groupby('row').flag.apply(lambda x:str(list(x))).value_counts().reset_index()
        entry_order = flag_combos.iloc[0].values[0]
        entry_order = json.loads(re.sub(r"\'", "\"", entry_order))
    else:
        entry_order = ['author', 'year', 'ending']
    # print(entry_order)

    # remove excessive blanks
    extra_blanks = ((flag_df['flag'] == "blank") &          # the row is blank
         (flag_df['flag'].shift() == "blank"))              # the row above is blank

    flag_df = flag_df.loc[~extra_blanks, :].reset_index()


    ## Now look for contradictions

    # identify the index of the next row with same flag
    def find_next_flag_of_type(index):
        row_flag = flag_df.iloc[index].flag
        subset = flag_df.iloc[(index+1):].copy()
        subset_flag_rows = subset.loc[subset['flag'] == row_flag, :]
        if not subset_flag_rows.empty:
            return subset_flag_rows.index.values[0]
        else:
            return None
    flag_df['next_flag'] = pd.Series(flag_df.index).apply(lambda x: find_next_flag_of_type(x))

    # deduce down to the next flag, starting with the most common type

    flag_df['index'] = flag_df.index.values

    def apply_deduction(row):
        # set a group id
        # flag_df.loc[(
        #                     (flag_df['index'] >= row['index'])& (flag_df['index'] < row['next_flag']) &  # the rows should be before next flag of same type
        #                     (flag_df['group'].isnull()) &  # the deduced flags should not belong to existing group
        #                     (flag_df.shift(-1)['index'] == (flag_df['index'] + 1))  # all flag rows should be consecutive
        #              ), ['status', 'group']] = ['deduced', row['row']]
        rows_to_update_indx = flag_df.loc[(
            (flag_df['index'] >= row['index'])& (flag_df['index'] < row['next_flag']) # the rows should be before next flag of same type
            )].index

        # create a sub-df with all flags up until the next flag (what flag is inserted into this function depends on the entry order)
        subdf = flag_df.loc[rows_to_update_indx].copy()
        # filter away already deduced group
        count_not_null = len(subdf.loc[~subdf['group'].isnull()].index)
        subdf.loc[~subdf['group'].isnull(), 'not_null_dummy'] = range(1, count_not_null+1)
        subdf.not_null_dummy = subdf.not_null_dummy.interpolate(method='pad')
        rows_to_update = subdf.loc[subdf['not_null_dummy'].isnull()]

        # exclude blank rows from the group (if there is an ending, blank and then ending).
        exclude_blank = rows_to_update.loc[ ((rows_to_update["flag"] == "blank") & (rows_to_update.shift(1)["flag"] == "ending") & (rows_to_update.shift(-1)["flag"] == "ending"))]
        if not exclude_blank.empty:
            rows_to_update = rows_to_update.loc[rows_to_update.index < exclude_blank.index.min()]

        rows_to_update_indx = rows_to_update.index

        # a deduced group should at most have 5 flags, otherwise, something is wrong
        if len(rows_to_update_indx) <= 5:
            flag_df.loc[rows_to_update_indx, ['status', 'group']] = ['deduced', row['row']]

    # HERE WE DO THE DEDUCTION
    for origin_deduce in entry_order:   # e.g. ['author', 'year', 'ending']
        deduce_start = flag_df.loc[((flag_df['flag']==origin_deduce) & (flag_df['status'].isnull())), :]
        deduce_start.apply(lambda x: apply_deduction(x), axis=1)


    # cleanup...
    # remove the index column
    flag_df.drop('index', axis=1, inplace=True)
    # remove any blanks from group that are at end of group
    flag_df.loc[((flag_df['flag'] == "blank") & (flag_df['group'] != flag_df.shift(-1)['group'])), ['status', 'group']] = [None, np.NaN]


    # Break sections apart from groups  (but not if e.g. "peer-reviewed" is part of a publication entry)
    flag_df.loc[(
                        (flag_df['flag'] == "section") &                # the flag is a section
                        (flag_df['start'] < 10) &                       # the match starts early on the row
                        (
                            (flag_df['row'] != flag_df.shift(1)['row']) |   # the section flag is not in the middle of a row/entry  Note shift(1) means row above
                            (
                                (flag_df['row'] == flag_df.shift(1)['row']) &
                                (flag_df.shift(-1)['flag'] == 'section')
                            )
                        )
                ), ['status', 'group']] = ["section", np.NaN]

    # ungroup blanks before or after identified sections
    flag_df.loc[(
                        (flag_df['flag'] == "blank") &
                        ((flag_df.shift()['flag'] == "section") | (flag_df.shift(-1)['flag'] == "section"))
                ), ['status', 'group']] = [None, np.NaN]


    # compute each flags strlen
    flag_df["flag_strlen"] = flag_df["content"].str.len()
    return flag_df



def create_pub_df(xdf, fdf):

    def get_row_section(i):
        rows_above = fdf.iloc[:i]
        previous_sections = rows_above.loc[rows_above['status'] == 'section', 'content'].values
        if len(previous_sections) > 0:
            most_recent_section = previous_sections[-1]
        else:
            most_recent_section = "unknown"
        return most_recent_section

    fdf['most_recent_section'] = pd.Series(fdf.index.values).apply(lambda x: get_row_section(x))

    print(fdf)   # note the last row is not deduced

    df = fdf.groupby('group').agg({'row': ['first', 'last'], 'start': 'first', 'end': 'last', 'most_recent_section': 'first', "flag_strlen": "sum"})

    if df.empty:
        return pd.DataFrame(index=['start_row', 'start_i', 'end_row', 'end_i', 'pub_type', "group_strlen"])


    df.columns = pd.Index(['start_row', 'end_row', 'start_i', 'end_i', 'pub_type', "group_strlen"])
    df = df[['start_row', 'start_i', 'end_row', 'end_i', 'pub_type', "group_strlen"]]

    # compute the length of the row with entry ending
    def compute_row_length(row_id):
        return len(xdf.iloc[row_id]['text'])   # perhaps change this function so that it takes length of all merged rows

    df['end_length'] = df['end_row'].apply(compute_row_length)

    # get the content
    def map_content(row):
        text_bits = []
        for row_id in range(row.start_row, row.end_row + 1):
            row_text = xdf.iloc[row_id]["text"]
            # if we are in the first row, start from start index
            if row_id == row.start_row:
                text_bits.append(row_text[int(row.start_i):])
            elif row_id == row.end_row:
                text_bits.append(row_text[:int(row.end_i)])
            else:
                text_bits.append(row_text)
        return "".join(text_bits).strip()

    print("We are about to apply")
    print(df)

    df["content"] = df.apply(map_content, axis=1)



    # filter the df, remove all pubs with less than 40 characters. These will be false positives
    df = df.loc[(df["content"].str.len() > 40)]

    # filter away rows where the end_length - group_strlen < 40
    df.reset_index(drop=True, inplace=True)
    df = df.loc[~(((df["end_length"] - df["group_strlen"] < 40)) & (df.index.values == 0))]

    df["pub_id"] = df.reset_index(drop=True).index.values +1


    return df



if __name__ == "__main__":
    app_id = "2013-05573"
    app_id = "2015-05611"

    app_id = "2016-00981" 
    app_id = "2012-06630"


    r = get_applications(app_id)
    xdf = extract_docx(app_id, r)
    fdf = create_flag_df(xdf, r)

    pub_df = create_pub_df(xdf, fdf)

    # output two dfs to one excel

    with pd.ExcelWriter(f"{fps.extractions_dir}/{app_id}.xlsx") as writer:
        xdf.to_excel(writer, sheet_name="Raw", index=False)
        fdf.to_excel(writer, sheet_name="Flags", index=False)
        pub_df.to_excel(writer, sheet_name="Pubs", index=False)

