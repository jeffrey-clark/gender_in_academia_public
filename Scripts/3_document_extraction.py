import os, sys, re
import json

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

import Functions.sql_functions as sql

import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
import json

from Functions.functions import *

from typing import List, Optional

from multiprocessing import Pool
from tqdm import tqdm

from functools import partial


#### SET PARAMETERS ####
data_dir = "D:/LocalStore/Gender_in_Academia"
pd.set_option('display.width', 1000)  # Width of output in characters
pd.set_option('display.max_columns', None)




def get_ocr_ext(app_id, clean=False, nlp=True):
    #df = sql.select("ocr_extractions_w_distance", ["*"], [('app_id', app_id)])
    df = sql.select("ocr_extractions_final", ["*"], [('app_id', app_id)])

    if df.empty:
        return False

    if clean:
        # remove all rows with less than 5 characters text (but not blank)
        df = df.loc[((df.text.str.len() > 5) | (df.text.str.len() == 0)), :]
        # # Finding blank (either NaN or "") rows
        # blank_rows = df["text"].replace("", np.nan).isna()
        # # Finding consecutive blank rows
        # double_blank_rows = blank_rows & blank_rows.shift(-1, fill_value=False)
        #
        # # Keeping only non-double blank rows
        # df = df.loc[~double_blank_rows]
    if nlp:

        # temporary fix
        nlp_flags = sql.select("nlp_final", ["*"], [('app_id', app_id)])
        df = df.merge(nlp_flags, on=["app_id", "row_id"], how="left")

    return df


def reorder_col_after(df, col_to_move, after_col):
    # Find the index of the column after which you want to place 'col_to_move'
    loc = df.columns.get_loc(after_col) + 1
    # Split the original columns at the position where 'col_to_move' is supposed to appear
    first_part = df.columns.tolist()[:loc]
    second_part = df.columns.tolist()[loc:]
    # Remove the 'col_to_move' if it is in the second_part
    second_part = [col for col in second_part if col != col_to_move]
    # Combine the parts and insert the 'col_to_move' into the correct position
    new_columns = first_part + [col_to_move] + second_part
    # Reorder the columns using the new order
    return df[new_columns]


def compute_distance(df):
    # Calculate distance
    df["dist"] = df.top - df.bottom.shift()

    # Determine the first row_id for each page
    first_row_ids = df.groupby('page')['row_id'].transform('min')

    # Set 'dist' to NaN where the row_id is the first row_id of the respective page
    df.loc[df['row_id'] == first_row_ids, 'dist'] = np.nan

    return df

def identify_new_entries(df):
    # what is the median font-weight
    #print(df.font_weight.median())
    #df["bold"] = df.font_weight > 1.5*df.font_weight.median()

    # --------  INDENTIFY BY ROW DISTANCE  -------------

    # Determine the first row_id for each page
    first_row_ids = df.groupby('page')['row_id'].transform('min')
    # Set 'dist' to NaN where the row_id is the first row_id of the respective page
    df.loc[df['row_id'] == first_row_ids, 'dist'] = np.nan

    # Calculate median distance for each page and map it back to the original dataframe
    df["median_dist"] = df.groupby('page')['dist'].transform(lambda x: np.ceil(x.median() * 100) / 100)

    def percentile_10th(series):
        positive_values = series[series > 0]
        if not positive_values.empty:
            return np.percentile(positive_values, 10)
        else:
            return np.nan  # or another appropriate value

    df["p10_dist"] = df.groupby('page')['dist'].transform(lambda x: np.ceil(percentile_10th(x) * 100) / 100)

    # Create new_entry column
    # A row is marked as a new entry if dist is NaN or dist is greater than 2 times the median_dist or dist is less than -100
    df["new_entry"] = np.where(
        df["dist"].isna() | (df["dist"] > 2 * df["p10_dist"]) | (df["dist"] < -100),
        True,
        False
    )


    # --------  INDENTIFY BY OUTDENT  -------------

    df.loc[df["outdent"], "new_entry"] = True


    #df = reorder_col_after(df, "new_entry", "text")
    return df


# def flag_repeated_headers_footers(df, header_rows=3, footer_rows=3, threshold=90):
#     # Initialize columns for header and footer flags
#     df['rep_header'] = False
#     df['rep_footer'] = False
#
#     # Extract potential header and footer texts from each page
#     header_texts = []
#     footer_texts = []
#     for page, group in df.groupby('page'):
#         header_candidates = group.iloc[:header_rows]['text'].tolist()
#         footer_candidates = group.iloc[-footer_rows:]['text'].tolist()
#         header_texts.extend(header_candidates)
#         footer_texts.extend(footer_candidates)
#
#     print(header_texts)
#     # Function to compare texts and flag if similar
#     def compare_and_flag(texts_to_compare, all_texts, column_name):
#         for text in texts_to_compare:
#             # Check against all texts (either headers or footers)
#             for compare_text in all_texts:
#                 if text != compare_text:  # Avoid comparing the text with itself
#                     ratio = fuzz.ratio(text, compare_text)
#                     if ratio > 80 or "Mikael" in compare_text:
#                         print(f"""
#                         comparing:
#                             (1) {text}
#                             (2) {compare_text}
#                             Score: {ratio}
#
#                         """)
#                     if ratio > threshold:
#                         # Flag this text as either a header or footer
#                         df.loc[df['text'] == text, column_name] = True
#                         break  # Break if one similar text is found, no need to check others
#
#     # Compare headers and footers
#     for page, group in df.groupby('page'):
#         # Flag headers
#         header_candidates = group.iloc[:header_rows]['text'].tolist()
#         compare_and_flag(header_candidates, header_texts, 'rep_header')
#
#         # Flag footers
#         footer_candidates = group.iloc[-footer_rows:]['text'].tolist()
#         compare_and_flag(footer_candidates, footer_texts, 'rep_footer')
#
#     return df

def flag_repeated_headers_footers(df, header_rows=3, footer_rows=3, threshold=90):
    # Initialize columns for header and footer flags
    df['rep_header'] = False
    df['rep_footer'] = False

    # Extract potential header and footer texts from each page, with their original indices
    header_texts = []
    footer_texts = []
    for page, group in df.groupby('page'):
        header_candidates = group.iloc[:header_rows][['text', 'row_id']].to_dict('records')
        footer_candidates = group.iloc[-footer_rows:][['text', 'row_id']].to_dict('records')
        header_texts.extend(header_candidates)
        footer_texts.extend(footer_candidates)

    # Function to compare texts and flag if similar
    def compare_and_flag(texts_to_compare, all_texts, column_name):
        for candidate in texts_to_compare:
            candidate_text = candidate['text']
            candidate_index = candidate['row_id']
            for compare_candidate in all_texts:
                compare_text = compare_candidate['text']
                compare_index = compare_candidate['row_id']
                if candidate_index != compare_index:  # Avoid comparing the text with its own instance
                    ratio = fuzz.ratio(candidate_text, compare_text)
                    if ratio > threshold:
                        # Flag this text as either a header or footer
                        df.loc[df['row_id'] == candidate_index, column_name] = True
                        break  # Break if one similar text is found, no need to check others

    # Compare headers and footers
    for page, group in df.groupby('page'):
        # Flag headers
        header_candidates = group.iloc[:header_rows][['text', 'row_id']].to_dict('records')
        compare_and_flag(header_candidates, header_texts, 'rep_header')

        # Flag footers
        footer_candidates = group.iloc[-footer_rows:][['text', 'row_id']].to_dict('records')
        compare_and_flag(footer_candidates, footer_texts, 'rep_footer')

    return df

def flag_invalid_rows(df: pd.DataFrame, invalid_columns: list) -> pd.DataFrame:
    # Check if the required columns exist
    for col in invalid_columns:
        if col not in df.columns:
            raise ValueError(f"DataFrame must contain '{col}' column.")

    # Add 'invalid' column, set to True where any specified columns are True
    df['invalid'] = df[invalid_columns].any(axis=1)

    return df

def drop_invalid_rows(df: pd.DataFrame, invalid_columns: list) -> pd.DataFrame:
    # Drops rows where 'invalid' column is True
    if 'invalid' not in df.columns:
        raise ValueError("DataFrame must contain 'invalid' column.")

    # Keep only the rows where 'invalid' is not True
    df_cleaned = df[~df['invalid']].copy()

    # Now, drop the specified columns
    columns_to_drop = ['invalid'] + invalid_columns
    for col in columns_to_drop:
        if col in df_cleaned.columns:
            df_cleaned.drop(col, axis=1, inplace=True)
        else:
            raise ValueError(f"Column '{col}' does not exist in DataFrame.")

    return df_cleaned


# Check indent

def check_indent(df, direction, threshold=100):
    """
    Check the indent/outdent based on the 'left' column of the DataFrame.

    :param df: DataFrame to process.
    :param direction: "in" or "out" to set the comparison direction.
    :param threshold: The threshold value for comparison.
    :return: A boolean Series indicating True where the condition is met.
    """
    if direction not in ['in', 'out']:
        raise ValueError("direction must be 'in' or 'out'")

    # Shift the 'left' column to compare with the previous row
    prev_left = df['left'].shift(1)

    if direction == 'in':
        return (df['left'] > prev_left + threshold)
    else:  # direction == 'out'
        return (df['left'] < prev_left - threshold)


# Find all section flags
def check_if_subset(mydf, row):
    check_df = mydf.loc[((mydf['start'] <= row.start) & (mydf['end'] >= row.end)) & (
            (mydf['start'] != row.start) | (mydf['end'] != row.end)), :]
    return len(check_df) > 0

def normalize_string(s):
    """Remove spaces and dashes from the string."""
    return re.sub(r"[\s-]", "", s)

def create_index_mapping(text):
    mapping = []
    index_in_original = 0
    for char in text:
        if char != ' ':
            mapping.append(index_in_original)
        index_in_original += 1
    return mapping

def adjust_span(original_text, span, mapping):
    start, end = span
    return mapping[start], mapping[end - 1] + 1

def find_section_flags(text, verbose=False):
    flags = []

    criteria = {
        'peer-reviewed': {
            "type": "regex",
            "patterns": ['peer-reviewed', "referee-reviewed", 'referee', 'peer', 'bedömda', 'fackgranskade', 'journal']
            },
        'publication': {
            "type": "regex",
            "patterns": ['publication', 'article', 'paper', 'original work', 'artiklar', 'publikation']
        },
        'conference': {
            "type": "regex",
            "patterns": ['conference', 'konferensbidrag']
        },
        'monographs': {
            "type": "regex",
            "patterns": ['monograph', 'manuscript', 'monograf', 'manuskript', 'thesis', 'theses']
        },
        'popular_science': {
            "type": "regex",
            "patterns": ["popular science", r"populärvetenskap"]
        },
        'patent': {
            "type": "regex",
            "patterns": ['patent']
        },
        'other': {
            "type": "regex",
            "patterns": ["other", "report", "regional", "national"]
        },
        'presentation': {
            "type": "regex",
            "patterns": [r'presentation']
        },
        'book': {
            "type": "regex",
            "patterns": ["book", "bok", "böcker"]
        },
        'chapter': {
            "type": "regex",
            "patterns": ["chapter", "kapitel", "letter"]
        },
        'review_article': {
            "type": "regex",
            "patterns": ["review article", 'research review']
        },
        'open_access': {
            "type": "regex",
            "patterns": ["open access", "developed", "utvecklad", "allmän"]
        },
        'computer': {
            "type": "regex",
            "patterns": ["computer",  "dator", "programs", "database", "software", "programvara"]
        },
        'not': {
            "type": "regex",
            "patterns":[r'non\b', r'\bnot\b', r'\bej\b', r'\bicke\b']
        }
    }


    section_types = {
        'Peer-reviewed publication':
            [  # criterion 1
                criteria['peer-reviewed'],
                # criterion 2
                criteria['publication']],
        'Non Peer-reviewed publication':
            [  # criterion 1
                criteria['not'],
                # criterion 2
                criteria['peer-reviewed'],
                # criterion 3
                criteria['publication']],
        'Publication':
            [  # criterion 1
                criteria['publication']],
        'Peer-reviewed conference contribution':
            [  # criterion 1
                criteria['peer-reviewed'],
                # criterion 2
                criteria['conference']],
        'Conference contribution':
            [  # criterion 1
                criteria['conference']],
        'Presentation':
            [  # criterion 1
                criteria[r'presentation']],
        'Monograph':
            [  # criterion 1
                criteria['monographs']],
        'Patent':
            [  # criterion 1
                criteria['patent']],
        'Popular science publication':
            [  # criterion 1
                criteria['popular_science'],
                # criterion 2
                criteria['publication']],
        'Popular science presentation':
            [  # criterion 1
                criteria['popular_science'],
                # criterion 2
                criteria['presentation'], ],
        'Other publication':
            [  # criterion 1
                criteria['other'],
                # criterion 2
                criteria['publication']],
        'Book/Chapter':
            [  # criterion 1
                criteria['book'],
                # criterion 2
                criteria['chapter']
            ],
        'Review article':
            [  # criterion 1
                criteria["review_article"]
            ],
        'Computer Program':
            [  #criterion 1
                criteria["open_access"],
                #criterion 2
                criteria["computer"]
            ]
    }


    section_ranking = {
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
        'Book_WEAK': 2,
        'Review article': 1,
        'Computer Program': 1,
        'Presentation': 2
    }

    # list of all section types
    section_type_keys = list(section_types.keys())
    # store number of criteria for each section type
    section_criteria_counts = [len(section_types[key]) for key in section_type_keys]
    section_total_criteria = dict(zip(section_type_keys, section_criteria_counts))

    # fix for computing the required_criteria_score
    total_criteria_key = {1: 1, 2: 3, 3: 6, 4: 10, 5: 15, 6: 21, 7: 28, 8: 36, 9: 45, 10: 55}

    dfs = []

    for s in section_type_keys:   # loop through the different section types
        section_criteria = section_types[s]
        for index_c, c in enumerate(section_criteria):  # loop through each criteria group
            typ = c["type"]
            for pattern in c["patterns"]: # loop through each criterion pattern
                # if typ == "fuzzy":
                #     flag_positions = fuzzy_match(text, pattern)
                if typ == "regex":
                    # Compile the regex
                    pattern_compiled = re.compile(r"(" + pattern + r")", re.IGNORECASE)
                    q = re.finditer(pattern_compiled, normalize_string(text))
                    mapping = create_index_mapping(text)
                    # Adjust the span using the mapping
                    flag_positions = []
                    for m in q:
                        start, end = m.span(0)
                        adjusted_start, adjusted_end = mapping[start], mapping[end - 1] + 1
                        flag_positions.append((adjusted_start, adjusted_end, m.group(1), s, (index_c + 1)))

                dfs.append(pd.DataFrame(flag_positions, columns=['start', 'end', 'match', 'section', 'criterion']))
    df = pd.concat(dfs).sort_values(['section', 'start'], ascending=[True, True], ignore_index=True)
    # drop duplicate section flags
    df = df.drop_duplicates()

    # If empty df, means that no criteria are found
    if df.empty:
        return flags

    # add the required criteria count
    df['req_criteria_score'] = df.section.apply(lambda x: total_criteria_key[section_total_criteria[x]])

    # let us restrict the distance between criteria matches 12 characters
    max_char = 20
    df.loc[(df['section'] == df.shift()['section']), 'delta_prev'] = df['start'] - df.shift()['end']
    df.loc[(df['section'] == df.shift(-1)['section']), 'delta_next'] = df.shift(-1)['start'] - df['end']

    index_to_drop = df.loc[(
                                   (
                                           ((df['delta_prev'] > max_char) & (df['delta_next'] > max_char)) |
                                           ((df['delta_prev'].isnull()) & (df['delta_next'] > max_char)) |
                                           ((df['delta_prev'] > max_char) & (df['delta_next'].isnull()))
                                   ) &
                                   (df['req_criteria_score'] > 1)
                           ), :].index
    #df.loc[index_to_drop, 'drop'] = 1
    df = df.drop(index_to_drop)

    # Identify groups
    df["group"] = ((df["delta_prev"] > max_char) | pd.isnull(df["delta_prev"])).cumsum()

    # For each group, compute if all criteria have matched in the match_score_df and merge into df
    match_score_df = df[['group', 'section', 'criterion']].drop_duplicates().groupby(
        ['group', 'section']).criterion.sum().reset_index()
    match_score_df.rename(columns={'criterion': 'criteria_score'}, inplace=True)
    df = df.merge(match_score_df, 'left', ['group', 'section']).sort_values("section")

    # If empty df, means that no criteria are found
    if df.empty:
        return flags

    # drop all rows that do not meet the match score requirement
    df = df.loc[df['criteria_score'] == df['req_criteria_score'], :].reset_index()

    # deal with the weak book criteria
    if len(text) <= 20:
        df.loc[(df['section'] == "Book_WEAK"), "section"] = "Book/Chapter"
    df = df.loc[df['section'] != "Book_WEAK", :]

    # merge the groups into single rows
    #     flag_combos = complete_rows.groupby('row').flag.apply(lambda x:str(list(x))).value_counts().reset_index()
    def combine_strings(x):
        return str(list(x))

    section_df = df.groupby('group').agg(
        {'start': np.min, 'end': np.max, 'match': combine_strings, 'section': 'first'}).reset_index()

    # drop subsets
    subset_bools = section_df.apply(lambda x: check_if_subset(section_df, x), axis=1)
    if not subset_bools.empty:
        section_df = section_df.loc[~subset_bools, :]

    # Rename non-peer-reviewed publications to just publications
    section_df.loc[section_df['section'] == "Non Peer-reviewed publication", "section"] = "Publication"
    section_df = section_df.sort_values('start')

    # sort by ranking
    section_df["rank"] = section_df.section.apply(lambda x: section_ranking[x])
    section_df = section_df.sort_values('rank', ascending=True)

    flags = section_df[["start", "end", "section"]].values

    if verbose:
        print(flags)
    return flags


def initial_section_identification(df):
    # # Create shifted columns for the text column to identify blank rows above and below
    # df['text_above'] = df['text'].shift(1).fillna('')  # Shift down
    # df['text_below'] = df['text'].shift(-1).fillna('')  # Shift up
    # df['text_below2'] = df['text'].shift(-2).fillna('')  # Shift up

    def compute_flag_span(flag_list):
        if flag_list is None:
            return 0
        # Make sure each element x in flag_list is not None and has more than one element
        spans = [x[1] - x[0] for x in flag_list if isinstance(x, (list, tuple, np.ndarray)) and len(x) > 1]
        return sum(spans)


    # df['span'] = df['section_flags'].apply(compute_flag_span)
    # df['span_below'] = df['section_flags'].shift(-1).apply(compute_flag_span)


    # SITUATION 1: ROW IS NEW ENTRY AND ROW BELOW IS ALSO NEW ENTRY
    df['section_dummy1'] = (
            df['section_flags'].apply(lambda x: len(x) > 0) & # Check if flags column is not an empty list
            (df['new_entry'] == True) &              # Check if the text above is empty
            (df['new_entry'].shift(-1) == True)                # Check if the text below is empty
    )

    # # SITUATION 2: ROW IS NEW ENTRY, ROW BELOW IS NOT NEW ENTRY
    # max_row_len = df.text.str.len().max()
    # df['section_dummy2'] = (
    #         df['section_flags'].apply(lambda x: len(x) > 0) & # Check if flags column is not an empty list
    #         (df['text_above'] == '') &              # Check if the text above is empty
    #         (df['text_below'] != '') &
    #         (df['text'].str.len() < 0.7*max_row_len)
    # )
    #
    #
    # # SITUATION 3: BLANK ROW ABOVE, HEADING SPANS TWO ROWS, TWO ROWS BELOW IS BLANK, MORE THAN 50% OF TEXT IS SECTION TEXT
    # max_row_len = df.text.str.len().max()
    # df['section_dummy3'] = (
    #         df['section_flags'].apply(lambda x: len(x) > 0) &  # Check if flags column is not an empty list
    #         (df['text_above'] == '') &                        # Check if the text above is empty
    #         (df['text_below'] != '') &                        # Check if the text below is not empty
    #         (df['text_below2'] == '') &                       # Check if the text below2 is empty
    #         ((df['span'] + df['span_below']) > 0.3*(df['text'].str.len() + df['text_below'].str.len()))
    # )

    #df["ratio"] = df['text'].str.len() + df['text_below'].str.len()
    #df["g40"] = (df['span'] + df['span_below']) > 0.3*(df['text'].str.len() + df['text_below'].str.len())

    # Drop the auxiliary columns if they are not needed
    #df.drop(['text_above', 'text_below', 'text_below2'], axis=1, inplace=True)

    # EXTRACT IDENTIFIED SECTIONS
    def get_section_flag(row):
        #if row[['section_dummy1', 'section_dummy2', 'section_dummy3']].any():
        if row[['section_dummy1']].any():
            return row['section_flags'][0][2]  # Assuming the third element is what you want
        else:
            return None

    df['section'] = df.apply(get_section_flag, axis=1)


    # ###### FOR CONDITION 1: ENSURE THAT THE ROW BELOW SECTION IS NEW_ENTRY #######
    # condition_indices = df.loc[(df["section"].notna()) & (df["section_dummy1"] == True)].index
    # shifted_indices = condition_indices + 1
    # valid_indices = shifted_indices[shifted_indices >= 0]
    # df.loc[valid_indices, 'new_entry'] = True
    #
    # condition_indices = df.loc[(df["section"].notna()) & (df["section_dummy2"] == True)].index
    # shifted_indices = condition_indices + 1
    # valid_indices = shifted_indices[shifted_indices >= 0]
    # df.loc[valid_indices, 'new_entry'] = True
    #
    # ###### Find the indices where "section" is not None and "section_dummy3" is True  #######
    # condition_indices = df.loc[(df["section"].notna()) & (df["section_dummy3"] == True)].index
    # shifted_indices = condition_indices + 3
    # valid_indices = shifted_indices[shifted_indices >= 0]
    # df.loc[valid_indices, 'new_entry'] = True

    return df


def merge_until_new_entry(df: pd.DataFrame, text_column: str, section_column: str, flag_columns: List[str]) -> pd.DataFrame:
    """
    Merge text rows until a new 'page' or 'new_entry' is True is encountered,
    adjusting flag annotations accordingly, and maintaining the 'section' data.
    """
    df = df.reset_index(drop=True)

    # Initialization
    merged_data = []
    temp_text = ""
    temp_flags = {flag_column: [] for flag_column in flag_columns}
    temp_section = None
    flag_buffers = {flag_column: 0 for flag_column in flag_columns}  # Dictionary for flag buffers
    previous_page = None

    # Helper function to update flag indices
    def update_flag_indices(flag_data, index_offset):
        return [(start + index_offset, end + index_offset, label) for start, end, label in flag_data]

    # Iterating over the DataFrame
    for i, row in df.iterrows():
        current_page = row['page']

        # Check if page changed or new entry flag is true, if so, reset buffers
        if previous_page is not None and (current_page != previous_page or row.get('new_entry', False)):
            merged_data.append({
                "app_id": row['app_id'],
                "page": previous_page,  # Use previous_page to assign the page to the merged entry
                text_column: temp_text.strip(),
                "text_length": len(temp_text.strip()),
                section_column: temp_section,
                **{flag_column: temp_flags[flag_column] for flag_column in flag_columns}
            })

            # Reset for the next entry
            temp_text = ""
            temp_flags = {flag_column: [] for flag_column in flag_columns}
            temp_section = None
            flag_buffers = {flag_column: 0 for flag_column in flag_columns}

        # Concatenate text and update section if necessary
        temp_text += " " + row[text_column] if temp_text else row[text_column]
        temp_section = row[section_column] if section_column in df.columns and pd.notnull(row[section_column]) else temp_section

        # Update flags and buffers
        for flag_column in flag_columns:
            flag_value = row[flag_column]
            if isinstance(flag_value, list):  # Assuming the flags are stored as lists directly
                flags = flag_value
            elif isinstance(flag_value, str) and flag_value.strip():  # If it's a JSON string
                flags = json.loads(flag_value)
            else:
                flags = []

            temp_flags[flag_column].extend(update_flag_indices(flags, flag_buffers[flag_column]))
            flag_buffers[flag_column] = len(temp_text)

        # Update previous page for the next iteration
        previous_page = current_page

    # Append the last entry if not already added
    if temp_text:
        merged_data.append({
            "app_id": row['app_id'],
            "page": previous_page,
            text_column: temp_text.strip(),
            "text_length": len(temp_text.strip()),
            section_column: temp_section,
            **{flag_column: temp_flags[flag_column] for flag_column in flag_columns}
        })

    # Convert merged data to DataFrame
    new_df = pd.DataFrame(merged_data)

    return new_df


def get_personnummer_flags(string):
    q = re.finditer(r"\b(1*9*[3-9]\d[0-1]\d[1-3]\d[-]*\d{4})\b", string, re.IGNORECASE)
    pnum_positions = [[m.start(0), m.end(0), "PERSONNUMMER", ] for m in q]
    return pnum_positions


def combine_flags(flags, max_distance=5):
    combined_flags = []
    current_flag = None

    for flag in flags:
        if current_flag is None:
            current_flag = flag
        else:
            start1, end1, label1 = current_flag
            start2, end2, label2 = flag

            # Check if the flags are for the same label (AUTHOR) and within the max_distance
            if label1 == label2 and start2 - end1 <= max_distance:
                current_flag = (start1, end2, label1)
            else:
                combined_flags.append(current_flag)
                current_flag = flag

    if current_flag is not None:
        combined_flags.append(current_flag)

    return combined_flags



def find_author_flags(input_string, author_fullnames):
    """
    Find flags indicating the positions of author names in the input string.

    Args:
        input_string (str): The input string to search for author names.
        author_fullnames (list): A list of author full names to search for in the input string.

    Returns:
        list: A list of tuples containing start and end indices along with the flag "AUTHOR"
              indicating the positions of author names in the input string.
    """
    # clean the author_fullnames, ensure it is list
    if isinstance(author_fullnames, str):
        author_fullnames = author_fullnames.split(" ")

    # Split the input string into words
    words = input_string.split()

    # Initialize variables to keep track of the accumulated length and matching flags
    accumulated_length = 0
    matched_flags = []

    # Iterate through the words and find matches with fuzzy string matching
    for word in words:
        for name in author_fullnames:
            # Calculate the ratio between the word and the author's full name
            score = fuzz.ratio(name, word)
            if score >= 70:  # You can adjust the threshold as needed
                start_index = accumulated_length
                end_index = accumulated_length + len(word)
                matched_flags.append((start_index, end_index, "AUTHOR"))
                break  # Exit the inner loop once a match is found for the current name

        # Update the accumulated length
        accumulated_length += len(word) + 1  # +1 for the space

    return combine_flags(matched_flags)



def find_author_list_flags(row):

    distance = 10

    accepted_ents = ["ORG", "PERSON", "GPE"]

    # Create a DataFrame from nlp_flags
    label_df = pd.DataFrame(row.nlp_flags, columns=["start", "end", "label"])

    # Filter the DataFrame to include only detected names and organizations
    label_df = label_df.loc[label_df["label"].isin(accepted_ents)]

    # Compute the distance between flags
    label_df["delta_up"] = label_df.start - label_df.end.shift()
    label_df["delta_down"] = abs(label_df.end - label_df.start.shift(-1))

    # Filter out tags if more than 5 characters apart
    label_df = label_df.loc[((label_df["delta_up"] < distance) | (label_df["delta_down"] < distance))]

    # Identify groups
    label_df["group"] = ((label_df["delta_up"] > distance) | pd.isnull(label_df["delta_up"])).cumsum()

    # Collapse the groups
    collapsed_df = label_df.groupby('group').agg({'start': 'first', 'end': 'last', 'label': 'first'}).reset_index()
    collapsed_df["label"] = "AUTHOR_LIST"
    flags = collapsed_df[["start", "end", "label"]].values

    return flags



def find_ending_flags(string):
    ending_flags = [
        r"p{1,2}\.*\s*\d+[\s\-\–\.\d]*",  # p.5-66 , p123
        r"[\)\(\d:\s-]{6,}\b",  # 12(2) 4:22
        r"vol\w*\s*\.*\d+\b",  # vol 14
        r"number of citations:*\s*[\d]+\b",  # number of citations 4
        r"\d{,3}\(\d{,3}\)",  # 1(12)
        r"\bdoi{,5}\d{2,}.{2,}\d{3,}.{2,}\d{3,}"  # doi
    ]

    # false_flags = [
    #     r"[^\d:;-_\s]\d{4,6}[^\d:;-_]|^\d{4,6}[^\d:;-_\s]"                               # up to six digits e.g. 101010
    # ]

    false_flags = []

    dfs = []
    # check for flags
    for pattern in ending_flags:
        q = re.finditer(r"(" + pattern + r")", string, re.IGNORECASE)
        flag_positions = [(m.group(1), m.start(0), m.end(0)) for m in q]
        dfs.append(pd.DataFrame(flag_positions, columns=['span', 'start', 'end']))
    df = pd.concat(dfs).sort_values('start', ascending=True)

    # check for false flags and remove from dfs
    def check_false_flag(string):
        for pattern in false_flags:
            if re.search(pattern, string):
                return True
            else:
                return False

    if len(false_flags) > 0:
        false_flag_inx = df.apply(lambda x: check_false_flag(x.span), axis=1)
        if not false_flag_inx.empty:
            df = df.loc[~false_flag_inx, :]

    # for pattern in false_flags:
    #     q = re.finditer(r"(" + pattern + r")", string, re.IGNORECASE)
    #     flag_positions = [(m.group(1), m.start(0), m.end(0)) for m in q]
    #     dfs.append(pd.DataFrame(flag_positions, columns=['span', 'start','end']))

    if df.empty:
        return []

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
    group_count = len(df.loc[(df['diff'] > 10) | (pd.isnull(df['diff'])), :].index)
    df.loc[(df['diff'] > 10) | (pd.isnull(df['diff'])), 'group_id'] = range(group_count)
    df.group_id = df.group_id.interpolate(method='pad')

    # # aggregate the group comma-group start and end
    df = pd.DataFrame(df.groupby(['group_id']).agg({'start': ['min'], 'end': ['max']}))
    df.columns = df.columns.droplevel(level=1)

    flags = [(x[0], x[1], "ENDING") for x in list(df.values)]

    return flags


def find_year_flags(string):

    # capture all four-digit combinations without dashes before or after
    pattern = r"[^-]([12]\d{3})[^-]|^([12]\d{3})[^-]|[^-]([12]\d{3})$"
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

    explicit_year_from_month_df = pd.DataFrame(year_positions,  columns=['start','end', 'year'])
    year_df = pd.concat([year_df, explicit_year_from_month_df]).drop_duplicates()

    # remove subset year-matches
    #subset_bools = year_df.apply(lambda x: check_if_subset(year_df, x), axis=1)
    flags = []
    if not year_df.empty:
        #year_df = year_df.loc[~subset_bools, :]
        year_df = year_df.drop_duplicates(subset=['start', 'end'])
        flags = [(x[0], x[1], "YEAR") for x in year_df.values]

    return flags


def identify_headers_footers(xdf):
    # Check if the word "appendix c" is in the first three rows of column 'text'
    header_patterns = [r"appendix\s*c", r"list of", r"bilaga\s*c"]
    footer_patterns = [r"page", r"sida"]

    # This function will check if the text matches any of the patterns
    def match_patterns(text, patterns):
        return any(pd.Series(text).str.contains(pat, case=False, regex=True).any() for pat in patterns)

    # Define a function to apply to each group of rows corresponding to each page
    def check_head(group):
        # Use the head method to select the first 4 rows of each group
        header_rows = group.head(4)
        group.loc[header_rows.index, 'is_header'] = header_rows['text'].apply(
            lambda text: len(text) < 100 and match_patterns(text, header_patterns))
        return group

    def check_foot(group):
        footer_rows = group.tail(4)
        group.loc[footer_rows.index, 'is_footer'] = footer_rows['text'].apply(
            lambda text: len(text) < 15 and match_patterns(text, footer_patterns))
        return group

    # Group the DataFrame by 'page' and apply the function to each group
    xdf['is_header'] = False  # Initialize the column with False
    xdf = xdf.groupby('page', group_keys=False).apply(check_head)
    xdf['is_footer'] = False  # Initialize the column with False
    xdf = xdf.groupby('page', group_keys=False).apply(check_foot)

    return xdf


def filter_flags(df, column_name, new_column_name, min_val=None, max_val=None):
    # The clean function now correctly uses min_val and max_val parameters
    def clean(flag_list, min_val, max_val):
        if max_val is None:
            max_val = float('inf')
        if min_val is None:
            min_val = float('-inf')
        # Assuming each flag in the flag_list is a tuple where the first value can be compared to min_val and max_val
        return [f for f in flag_list if min_val <= f[0] and f[1] <= max_val]

    # Apply the clean function to each element in column_name
    # We pass the values for min_val and max_val to the clean function
    df[new_column_name] = df[column_name].apply(lambda flags: clean(flags, min_val, max_val))
    return df


def create_pub_df(xdf):

    pub_df = xdf.copy()

    # Step 2: Create a dummy column equal to one if 'section' is not None
    pub_df['section_dummy'] = np.where(pub_df['section'].notna(), 1, 0)

    # Step 3: Propagate the non-None 'section' values down to subsequent rows
    pub_df['section'] = pub_df['section'].fillna(method='ffill')

    # Step 4: Remove the rows where section dummy is 1
    pub_df = pub_df[pub_df['section_dummy'] == 0]

    # Step 5: Drop the section dummy column
    pub_df.drop('section_dummy', axis=1, inplace=True)

    # Create pub_id
    pub_df['pub_id'] = range(1, len(pub_df) + 1)

    # rename section to type
    pub_df.rename(columns={"section": "type"}, inplace=True)

    # Step keep cols of interest
    pub_df = pub_df[["app_id", "page", "pub_id", "text", "type"]]


    return pub_df



def export_excel(path, ocr_ext, clean, xdf, pub_df):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        # Write each DataFrame to a different worksheet.
        ocr_ext.to_excel(writer, sheet_name='OCR', index=False)
        clean.to_excel(writer, sheet_name='Clean', index=False)
        xdf.to_excel(writer, sheet_name='Merged', index=False)
        pub_df.to_excel(writer, sheet_name='Publications', index=False)
    # print(f"saved {path}")


def meta_analyze(clean_df, xdf, round):

    # ----- create table if does not exist ------

    # create table
    cols_tups = [
        ('round', 'INT'),
        ('app_id', 'VARCHAR(20)'),
        ('pages', "INT"),
        ('dist_0_m100', "INT"),
        ('dist_m100_m300', "INT"),
        ('dist_m300_m1000', "INT"),
        ('dist_m1000_minf', "INT"),
        ("header_count", "INT"),
        ("pnum_str", "VARCHAR(256)"),
        ("pnum_count", "INT"),
    ]

    table_name = "extraction_metadata"
    if not sql.check_table_exists(table_name):
        sql.create_table(table_name, cols_tups,['round', 'app_id'], verbose=False)

    app_id = clean_df.iloc[0].app_id
    pages = clean_df.iloc[-1].page

    row = {"round": round, "app_id": app_id, "pages": pages}

    # ----- compute neagtive distance dummies ------
    ranges = {"dist_0_m100" : [0, -100], "dist_m100_m300": [-100, -300], "dist_m300_m1000": [-300, -1000],
              "dist_m1000_minf": [-1000, -100000000000]}

    for key, value in ranges.items():
        m = clean_df.loc[((clean_df["dist"] < value[0]) & (clean_df["dist"] >= value[1]))]
        row[key] = int(len(m) > 0)

    # ----- compute the number of headers -----
    pages_with_header = []

    for page, group in xdf.groupby('page'):
        if group.head(5)['is_header'].any():
            pages_with_header.append(page)


    # ----- extract and count personnummer --------

    pnums = extract_personnummer(clean_df)
    row["pnum_str"] = json.dumps(pnums)
    row["pnum_count"] = len(pnums)


    # ----- Upload to db --------
    df = pd.DataFrame([row])
    sql.upload_to_table(table_name, df)



def extract_personnummer_from_flags(row):
    extracted = []
    for flag in row['personnummer_flags']:
        start_index, end_index, _ = flag
        extracted.append(row['text'][start_index:end_index])
    return ', '.join(extracted)  # Joining multiple extractions with a comma

def extract_personnummer(df, top_n_rows=5):
    extracted_values = []

    for page, group in df.groupby('page'):
        first_three_rows = group.head(top_n_rows)
        non_missing_personnummer_rows = first_three_rows[first_three_rows['personnummer_flags'].notna()]

        # Apply the function to each row
        non_missing_personnummer = non_missing_personnummer_rows.apply(extract_personnummer_from_flags, axis=1)

        extracted_values.extend(non_missing_personnummer.tolist())

    # clean the extracted values because index might shift
    extracted_values_clean = []
    for x in extracted_values:
        x = re.sub(r"[^\d-]", "", x)
        extracted_values_clean.append(x)

    pnums = []
    for item in extracted_values_clean:
        for x in pnums:
            if x in item or item in x:
                if len(item) > len(x):
                    pnums.remove(x)
                else:
                    continue

        if item not in pnums:
            pnums.append(item)

    if "" in pnums:
        pnums.remove("")
    return pnums



def multi_author_predict(df):
    '''
    Predicts whether a pdf is a multi-author pdf
    :param df: Probably the xdf
    :return:
    '''

    # Create groups based on is_header. Each time 'is_header' is True, a new group starts.
    df["group"] = (df["is_header"]).cumsum()

    # Count the occurrences of non-blank author_flag for each group
    author_count = df[df["author_flags"] != ""][["group", "author_flags"]].groupby("group").count()

    #print(author_count)



###################################


def parse_extraction(app_id, path):

    # ------- IMPORT -------------

    # get author data from the VR dataset
    application = get_applications(app_ids=[app_id], format="df")

    # get OCR extraction data
    ocr = get_ocr_ext(app_id, clean=True, nlp=True)

    if ocr is False:
        return False

    # ------- CLEANING -------------

    # flag out repeated headers (only first rows)
    ocr = flag_repeated_headers_footers(ocr)

    ocr.to_excel(path, index=False)
    raise ValueError('bb')

    # Flag invalid rows, to be filtered
    invalid_bools = ["rep_header", "rep_footer"]
    ocr = flag_invalid_rows(ocr, invalid_bools)

    # filter out invalid rows, creating clean df
    clean = drop_invalid_rows(ocr, invalid_bools)

    # ------- FLAGGING --------------

    # check for indentation
    clean["indent"] = check_indent(clean, "in", 100)
    clean["outdent"] = check_indent(clean, "out", 100)

    # grab all section flags
    clean['section_flags'] = clean.text.apply(find_section_flags)

    # FIND FLAGS
    clean['personnummer_flags'] = clean['text'].apply(get_personnummer_flags)

    # find author flags
    clean['author_flags'] = clean['text'].apply(lambda x: find_author_flags(x, f'{application["name"]} {application["surname"]}'))

    #clean['author_list_flags'] = clean.apply(find_author_list_flags, axis=1)

    clean['ending_flags'] = clean.text.apply(find_ending_flags)

    clean['year_flags'] = clean.text.apply(find_year_flags)

    # ------- IDENTIFICATION --------------

    # recompute distance between rows (previously computed before cleaning)
    clean = compute_distance(clean)

    clean = identify_new_entries(clean)

    # identify valid sections
    clean = initial_section_identification(clean)

    # check for multi-author pdfs predict pages
    #multi_author_check(xdf)

    # ------- MERGING --------------

    flag_cols = ["nlp", "personnummer_flags", "author_flags", "ending_flags", "year_flags"]
    xdf = merge_until_new_entry(clean, text_column="text", section_column="section", flag_columns=flag_cols)

    # ------- POST-MERGE CLEANING --------------

    xdf = identify_headers_footers(xdf)

    #multi_author_predict(xdf)
    #raise ValueError('aaa')

    # ------- CREATE PUB DF --------------

    # Flag invalid rows, to be filtered
    invalid_bools = ["is_header", "is_footer"]
    xdf = flag_invalid_rows(xdf, invalid_bools)

    # filter out invalid rows, creating clean df
    xdf_clean = drop_invalid_rows(xdf, invalid_bools)

    pub_df = create_pub_df(xdf_clean)

    # ------- META ANALYSIS ------------

    #meta_analyze(clean, xdf, 1)

    # ------- EXPORT FILE --------------

    export_excel(path, ocr, clean, xdf, pub_df)


    #print(f"Completed parsing of application of {app_id}")


def try_parse_extraction(app_id, output_dir):
    path = f"{output_dir}/{app_id}.xlsx"
    try:
        parse_extraction(app_id, path)
    except:
        return app_id

def parse_all_extractions(output_dir):

    all_app_ids = get_applications(format="df").app_id.to_list()

    completed = [x[:-5] for x in os.listdir(output_dir) if ".xlsx" in x]
    remaining = list(np.setdiff1d(all_app_ids, completed))
    print(f"{len(remaining)} Remaining")

    num_processes = 8

    g = partial(try_parse_extraction, output_dir=output_dir)

    # Create a pool of workers and process the items
    with Pool(processes=num_processes) as pool:
        # Use imap_unordered to get an iterator over the results
        # Use tqdm to create a progress bar
        results = list(tqdm(pool.imap_unordered(g, remaining), total=len(remaining), desc='Parsing OCR Scans', unit='Document'))

    print("FAILS", results)

if __name__ == "__main__":

    # t = "3.	Översiktsartiklar,	bokkapitel,	böcker"
    # find_section_flags(t, verbose=True)
    # exit()




    # single
    path = f"{data_dir}/debug/temp3.xlsx"
    # x = parse_extraction("2012-00202", path)
    x = parse_extraction("2012-04184", path)
    #
    exit()
    # x = parse_extraction("2014-04153", path)
    # x = parse_extraction("2012-00202", path)

    # multiprocessing
    output_dir = f"{data_dir}/extractions/2023-11-09"
    parse_all_extractions(output_dir)

