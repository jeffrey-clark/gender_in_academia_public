from Functions.functions import *
import Functions.functions as f
import Functions.sql_functions as sql
from Functions.StringComp import StringComp
from multiprocessing import Pool


def match_api(app_id):

    extraction = sql.select("nlp_extractions", ["*"], ['app_id', app_id])
    api_data =   sql.select("clarivate_data", ["*"], ['app_id', app_id])

    row = extraction.iloc[3]
    #print(row)

    api_row = api_data.iloc[0]
    #print(api_row)

    #for id, row in extraction.iterrows():
    #for id, row in [(3, row)]:


    def get_matches(row):

        def compare(api_row):
            s = StringComp(row.text, api_row.title, 2, 1)
            return (s.s1.percent_letters_matched, s.s1.percent_words_matched, s.max_percent_letters_matched, s.max_percent_words_matched)
            # return (s.max_percent_letters_matched, s.max_percent_words_matched)
        #print(row.text)
        api_pubs = api_data.copy()
        api_pubs[['letter_match', 'word_match', 'letter_match_max', 'word_match_max']] = api_pubs.apply(compare, axis=1).apply(pd.Series)
        api_pubs.sort_values(by=["word_match"], ascending=[False], inplace=True)

        top_5 = api_pubs.iloc[:5].copy()

        # drop if letter_match_max < 50 and letter_match_max < 50
        top_5 = top_5.loc[((top_5["letter_match_max"] >= 50) & (top_5["word_match_max"] >= 50)), :].reset_index(drop=True)

        cols = list(top_5.columns)[1:]
        top_5["row_id"] = row.row_id

        top_5 = top_5[["app_id", "row_id"] + cols]
        return top_5

    result = pd.concat(extraction.apply(get_matches, axis=1).values)

    return result



def setup():
    # Create SQL table if it does not exist

    # Set Table Name
    table_name = "raw_matches"
    print("RUNNING MATCH")
    # create sample table
    df = match_api("2012-00202")

    # create table if not exists
    table_exists = sql.check_table_exists(table_name)
    if not table_exists:
        sql.create_table_from_df(table_name, df, [('app_id', 20), ('row_id'), ('api_id', 40)], verbose=True)


    # create status table
    cols_tups = [('app_id', 'VARCHAR(20)'),
                 ('success', 'INT'),
                 ('time', 'DATETIME', 'DEFAULT CURRENT_TIMESTAMP')]
    if not sql.check_table_exists('status_raw_matches'):
        sql.create_table('status_raw_matches', cols_tups, 'app_id', verbose=True)


def drop_tables():
    # drop the table if it exists
    sql.drop_table("raw_matches")
    sql.drop_table("status_raw_matches")



def execute_raw_matching(app_id):

    df = match_api(app_id)
    if df.empty is False:
        # here we do the uploading
        sql.upload_to_table("raw_matches", df)


def try_execute_raw_matching(app_id):
    try:
        execute_raw_matching(app_id)
        success = 1
        print("completed", app_id)
    except:
        success = 0

    sql.insert('status_raw_matches', [('app_id', app_id), ('success', success)], verbose=False)


if __name__ == '__main__':

    # If we are restarting, drop tables
    # drop_tables()

    # first we setup
    #setup()

    # Let us run the model on each row
    all_app_ids = get_applications(format="df").app_id.to_list()

    # filter out the completed applications
    completed = sql.select("status_raw_matches", ["app_id"], ['success', 1]).app_id.to_list()
    remaining = list(np.setdiff1d(all_app_ids, completed))

    print(f"{len(remaining)} app_ids remaining")


    with Pool(processes=8) as pool:
        result = pool.map(try_execute_raw_matching, remaining)
        print(result)