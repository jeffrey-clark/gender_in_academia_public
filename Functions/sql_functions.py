import re
import json
import Functions.functions as f
import mysql.connector as connection
import pandas as pd


def get_connection():
    return connection.connect(host="localhost", database='gender_in_academia', user="root", password="monoxide@washout0clinical5timbre", charset='utf8')

def check_table_exists(table_name):
    mydb = get_connection()
    cursor = mydb.cursor()

    command = f'''
    SELECT * FROM information_schema.tables
    WHERE table_name = '{table_name}';
       '''

    cursor.execute(command)
    output = bool(len(cursor.fetchall()))
    cursor.close()
    mydb.close()
    return output

def drop_table(table_name):
    mydb = get_connection()
    cursor = mydb.cursor()

    # drop table if it already exists
    command = f'''
       DROP TABLE IF EXISTS {table_name};
       '''
    cursor.execute(command)
    mydb.commit()
    cursor.close()
    mydb.close()
    print(f"SQL table '{table_name}' successfully dropped")


def create_table(table_name, tuple_list, primary_key, unique_keys=None, verbose=False):
    col_type_entries = []

    # if we have AUTO_INCREMENT, we need ot declare that column as primary key upon table creation
    primary_key_on_init = ""

    for t in tuple_list:
        col_type_entries.append(f"`{t[0]}` {' '.join(t[1:])}")
        if 'AUTO_INCREMENT' in t:
            # overwrite primary key
            primary_key_name = primary_key
            primary_key_length = ""
            if type(primary_key) is tuple:
                primary_key_name = primary_key[0]
                primary_key_length = f" ({primary_key[1]})"

            if primary_key_name != t[0]:
                raise SyntaxError('Primary key must be the columns with AUTO_INCREMENT')

            primary_key_on_init = f", PRIMARY KEY (`{primary_key}`{primary_key_length}) "

    query_data = ", ".join(col_type_entries)

    mydb = get_connection()
    cursor = mydb.cursor()

    # create the table
    command = f'''
            CREATE TABLE {table_name}(
            {query_data}{primary_key_on_init}        
            );
            '''
    if verbose:
        print(command)
    cursor.execute(command)

    # add primary key retrospectively (allowing for multi-column keys
    if primary_key_on_init == "":
        key_string_list = []
        if type(primary_key) is not list:
            primary_key = [primary_key]
        for p in primary_key:
            if (type(p) is list) or (type(p) is tuple):
                key_string_list.append(f"`{p[0]}` ({p[1]})")
            else:
                key_string_list.append(f"`{p}`")
        key_string = ", ".join(key_string_list)

        command = f'''
        ALTER TABLE `{table_name}` ADD PRIMARY KEY ({key_string});
        '''

        if verbose:
            print(command)
        cursor.execute(command)

    # add any unique keys
    if unique_keys is not None:
        if type(unique_keys) is not list:
            unique_keys = [unique_keys]
        for u in unique_keys:
            if (type(u) is list) or (type(u) is tuple):
                unique_key = ", ".join([f"`{x}`" for x in u])
            else:
                unique_key = f"`{u}`"

            command = f'''
            ALTER TABLE `{table_name}` ADD UNIQUE ({unique_key});
            '''
            if verbose:
                print(command)
            cursor.execute(command)


    cursor.close()
    mydb.close()
    print(f"Created SQL table '{table_name}")


def create_table_from_df(table_name, df, primary_key, verbose=False):

    # create string of columns for SQL command
    col_type_entries = []
    dtype_mapping = {
        'object': 'TEXT',
        'float64': 'FLOAT',
        'int64': 'INT'
    }

    tup_list = []
    for i, col in enumerate(df.columns):
        #print(col, df.dtypes[i].name)
        tup_list.append((col, dtype_mapping[df.dtypes[i].name]))

    create_table(table_name, tup_list, primary_key, unique_keys=None, verbose=verbose)


def upload_to_table(table_name, df, verbose=False):
    df = df.fillna("NULL")

    mydb = get_connection()
    cursor = mydb.cursor()

    # now iteratively insert
    col_tuples_list = [f"`{x}`" for x in df.columns]
    col_tuple_string = ", ".join(col_tuples_list)
    row_count = len(df)
    if verbose:
        print(f"uploading data to SQL table: {table_name}")
    for row_id, row in df.iterrows():
        command = f'''
        INSERT IGNORE INTO `{table_name}` ({col_tuple_string})
        VALUES {tuple(row.values.tolist())};

        '''
        if verbose:
            f.progress_bar(row_id, row_count)
        cursor.execute(command)

    mydb.commit()
    cursor.close()
    mydb.close()


def insert(table_name, col_val_tups, verbose=False):
    mydb = get_connection()
    cursor = mydb.cursor()

    col_tuples_list = [f"`{x[0]}`" for x in col_val_tups]
    col_tuple_string = ", ".join(col_tuples_list)
    vals = []
    for x in col_val_tups:
        if x[1] is not None:
            if type(x[1]) is str:
                v = re.sub(r"\\\"", "", json.dumps(x[1]))
            else:
                v = f"\"{x[1]}\""
        else:
            v = "NULL"
        vals.append(v)

    val_string = ", ".join(vals)
    #print("VAL STRING IS", val_string)

    command = f'''
       INSERT IGNORE INTO `{table_name}` ({col_tuple_string})
       VALUES ({val_string});

       '''
    if verbose:
        print(command)
    cursor.execute(command)

    mydb.commit()
    cursor.close()
    mydb.close()


def select(table_name, cols=None, where=None, operation=None, verbose=False):
    output = None

    if str(operation).lower() == "count":
        operation = "count"

    mydb = get_connection()
    cursor = mydb.cursor()

    where_string = ""
    if type(where) is list:

        equalities = []
        # flatten list if nested
        flat_list = []
        if type(where[0]) is list or type(where[0]) is tuple:
            for sub_list in where:
                flat_list += sub_list
        else:
            flat_list = where

        for i in range(0, int((len(flat_list)*0.5))):
            if type(flat_list[i*2+1]) is list:
                inner_list_string = json.dumps(flat_list[i*2+1])[1:-1]
                equalities.append(f"`{flat_list[i * 2]}` IN ({inner_list_string})")
            else:
                equalities.append(f"`{flat_list[i*2]}`=\"{flat_list[i*2+1]}\"")
        where_string = f"WHERE {' AND '.join(equalities)}"

    if type(cols) is list:
        col_string = ", ".join(cols)
    elif cols is None:
        col_string = "*"
    else:
        raise SyntaxError("Unknown argument cols in select function")

    if operation == "count":
        col_string = f"COUNT({col_string})"
    elif operation is None:
        pass
    else:
        raise SyntaxError("Unknown argument operation of select function")

    command = f'''
    SELECT {col_string} FROM `{table_name}`
    {where_string};

       '''
    if verbose:
        print(command)
    cursor.execute(command)


    if col_string == "*":
        cols = [i[0] for i in cursor.description]

    if operation == "count":
        output = int(cursor.fetchall()[0][0])
    elif operation is None:
        output = pd.DataFrame(cursor.fetchall(), columns=cols)

    cursor.close()
    mydb.close()

    return output


def get_max_in_table(table_name, column):
    # connect to the database
    mydb = get_connection()
    cursor = mydb.cursor()
    query = f'''
                SELECT MAX(`{column}`)
                FROM {table_name};
            '''
    cursor.execute(query)
    max_id = cursor.fetchall()[0][0]
    mydb.close()
    return max_id


def get_code_table(table_name):
    # connect to the database
    mydb = get_connection()
    # cursor = mydb.cursor()
    query = f'''
                SELECT *
                FROM {table_name}
                WHERE `name` IS NOT NULL;
            '''
    # cursor.execute(query)
    df = pd.read_sql(query, mydb)
    # print(cursor.fetchall())
    mydb.close()
    return df


def get_api_ids(table_name, name_list):
    # connect to the database
    mydb = get_connection()
    cursor = mydb.cursor()
    name_string = ", ".join([f"\"{x}\"" for x in name_list])
    query = f'''
        SELECT *
        FROM {table_name}
        where `name` in ({name_string}) 
        GROUP BY `name`
        HAVING COUNT(`name`) = 1
        ;
        '''
    cursor.execute(query)

    # store the query results and create dataframe with bool status of name list whether name matched
    result = cursor.fetchall()
    query_df = pd.DataFrame(result)
    mydb.close()
    query_df.columns = ['id', 'name']
    index_bool = pd.Series(name_list).isin(query_df['name'])
    output_df = pd.DataFrame({'name': name_list, 'match_df': index_bool})
    output_df = pd.merge(output_df, query_df, 'left', 'name')

    id_list = []
    for x in output_df['id'].values:
        if pd.isnull(x):
            id_list.append(None)
        else:
            id_list.append(int(x))

    return id_list


def get_column_names(table_name):
    # connect to the database
    mydb = get_connection()
    cursor = mydb.cursor()
    query = f'''
        SELECT *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = N'{table_name}';
        '''
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)
    col_names = [x[3] for x in result]
    cursor.close()
    mydb.close()
    return col_names


def download(table_name):

    # connect to the database
    mydb = get_connection()
    cursor = mydb.cursor()
    query = f'''
           SELECT *
           FROM {table_name};
           '''
    cursor.execute(query)
    data = []
    for row in cursor:
        data.append(row)

    cursor.close()
    mydb.close()

    col_names = get_column_names(table_name)
    df = pd.DataFrame(data, columns=col_names)

    #df.to_excel(f"../Data/{table_name}.xlsx", index=False, encoding='utf-16')
    return df



def show_dbs():
    mydb = get_connection()
    cursor = mydb.cursor()

    command = f'''
    SHOW DATABASES
       '''

    cursor.execute(command)
    output = cursor.fetchall()
    cursor.close()
    mydb.close()
    return output




def count_rows(table_name, where=None, verbose=False):
    output = None
    mydb = get_connection()
    cursor = mydb.cursor()

    where_string = ""
    if type(where) is list:

        equalities = []
        # flatten list if nested
        flat_list = []
        if type(where[0]) is list:
            for sub_list in where:
                flat_list += sub_list
        else:
            flat_list = where

        for i in range(0, int((len(flat_list)*0.5))):
            equalities.append(f"`{flat_list[i*2]}`=\"{flat_list[i*2+1]}\"")
        where_string = f"WHERE {' AND '.join(equalities)}"


    command = f'''
    SELECT COUNT(*) FROM `{table_name}`
    {where_string};

       '''
    if verbose:
        print(command)
    cursor.execute(command)

    output = int(cursor.fetchall()[0][0])
    cursor.close()
    mydb.close()
    return output


######################################################

def get_all_units():
    mydb = get_connection()
    cursor = mydb.cursor()

    command = f'''
        SELECT * FROM all_units;
           '''

    cursor.execute(command)
    result = cursor.fetchall()
    cursor.close()
    mydb.close()

    df = pd.DataFrame(result, columns=['county_id', 'county', 'area_id', 'area', 'unit_id', 'unit'])
    return df


if __name__ == "__main__":
    print(show_dbs())