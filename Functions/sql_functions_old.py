import Functions.functions as f
import mysql.connector as connection

def check_table_exists(table_name):
    mydb = connection.connect(host="localhost", database='gender_in_academia', user="root")
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
    mydb = connection.connect(host="localhost", database='gender_in_academia', user="root")
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

    mydb = connection.connect(host="localhost", database='gender_in_academia', user="root")
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
        if type(primary_key) is list:
            key_string = ", ".join([f"`{x[0]}` ({x[1]})" for x in primary_key])
        else:
            key_string = f"`{primary_key}`"
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
    for col in df.columns:
        tup_list.append((col, dtype_mapping[df.dtypes[0].name]))

    create_table(table_name, tup_list, primary_key, unique_keys=None, verbose=verbose)


def upload_to_table(table_name, df):
    df = df.fillna("NULL")

    mydb = connection.connect(host="localhost", database='gender_in_academia', user="root")
    cursor = mydb.cursor()

    # now iteratively insert
    col_tuples_list = [f"`{x}`" for x in df.columns]
    col_tuple_string = ", ".join(col_tuples_list)
    row_count = len(df)
    print(f"uploading data to SQL table: {table_name}")
    for row_id, row in df.iterrows():
        command = f'''
        INSERT IGNORE INTO `{table_name}` ({col_tuple_string})
        VALUES {tuple(row.values.tolist())};

        '''
        f.progress_bar(row_id, row_count)
        cursor.execute(command)

    mydb.commit()
    cursor.close()
    mydb.close()


def insert(table_name, col_val_tups, verbose=False):
    mydb = connection.connect(host="localhost", database='gender_in_academia', user="root")
    cursor = mydb.cursor()

    col_tuples_list = [f"`{x[0]}`" for x in col_val_tups]
    col_tuple_string = ", ".join(col_tuples_list)
    vals = [f"\"{x[1]}\"" for x in col_val_tups]
    command = f'''
       INSERT INTO `{table_name}` ({col_tuple_string})
       VALUES ({", ".join(vals)});

       '''
    if verbose:
        print(command)
    cursor.execute(command)

    mydb.commit()
    cursor.close()
    mydb.close()


def count_rows(table_name, where=None, verbose=False):
    output = None
    mydb = connection.connect(host="localhost", database='gender_in_academia', user="root")
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
