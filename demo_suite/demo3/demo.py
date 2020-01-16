
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import csv, sqlite3
import logging
import cProfile


# In[2]:


### utilities

def _get_col_datatypes(fin):
    """from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python """
    dr = csv.DictReader(fin) # comma is default delimiter
    fieldTypes = {}
    for entry in dr:
        feildslLeft = [f for f in dr.fieldnames if f not in fieldTypes.keys()]
        if not feildslLeft: break # We're done
        for field in feildslLeft:
            data = entry[field]

            # Need data to decide
            if len(data) == 0:
                continue

            if data.isdigit():
                fieldTypes[field] = "INTEGER"
            else:
                fieldTypes[field] = "TEXT"
        # TODO: Currently there's no support for DATE in sqllite

    if len(feildslLeft) > 0:
        raise Exception("Failed to find all the columns data types - Maybe some are empty?")

    return fieldTypes


def escapingGenerator(f):
    """from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python """
    for line in f:
        yield line.encode("ascii", "xmlcharrefreplace").decode("ascii")


def csvToDb(csvFile, outputToFile = False, con=None, table_name=None):
    """ from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python 
    with small changes """
    # TODO: implement output to file
    if table_name is None:
        table_name = 'ads'

    with open(csvFile,mode='r', encoding="ISO-8859-1") as fin:
        dt = _get_col_datatypes(fin)

        fin.seek(0)

        reader = csv.DictReader(fin)

        # Keep the order of the columns name just as in the CSV
        fields = reader.fieldnames
        cols = []

        # Set field and type
        for f in fields:
            cols.append("%s %s" % (f, dt[f]))

        # Generate create table statement:
        stmt = "CREATE TABLE " + table_name + " (%s)" % ",".join(cols)
        if con is None:
            print('Creating engine')
            con = sqlite3.connect(":memory:")
        cur = con.cursor()
        cur.execute(stmt)

        fin.seek(0)


        reader = csv.reader(escapingGenerator(fin))

        # Generate insert statement:
        stmt = "INSERT INTO " + table_name + " VALUES(%s);" % ','.join('?' * len(cols))

        cur.executemany(stmt, reader)
        con.commit()

    return con

class Profiler():
    """ ctxtM for old python without profile"""
    def __init__(self, nlines):
        self.nlines = nlines

    def __enter__(self):
        self.pr = cProfile.Profile()
        self.pr.enable()

    def __exit__(self, *args):
        import pstats, io
        self.pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(self.pr, stream=s).sort_stats(sortby)
        ps.print_stats(self.nlines)
        print(s.getvalue())
### end of utilities


# In[3]:


number_of_test_subjects = 8
## pandas
with Profiler(2):
    client_info = pd.read_csv('../MOCK_DATA/mock_data_1.csv')
    male_clients = client_info['gender'] == 'Male'
    test_male_client = client_info[ male_clients ][:number_of_test_subjects]
    pd_out = test_male_client.ip_address

## sqlite3
with Profiler(2):
    con = sqlite3.connect(':memory:') # or file or ?
    con = csvToDb('../MOCK_DATA/mock_data_1.csv', con=con, table_name='client_info')
    query = """
    select ip_address
    from client_info
    where gender = 'Male'
    limit """ + str(number_of_test_subjects) + """
    """
    sql_out = con.execute(query)
    sql_label = [description[0] for description in sql_out.description]
    sql_data = sql_out.fetchall()

## now check
for i, (sql_x, pd_x) in enumerate(zip(sql_data, pd_out)):
    if i < number_of_test_subjects//2:
        print(sql_x[0], '|', pd_x)
    assert sql_x[0] == pd_x
print('     PASSED')


# In[4]:


client_info = pd.read_csv('../MOCK_DATA/mock_data_1.csv')
works_at = pd.read_csv('../MOCK_DATA/mock_data_2.csv')

# dumb pandas
with Profiler(2):
    print('email company name')
    print('------------------')
    for id_w, w in enumerate(works_at.email):
        for id_c, c in enumerate(client_info.email):
            if w == c:
                print(works_at.email[id_w], works_at.company[id_w], client_info.first_name[id_c])


# In[5]:


## pandas
with Profiler(2):
    matches = works_at.email.isin(client_info.email)
    print('email company name')
    print('------------------')
    for email, company in zip( works_at[matches].email, works_at[matches].company):
        guy = client_info[client_info.email == email]
        print(email, company, guy.first_name.values[0])


# In[6]:


## sql
with Profiler(2):
    con = sqlite3.connect(':memory:') # or file or ?
    con = csvToDb('../MOCK_DATA/mock_data_1.csv', con=con, table_name='client_info')
    con = csvToDb('../MOCK_DATA/mock_data_2.csv', con=con, table_name='works_at')
    query = """
    select works_at.email, works_at.company, client_info.last_name
    from client_info, works_at
    where client_info.email = works_at.email
    """
    sql_out = con.execute(query)
    sql_label = [description[0] for description in sql_out.description]
    sql_data = sql_out.fetchall()
    print(sql_data)

