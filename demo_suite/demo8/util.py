import csv
import RA
import pandas as pd

def _get_col_datatypes(fin):
    """modified from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python """
    dr = csv.DictReader(fin) # comma is default delimiter
    fieldTypes = {}
    dtypes = []
    for entry in dr:
        feildslLeft = [f for f in dr.fieldnames if f not in fieldTypes.keys()]
        if not feildslLeft: break # We're done
        for field in feildslLeft:
            data = entry[field]

            # Need data to decide
            if len(data) == 0:
                continue
            
            if data.isdigit(): # check integer
#                fieldTypes[field] = "INTEGER"
#                dtypes = int
                fieldTypes[field] = "REAL"
                dtypes = float
            else: # default to text
                try: # try real
                    temp = float(data)
                except:
                    fieldTypes[field] = "TEXT"
                    dtypes = str
                else:
                    fieldTypes[field] = "REAL"
                    dtypes = float

        # TODO: Currently there's no support for DATE in sqllite

    if len(feildslLeft) > 0:
        raise Exception("Failed to find all the columns data types - Maybe some are empty?")

    return fieldTypes, dtypes


def escapingGenerator(f):
    """from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python """
    for line in f:
        yield line.encode("ascii", "xmlcharrefreplace").decode("ascii")

def csvToDb(csvFile, con=None, table_name=None):
    from lazypandas import lazyDf
    if table_name is None:
        print('no table name found')
        table_name = 'ads'
    else:
        print('table name', table_name)

    if con is None:
        print('No con. Creating engine')
        con = sqlite3.connect(":memory:")
    tables_already_present_with_stupid_tuple = con.execute("select name from sqlite_master where type='table';").fetchall()
    tables_already_present = [x[0] for x in tables_already_present_with_stupid_tuple]
    print(tables_already_present)
    if table_name in tables_already_present:
        print('WARNING::Overwriting table_name inside db', flush=True)
        con.execute('drop table ' + table_name + ' ;')
    with open(csvFile, mode='r') as fin:
        dt, dtypes = _get_col_datatypes(fin)
        fin.seek(0)
        reader = csv.DictReader(fin)
        # Keep the order of the columns name just as in the CSV
        fields = reader.fieldnames
        cols = []
        # Set field and type
        for f in fields:
            cols.append("%s %s" % (f, dt[f]))
    df = pd.read_csv(csvFile, engine='c' ) #  dtype=dtypes
    df.to_sql(table_name, con=con)
    dotted_fields = {}
    dotted_datatype = {}
    dotted_fields[table_name] = fields
    dotted_datatype = dt # [table_name]
    ret = lazyDf([table_name], dotted_fields, dotted_datatype, RA.RA_from(table_name, fields))
    ret.add_engine(con)
    return ret


def out_to_pd(out):
    colname = [d[0] for d in out.description]
    return pd.DataFrame(data=out.fetchall(), columns=colname)

def column_equal(cols, attr):
    ret = ""
    print(cols)
    for table in cols:
        for col in cols[table][:-1]:
            ret += ' ' + col + ' = ' + str(attr) + ' and '
        ret += ' ' + cols[table][-1] + ' = ' + str(attr) + ' '
    return ret
