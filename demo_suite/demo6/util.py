import csv

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

def csvToDb(csvFile, con=None, table_name=None):
    """ from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python 
    with small changes """
    from lazypandas import lazyDf
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
            print('No con. Creating engine')
            con = sqlite3.connect(":memory:")
        cur = con.cursor()
        cur.execute(stmt)
        fin.seek(0)
        reader = csv.reader(escapingGenerator(fin))
        # Generate insert statement:
        print('insert table', table_name, con)
        stmt = "INSERT INTO " + table_name + " VALUES(%s);" % ','.join('?' * len(cols))
        cur.executemany(stmt, reader)
        con.commit()
    ret = lazyDf()
    ret.DAG.add(('table', table_name))
    ret.add_engine(con)
    return ret

class csv_reader():
    def __init__(self, con):
        self.con = con
    def __call__(self, filename):
        pass
