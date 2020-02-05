from pandas import pandas as pd
import numpy as np
import csv, sqlite3
import logging
import cProfile

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

class DAG():
    def __init__(self):
        self.nodes = []
        self.edges = []
    def add(self, operator):
        self.nodes.append(operator)
    def __repr__(self):
        ret = ''
        for node in self.nodes:
            ret += str(node) + '\n'
        return ret

class RA_operator():
    def __init__(self, tokens):
        self.tokens = tokens
    def __repr__(self):
        ret = ''
        for token in self.tokens:
            if isinstance(token, lazyDf):
                ret += 'df '
            else:
                ret += str(token) + ' '
        return ret
        
        
class lazyDf():
    def __init__(self):
        self.DAG = DAG()
    def add_engine(self, con):
        self.con = con
    def __getitem__(self, attribute):
        print('caught []')
        self.DAG.add( RA_operator(('project', attribute)) )
        return self
    def __eq__(self, attribute):
        print('caught eq')
        self.DAG.add( RA_operator(('predicate', 'equal', attribute)) )
        return self
    def __repr__(self):
        ret = ' lazy Df '
        ret += str(self.con) + '\n'
        ret += str(self.DAG) + '\n'
        return ret
    def execute(self, query):
        return self.con.execute(query)

def csvToDb(csvFile, con=None, table_name=None):
    """ from https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python 
    with small changes """
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
    ret = lazyDf()
    ret.add_engine(con)
    return ret

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

class csv_reader():
    def __init__(self, con, table_name):
        self.con = con
        self.table_name = table_name
    def __call__(self, filename):
        return csvToDb(filename, con=self.con, table_name=self.table_name)

class lazy_pandas():
    def __init__(self):
        self.con = None
        self.db = None
        
    def __getattribute__(self, name, *args, **kwargs):
        base = pd
        TO_OVERRIDE = ['read_csv', 'con']
        if name in TO_OVERRIDE:
            print('over', name, args, kwargs)
            if name == 'read_csv':
                self.con = sqlite3.connect(':memory:')
                return csv_reader(con=self.con, table_name='client_info')
        else:
            ret = base.__getattribute__(name)
            return ret