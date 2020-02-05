from pandas import pandas as pd
import numpy as np
import csv, sqlite3
import logging
import cProfile
import copy as cp

def maketabbed(func):
    def tabbed():
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            func()
        for line in output.getvalue().splitlines():
            print('\t' + line)
    return tabbed

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
        self.isempty = True
    def add(self, operator):
        self.isempty = False
        self.nodes.append(operator)
    def _as_string(self, depth=0, indent=4):
        ret = 'dag '+str(depth)+'\n'
        for node in self.nodes:
            if hasattr(node, '_as_string'):
                ret += node._as_string(depth+1) + '\n'
            else:
                ret += " "*depth*indent + str(node)
        return ret
        
    def __repr__(self):
        return self._as_string(depth=0, indent=4)

class RA_operator():
    def __init__(self, tokens):
        self.tokens = tokens
    def _as_string(self, depth=0, indent=4):
        ret = 'raop '+str(depth)+'\n'
        for token in self.tokens:
            if hasattr(token, '_as_string'):
                ret += token._as_string(depth+1)
            else:
                ret += " "*depth*indent + str(token) + '\n'
        return ret
        
    def __repr__(self):
        return self._as_string(depth=0, indent=4)

class RA_join(RA_operator):
    def __init__(self, right, left, left_on, right_on):
        self.right = right
        self.right_on = right_on
        self.left  = left
        self.left_on = left_on
    def _as_string(self, depth=0, indent=4):
        ret = 'rajo '+str(depth)+'\n'
        ret += " "*depth*indent + 'Join ' + self.left_on + " " + self.right_on + '\n'
        ret += self.left._as_string(depth+1) + '\n'
        ret += self.right._as_string(depth+1) + '\n'
        return ret
        
class lazyDf():
    def __init__(self, dag = None, con = None):
        if dag is None:
            self.DAG = DAG()
        else:
            self.DAG = cp.copy(dag)
        self.con = con
    def add_engine(self, con):
        self.con = con

    def __getitem__(self, attribute):
        print('caught get []')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator(('project', self, attribute)) )
        return ret
    def __setitem__(self, attribute, value):
        print('caught set []')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator(('add column', self, attribute, value)) )
        return ret
    def __eq__(self, attribute):
        print('caught eq')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator(('predicate', 'equal', self, attribute)) )
        return ret

    def merge(self, *args, **kwargs):
        print('caught merge')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_join( self, args[0], kwargs['right_on'], kwargs['left_on']) )
        return ret
    def groupby(self, *args, **kwargs):
        print('caught groupby', args, kwargs)
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator( ('groupby', args, kwargs, self) ) )
        return ret
    def mean(self, *args, **kwargs):
        print('caught mean')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator( ('mean', self, args, kwargs) ) )
        return ret
    def reset_index(self, *args, **kwargs):
        print('caught reset index')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator( ('reset_index', self, args, kwargs) ) )
        return ret
    def sort_values(self, *args, **kwargs):
        print('caught sort_values')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA_operator( ('sort_values', self, args, kwargs) ) )
        return ret

    def _as_string(self, depth=0, indent=4):
        ret = 'lzdf '+str(depth)+'\n'
        if self.DAG.isempty:
            ret += " "*depth*indent + 'None' + '\n'
        else:
            ret += " "*depth*indent + self.DAG._as_string(depth+1) + '\n'
        return ret
    def __repr__(self):
        return self._as_string()
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
    def __init__(self, con):
        self.con = con
    def __call__(self, filename):
        pass
        

class lazy_pandas():
    def __init__(self):
        print('LZ Creating engine')
        self.con = sqlite3.connect(":memory:")
        
    def read_csv(self, filename, *args, **kwargs):
        return csvToDb(filename, con=self.con, table_name=filename.split('.')[0])
