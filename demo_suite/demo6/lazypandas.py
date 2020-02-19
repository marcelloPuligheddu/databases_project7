from pandas import pandas as pd
import numpy as np
import sqlite3
import logging
import cProfile
import copy as cp
from util import csvToDb
from DAG import DAG
import RA

class lazy_pandas():
    def __init__(self):
        print('LZ Creating engine')
        self.con = sqlite3.connect(":memory:")
        
    def read_csv(self, filename, *args, **kwargs):
        return csvToDb(filename, con=self.con, table_name=filename.split('.')[0])

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
        ret.DAG.add( RA.RA_operator(('project', self, attribute)) )
        return ret
    def __setitem__(self, attribute, value):
        print('caught set []')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA.RA_operator(('add column', self, attribute, value)) )
        return ret
    def __eq__(self, attribute):
        print('caught eq')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA.RA_operator(('predicate', 'equal', self, attribute)) )
        return ret
    def merge(self, *args, **kwargs):
        print('caught merge')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA.RA_join( self, args[0], kwargs['right_on'], kwargs['left_on']) )
        return ret
    def groupby(self, *args, **kwargs):
        print('caught groupby', args, kwargs)
        ret = lazyDf(DAG(), self.con)
        by = kwargs.get('by', args[0])
        ret.DAG.add( RA.RA_groupby( self, by ) )
        return ret
    def mean(self, *args, **kwargs):
        print('caught mean')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA.RA_operator( ('mean', self, args, kwargs) ) )
        return ret
    def reset_index(self, *args, **kwargs):
        print('caught reset index')
        ret = lazyDf(DAG(), self.con)
        ret.DAG.add( RA.RA_operator( ('reset_index', self, args, kwargs) ) )
        return ret
    def sort_values(self, *args, **kwargs):
        print('caught sort_values', args, kwargs)
        ret = lazyDf(DAG(), self.con)
        by = kwargs.get('by', args[0])
        ret.DAG.add( RA.RA_sort( self, by ) )
        return ret
    def __array__(self):
        return np.zeros(4)
    def _as_string(self, depth=0, indent=2):
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
