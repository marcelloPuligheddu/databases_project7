from pandas import pandas as pd
import numpy as np
import sqlite3
import logging
import cProfile
import copy as cp
from util import csvToDb, out_to_pd
from DAG import DAG
import RA

class lazy_pandas():
    def __init__(self):
        print('LZ Creating engine')
        self.con = sqlite3.connect(":memory:")
        
    def read_csv(self, filename, *args, **kwargs):
        table_name = (filename.split('/')[-1]).split('.')[0]
        return csvToDb(filename, con=self.con, table_name=table_name)

class lazyDf():
    def __init__(self, table_names, dotted_fields, dotted_datatype, dag = None, con = None):
        self.table_names = table_names
        self.dotted_fields = dotted_fields
        self.dotted_datatype = dotted_datatype
        if dag is None:
            self.DAG = DAG()
        else:
            self.DAG = cp.copy(dag)
        self.con = con
        self._shape = None
        self._ndim = None
    def add_engine(self, con):
        self.con = con
    # RA operator
    def __getitem__(self, attribute):
        print('caught get []', self._ndim)
        if self._ndim == 1:
            print('1 dim. Reverting to numpy')
            return self.__array__()[attribute]
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_project(self, attribute) )
        return ret
    def __setitem__(self, attribute, value):
        print('caught set []')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_operator(('add column', self, attribute, value)) )
        return ret
    # fix
    def isnull(self):
        print('caught isnull')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        pred = ''
        
        ret.DAG.add( RA.RA_select( self, pred ) )
        return ret
    def __eq__(self, attribute):
        print('caught eq')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_select(self, attribute) )
        return ret
    def merge(self, *args, **kwargs):
        print('caught merge')
        other = args[0]
        ### add _x _y and user prefix to merge
        combined_table_names = self.table_names + other.table_names
        combined_dotted_fields = {**self.dotted_fields, **other.dotted_fields}
        combined_dotted_datatype = {**self.dotted_datatype, **other.dotted_datatype}
        ret = lazyDf(combined_table_names, combined_dotted_fields, combined_dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_join( self, other, kwargs['right_on'], kwargs['left_on']) )
        return ret
    def groupby(self, *args, **kwargs):
        print('caught groupby', args, kwargs)
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        by = kwargs.get('by', args[0])
        ret.DAG.add( RA.RA_groupby( self, by ) )
        return ret
    def mean(self, *args, **kwargs):
        print('caught mean')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_operator( ('mean', self, args, kwargs) ) )
        return ret
    def reset_index(self, *args, **kwargs):
        print('caught reset index')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_operator( ('reset_index', self, args, kwargs) ) )
        return ret
    def sort_values(self, *args, **kwargs):
        print('caught sort_values', args, kwargs)
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        by = kwargs.get('by', args[0])
        ret.DAG.add( RA.RA_sort( self, by ) )
        return ret
    def where(self, *args, **kwargs):
        print('caught where', args, kwargs)
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        cond = kwargs.get('cond', args[0])
        ret.DAG.add( RA.RA_select( self, cond ) )
        return ret
    def istable(self):
        return len(self.DAG.nodes) == 1 and isinstance(self.DAG.nodes[0], RA.RA_from)
    def dropna(self, *args, **kwargs):
        print('caught drop na subset', args, kwargs)
        if 'subset' in kwargs:
            subset = kwargs['subset']
        else:
            subset = args[0]
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_dropna_subset( self, subset ) )
        return ret
    def drop(self, *args, **kwargs):
        print('caught drop  labels', args, kwargs)
        if 'labels' in kwargs:
            labels = kwargs['labels']
        else:
            labels = args[0]
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, DAG(), self.con)
        ret.DAG.add( RA.RA_drop_labels( self, labels ) )
        return ret
    def apply(self, func):
        return self.to_pd().apply(func)
    ## out representation of the lazy db
    @property
    def shape(self):
        if self._shape is None:
            stack = self.generate_stack()
            ncols = len(stack.select_stack)
            stack.select_stack = ['count(*)']
            query = stack.generate_query()
            out = self.execute( query )
            nrows = out.fetchall()[0]
            self._shape = (ncols, nrows)
        return self._shape
    @property
    def ndim(self):
        if self._ndim is None:
            stack = self.generate_stack()
            ncols = len(stack.select_stack)
            if ncols == 1:
                self._ndim = 1
            else:
                self._ndim = 2
        return self._ndim
    def _as_string(self, depth=0, indent=2):
        ret = 'lzdf '+str(depth)+'\n'
        if self.DAG.isempty:
            ret += " "*depth*indent + 'None' + '\n'
        elif self.istable():
            curs = self.con.cursor()
            sql = "select * from %s where 1=0;" % self.DAG.nodes[0].table_name
            curs.execute(sql)
            print([d[0] for d in curs.description])
        else:
            ret += " "*depth*indent + self.DAG._as_string(depth+1) + '\n'
        return ret
    def __array__(self):
        sql_np_type = {'REAL':float, 'INTEGER':int, 'TEXT':'U16'}
        out = self.execute()
        labels = [d[0] for d in out.description]
        if len(labels) == 1:
            return np.array(out.fetchall(), dtype=sql_np_type[self.dotted_datatype[labels[0]]]).flatten()
        dtypes = [(label, sql_np_type[self.dotted_datatype[label]]) for label in labels]
        print(dtypes)
        return np.array(out.fetchall(), dtype=dtypes)
    def __repr__(self):
        return str(self.__array__())
    #    return self._as_string()
    def to_pd(self):
        query = self.generate_query()
        out = self.execute(query)
        return out_to_pd(out)
    def info(self):
        print('lazyDataFrame')
        print('tables')
        for t in self.table_names:
            print(' ', t, ':')
            for f in self.dotted_fields[t]:
                print('   ', f , ':', self.dotted_datatype[f])
            print()
    def head(self, n=10):
        query = self.generate_query()
        query += ' limit ' + str(n) + '\n'
        out = self.execute(query)
        return out_to_pd(out)
    
    ### query related
    def execute(self, query=None):
        if query is None:
            query = self.generate_query()
        return self.con.execute(query)
    def generate_stack(self):
        return self.DAG.generate_stack()
    def generate_query(self):
        return self.DAG.generate_query()

    
    