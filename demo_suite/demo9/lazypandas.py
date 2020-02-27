from pandas import pandas as pd
import numpy as np
import sqlite3
import logging
import cProfile
import copy as cp
from util import csvToDb, out_to_pd, column_equal, get_types, managed_to_sql, update_sql_data
import RA

class lazy_pandas():
    def __init__(self):
        print('LZ Creating engine')
        self.con = sqlite3.connect(":memory:")
        
    def read_csv(self, filename, *args, **kwargs):
        table_name = (filename.split('/')[-1]).split('.')[0]
        return csvToDb(filename, con=self.con, table_name=table_name)

class lazyDf():
    def __init__(self, table_names, dotted_fields, dotted_datatype, RA, con = None):
        self.table_names = table_names
        self.dotted_fields = dotted_fields
        self.dotted_datatype = dotted_datatype
        self.con = con
        self._shape = None
        self._ndim = None
        self._index = None
        self._array = None
        self.RA = RA
        self.stack = self.RA.generate_stack()
        print('new lzpd stack', self.dotted_fields, self.stack)
    def add_engine(self, con):
        self.con = con

    # RA operator
    def __getitem__(self, attribute):
        print('caught get [', attribute, ']')
        if isinstance(attribute, lazyDf):
            print('Caught lazyDf in get item')
            return
        elif isinstance(attribute, str):    # if is (probably) a column
            return lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, 
                RA.RA_project(self, attribute), self.con)
        elif isinstance(attribute, int):    # if is (probably) a row
            if self.ndim == 1:
                return self.array[attribute]
            else:
                print('Caught int in get item with ndim > 1')
                return
        
        return
    def __setitem__(self, attribute, value):
        print('caught set []', attribute, value)
        #
        if isinstance(attribute, str):    # if is (probably) a column
            table = self.find_table(attribute)
            if table is not None: # if it is a know column
                update_sql_data(table, attribute, self.index, value, con=self.con)
            else: # if it is (?) a new column
                sqltype = get_types(str(value))
                table_name = 'auto_generated_table_' + str(attribute)
                # create new table from value with self index and add to db
                df = pd.DataFrame({'auto_index':self.index, attribute:value} ) #  dtype=dtypes
                managed_to_sql(df, table_name=table_name, con=self.con)
                # substitute self with join(self, new)
                new = lazyDf([table_name], {table_name:[attribute, 'auto_index']}, \
                             {attribute:sqltype, 'auto_index':"INTEGER"}, \
                             RA.RA_from(table_name, [attribute, 'auto_index']), self.con)
                temp = self.merge(new, right_on='auto_index', left_on='auto_index')
                self.__init__(temp.table_names, temp.dotted_fields, temp.dotted_datatype, temp.RA, temp.con)
                self.stack.select_stack.remove(table_name+'.'+'auto_index')
        elif isinstance(attribute, int):
            print('set[int]. Please use named column/Series')
            pass
        elif isinstance(attribute, lazyDf):
            print('set[lazyDf]')
            pass
    # fix
    def isnull(self):
        print('caught isnull')
        pred = ''
        for col in self.stack.select_stack[:-1]:
            pred += col + ' is null and '
        pred += self.stack.select_stack[-1] + ' is null '
        print(' pred: ', pred)
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_select( self, pred ), self.con)
        return ret
    def __eq__(self, attribute):
        print('caught eq', attribute)
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_select(self, self.stack.select_stack[0] + ' = ' + str(attribute) ), self.con)
        return ret
    def merge(self, *args, **kwargs):
        print('caught merge')
        other = args[0]
        ### add _x _y and user prefix to merge
        print('merge', self.table_names, other.table_names)
        combined_table_names = self.table_names + other.table_names
        combined_dotted_fields = {**self.dotted_fields, **other.dotted_fields}
        combined_dotted_datatype = {**self.dotted_datatype, **other.dotted_datatype}
        ret = lazyDf(combined_table_names, combined_dotted_fields, combined_dotted_datatype,
                     RA.RA_join( self, other, kwargs['right_on'], kwargs['left_on']), self.con)
        return ret
    def sum(self):
        print('caught sum')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_sum( self ), self.con)
        return ret
    def std(self):
        print('caught std')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_stdev( self ), self.con)
        return ret
    def std_pop(self):
        print('caught std')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_stdevp( self ), self.con)
        return ret
    def stdev(self):
        return self.std()
    def stdevp(self):
        return self.std_pop()
    def max(self):
        print('caught max')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_max( self ), self.con)
        return ret
    def min(self):
        print('caught min')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_min( self ), self.con)
        return ret
    def count(self):
        print('caught max')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_count( self ), self.con)
        return ret
    def avg(self):
        print('caught min')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_avg( self ), self.con)
        return ret
    def mean(self):
        return self.avg()
    def ave(self):
        return self.avg()
    def groupby(self, by):
        print('caught groupby', by)
        by = self.find_table(by) + '.' + by
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_groupby( self, by ), self.con)
        return ret
    def mean(self, *args, **kwargs):
        print('caught mean')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, 
                     RA.RA_operator( ('mean', self, args, kwargs) ), self.con)
        return ret
    def reset_index(self, *args, **kwargs):
        print('caught reset index')
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_operator( ('reset_index', self, args, kwargs) ), self.con)
        return ret
    def sort_values(self, *args, **kwargs):
        print('caught sort_values', args, kwargs)
        by = kwargs.get('by', args[0])
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype,
                     RA.RA_sort( self, by ), self.con)
        return ret
    def where(self, *args, **kwargs):
        print('caught where', args, kwargs)
        cond = kwargs.get('cond', args[0])
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, 
                     RA.RA_select( self, cond ), self.con)
        return ret
    def istable(self):
        return len(self.stack.from_select) == 1 and isinstance(self.RA, RA.RA_from)
    def dropna(self, *args, **kwargs):
        print('caught drop na subset', args, kwargs)
        if 'subset' in kwargs:
            subset = kwargs['subset']
        else:
            subset = args[0]
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, 
                     RA.RA_dropna_subset( self, subset ), self.con)
        return ret
    def drop(self, *args, **kwargs):
        print('caught drop  labels', args, kwargs)
        if 'labels' in kwargs:
            labels = kwargs['labels']
        else:
            labels = args[0]
        ret = lazyDf(self.table_names, self.dotted_fields, self.dotted_datatype, RA.RA_drop_labels( self, labels ) , self.con)
        return ret
    def apply(self, func):
        return self.to_pd().apply(func)
    ## out representation of the lazy db
    @property
    def shape(self):
        if self._shape is None:
            self._shape = self.array.shape
        return self._shape
    @property
    def ndim(self):
        if self._ndim is None:
            stack = cp.copy(self.stack)
            ncols = len(stack.select_stack)
            if ncols == 1:
                self._ndim = 1
            else:
                self._ndim = 2
        return self._ndim
    @property
    def index(self):
        if self._index is None:
            self._index = self[self.table_names[0]+'.auto_index'].array
        return self._index
    def _as_string(self, depth=0, indent=2):
        ret = 'lzdf '+str(depth)+'\n'
        if self.istable():
            curs = self.con.cursor()
            sql = "select * from %s where 1=0;" % self.RA.table_name
            curs.execute(sql)
            print([d[0] for d in curs.description])
        else:
            ret += " "*depth*indent + self.RA._as_string(depth+1) + '\n'
        return ret
    @property
    def array(self):
        if self._array is None:
            sql_np_type = {'REAL':float, 'INTEGER':int, 'TEXT':'U16'}
            out = self.execute()
            labels = [d[0] for d in out.description]
            if len(labels) == 1:
                print('1D')
#                self._array = np.array(out.fetchall(), dtype=sql_np_type[self.dotted_datatype[labels[0]]]).flatten()
                self._array = np.array(out.fetchall()).flatten()
            else:
#                dtypes = [(label, sql_np_type[self.dotted_datatype[label]]) for label in labels]
#                self._array = np.array(out.fetchall(), dtype=dtypes)
                self._array = np.array(out.fetchall())
        return self._array
    def __array__(self, dtype=float):
        return self.array
    def __repr__(self):
        return str(self.__array__())
    #    return self._as_string()
    def to_pd(self):
        query = self.generate_query()
        out = self.execute(query)
        return out_to_pd(out)
    def describe(self):
        return self.to_pd().describe()
    def info(self):
        print('lazyDataFrame')
        print('tables')
        for t in self.table_names:
            print(' ', t, ':')
            for f in self.dotted_fields[t]:
                print('   ', f , ':', self.dotted_datatype[f])
            print()
    def head(self, n=5):
        query = self.generate_query()
        query += ' limit ' + str(n) + '\n'
        out = self.execute(query)
        return out_to_pd(out)
    def tail(self, n=5):
        query = self.generate_query()
        query += ' order by auto_index desc limit ' + str(n) + '\n'
        out = self.execute(query)
        return out_to_pd(out)
    def find_table(self, attribute):
        for table_name in self.dotted_fields:
            print('R: looking for ', attribute, 'in', table_name, self.dotted_fields[table_name])
            if attribute in self.dotted_fields[table_name]:
                return table_name
        return None
    ### query related
    def execute(self, query=None):
        if query is None:
            query = self.generate_query()
        return self.con.execute(query)
    def generate_query(self):
        return self.stack.generate_query()
    
    def generate_stack(self):
        return self.stack
    