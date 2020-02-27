import numpy as np
import copy as cp
class RA_operator():
    def __init__(self, tokens):
        self.tokens = tokens
    def _as_string(self, depth=0, indent=2):
        ret = 'raop '+str(depth)+'\n'
        for token in self.tokens:
            if hasattr(token, '_as_string'):
                ret += token._as_string(depth+1)
            else:
                ret += " "*depth*indent + str(token) + '\n'
        return ret
    def __repr__(self):
        return self._as_string(depth=0, indent=2)
        
    def generate_query(self):
        ra_stack = self.generate_stack()
        return ra_stack.generate_query()

class RA_join(RA_operator):
    def __init__(self, right, left, left_on, right_on):
        self.right = right
        self.right_on = right_on
        self.left  = left
        self.left_on = left_on
        ra_stack = RA_stack()
        ra_stack_right = self.right.generate_stack()
        ra_stack_left = self.left.generate_stack()
        #
        stack_right_on = self.right_on
        for table_name in right.dotted_fields:
            print('R: looking for ', right_on, 'in', table_name)
            if right_on in right.dotted_fields[table_name]:
                stack_right_on = table_name + '.' + right_on
            
        ra_stack_left = self.left.generate_stack()
        stack_left_on = self.left_on
        for table_name in left.dotted_fields:
            print('L: looking for ', left_on, 'in', table_name)
            if left_on in left.dotted_fields[table_name]:
                stack_left_on = table_name + '.' + left_on
        #
        ra_stack += ra_stack_right
        ra_stack += ra_stack_left
        ra_stack.where_stack += [stack_left_on + ' = ' + stack_right_on ]
        self.stack = ra_stack
    def generate_stack(self):
        return self.stack

    def _as_string(self, depth=0, indent=2):
        ret = 'rajo '+str(depth)+'\n'
        ret += " "*depth*indent + 'Join ' + self.left_on + " " + self.right_on + '\n'
        ret += self.left._as_string(depth+1) + '\n'
        ret += self.right._as_string(depth+1) + '\n'
        return ret

class RA_from(RA_operator):
    def __init__(self, table_name, column_names):
        self.table_name = table_name
        self.column_names = column_names
        self.ra_stack = RA_stack()
        self.ra_stack.select_stack = [self.table_name+'.'+x for x in self.column_names]
        self.ra_stack.from_stack = [self.table_name]
    def _as_string(self, depth=0, indent=2):
        ret = 'rafr '+str(depth)+'\n'
        ret += " "*depth*indent + self.table_name + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack
        
class RA_project(RA_operator):
    def __init__(self, orig, col):
        self.orig = orig
        self.col = col
        print("col_name", col)
        self.ra_stack = cp.copy(self.orig.generate_stack())
        if self.ra_stack.groupby_stack == []:
            print('simple project', col)
            self.ra_stack.select_stack = [self.col]
        else:
            self.grby = self.ra_stack.groupby_stack[0] ### fix
            if self.ra_stack.aggregate_stack != []:
                self.col_renamed = self.ra_stack.aggregate_stack[0] + '(' + col + ')'
            else:
                print('empty aggr func stak', self.ra_stack.aggregate_stack)
                self.col_renamed = self.col
            print('aggr project', self.grby, self.col_renamed, self.col)
            self.ra_stack.select_stack = [self.col_renamed]
    def _as_string(self, depth=0, indent=2):
        ret = 'rapr '+str(depth)+'\n'
        ret += " "*depth*indent + self.col + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack
    
class RA_select(RA_operator):
    def __init__(self, orig, rule):
        self.orig = orig
        self.rule = rule
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.where_stack += [self.rule]
    def _as_string(self, depth=0, indent=2):
        ret = 'rase '+str(depth)+'\n'
        ret += " "*depth*indent + self.rule + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_sort(RA_operator):
    def __init__(self, orig, keys):
        self.orig  = orig
        self.keys = keys
    def _as_string(self, depth=0, indent=2):
        ret = 'raso '+str(depth)+'\n'
        ret += " "*depth*indent + 'sort on ' + self.keys + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret

class RA_groupby(RA_operator):
    def __init__(self, orig, keys):
        self.orig = orig
        self.keys = keys
        self.ra_stack = cp.deepcopy(orig.generate_stack())
        self.ra_stack.groupby_stack.append(keys)
    def _as_string(self, depth=0, indent=2):
        ret = 'ragr '+str(depth)+'\n'
        ret += " "*depth*indent + 'group by ' + self.keys + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_sum(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('sum')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'sum('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'rasu '+str(depth)+'\n'
        ret += " "*depth*indent + 'sum \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_min(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('min')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'min('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'rami '+str(depth)+'\n'
        ret += " "*depth*indent + 'min \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_max(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('max')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'max('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'rama '+str(depth)+'\n'
        ret += " "*depth*indent + 'max \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_count(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('count')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'count('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'ract '+str(depth)+'\n'
        ret += " "*depth*indent + 'count \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_avg(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('avg')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'avg('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'raav '+str(depth)+'\n'
        ret += " "*depth*indent + 'avg \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_stdev(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('stdev')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'stdev('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'rasv '+str(depth)+'\n'
        ret += " "*depth*indent + 'stdev \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_stdevp(RA_operator):
    def __init__(self, orig):
        self.orig = orig
        self.ra_stack = cp.copy(self.orig.generate_stack())
        self.ra_stack.aggregate_stack.append('stdevp')
        if len(self.ra_stack.select_stack) == 1:
            self.ra_stack.select_stack = 'stdevp('+self.ra_stack.select_stack[0]+')'
    def _as_string(self, depth=0, indent=2):
        ret = 'rasp '+str(depth)+'\n'
        ret += " "*depth*indent + 'stdevp \n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_dropna_subset(RA_operator):
    def __init__(self, orig, subset):
        self.orig  = orig
        self.subset = subset
        self.ra_stack = cp.copy(self.orig.generate_stack())
        for x in self.subset:
#            print(x, type(x), ra_stack.where_stack, type(ra_stack.where_stack))
            temp = x + " is not null"
            self.ra_stack.where_stack = self.ra_stack.where_stack + [temp]
    def _as_string(self, depth=0, indent=2):
        ret = 'radn '+str(depth)+'\n'
        ret += " "*depth*indent + 'drop na by ' + str(self.subset) + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        return self.ra_stack

class RA_drop_labels(RA_operator):
    def __init__(self, orig, labels):
        self.orig  = orig
        self.labels = labels
        
        self.ra_stack = cp.copy(self.orig.generate_stack())
        orig_table_name = ''
        dotted_labels= self.labels
        if len(self.ra_stack.from_stack) == 1:
            orig_table_name = self.ra_stack.from_stack[0]+'.'
            dotted_labels = set([orig_table_name+x for x in self.labels])
        self.ra_stack.select_stack = [x for x in self.ra_stack.select_stack if x not in dotted_labels]

    def _as_string(self, depth=0, indent=2):
        ret = 'radn '+str(depth)+'\n'
        ret += " "*depth*indent + 'drop by ' + str(self.labels) + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    
    def generate_stack(self):
        return self.ra_stack

class RA_stack():
    def __init__(self):
        print('new_stack')
        self.from_stack = []
        self.select_stack = []
        self.where_stack = []
        self.groupby_stack = []
        self.aggregate_stack = []
        print(id(self))
    def __add__(self, other):
        self.from_stack += other.from_stack
        self.select_stack += other.select_stack
        self.where_stack += other.where_stack
        self.groupby_stack += other.groupby_stack
        self.aggregate_stack += other.aggregate_stack
        self.adjust()
        return self
    def adjust(self):
        self.from_stack = unique_original_order(self.from_stack).tolist()
        self.select_stack = unique_original_order(self.select_stack).tolist()
        self.where_stack = unique_original_order(self.where_stack).tolist()
        self.groupby_stack = unique_original_order(self.groupby_stack).tolist()
        self.aggregate_stack = unique_original_order(self.aggregate_stack).tolist()
    def generate_query(self):
        self.adjust()
        ret = ''
        #
        ret += 'select '
        for x in self.select_stack[:-1]:
            ret += str(x) + ', '
        ret += str(self.select_stack[-1]) + '\n'
        #
        ret += 'from '
        for x in self.from_stack[:-1]:
            ret += str(x) + ', '
        ret += str(self.from_stack[-1]) + '\n'
        #
        if len(self.where_stack) != 0 :
            ret += 'where '
            for x in self.where_stack[:-1]:
                ret += '(' + str(x) + ') and '
            ret += '(' + str(self.where_stack[-1]) + ')\n'
        if len(self.groupby_stack) != 0 :
            ret += 'group by '
            for x in self.groupby_stack[:-1]:
                ret += '(' + str(x) + ') and '
            ret += '(' + str(self.groupby_stack[-1]) + ')\n'
        
        return ret
    def __repr__(self):
        return self.generate_query()
        
def unique_original_order(x):
    uniq, index = np.unique(x, return_index=True)
    return uniq[index.argsort()]
        
        