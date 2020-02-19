import numpy as np

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

class RA_join(RA_operator):
    def __init__(self, right, left, left_on, right_on):
        self.right = right
        self.right_on = right_on
        self.left  = left
        self.left_on = left_on
    def generate_stack(self):
        ra_stack = RA_stack()
        #
        ra_stack_right = self.right.generate_stack()
        if len(ra_stack_right.from_stack) == 1:
            stack_right_on = ra_stack_right.from_stack[0] + '.' + self.right_on
        else:
            stack_right_on = self.right_on
            
        ra_stack_left = self.left.generate_stack()
        if len(ra_stack_left.from_stack) == 1:
            stack_left_on = ra_stack_left.from_stack[0] + '.' + self.left_on
        else:
            stack_left_on = self.left_on
        #
        ra_stack += ra_stack_right
        ra_stack += ra_stack_left
        ra_stack.where_stack += [stack_left_on + ' = ' + stack_right_on ]
        return ra_stack

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
#        print("table_name", table_name)
#        print("cols", column_names)
    def _as_string(self, depth=0, indent=2):
        ret = 'rafr '+str(depth)+'\n'
        ret += " "*depth*indent + self.table_name + '\n'
        return ret
    def generate_stack(self):
        ra_stack = RA_stack()
#        print('STACK::::::')
#        print(self.column_names)
        ra_stack.select_stack = [self.table_name+'.'+x for x in self.column_names]
        ra_stack.from_stack = [self.table_name]
        return ra_stack
        
class RA_project(RA_operator):
    def __init__(self, orig, col):
        self.orig = orig
        self.col = col
        print("col_name", col)
    def _as_string(self, depth=0, indent=2):
        ret = 'rapr '+str(depth)+'\n'
        ret += " "*depth*indent + self.col + '\n'
        return ret
    def generate_stack(self):
        ra_stack = self.orig.generate_stack()
        ra_stack.select_stack = [self.col]
        return ra_stack
    
class RA_select(RA_operator):
    def __init__(self, orig, rule):
        self.orig = orig
        self.rule = rule
#        print("select", rule)
    def _as_string(self, depth=0, indent=2):
        ret = 'rase '+str(depth)+'\n'
        ret += " "*depth*indent + self.rule + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        ra_stack = self.orig.generate_stack()
        ra_stack.where_stack = [self.rule]
        return ra_stack

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
        self.orig  = orig
        self.keys = keys
    def _as_string(self, depth=0, indent=2):
        ret = 'ragr '+str(depth)+'\n'
        ret += " "*depth*indent + 'group by ' + self.keys + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret

class RA_dropna_subset(RA_operator):
    def __init__(self, orig, subset):
        self.orig  = orig
        self.subset = subset
    def _as_string(self, depth=0, indent=2):
        ret = 'radn '+str(depth)+'\n'
        ret += " "*depth*indent + 'drop na by ' + str(self.subset) + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    def generate_stack(self):
        ra_stack = self.orig.generate_stack()
        for x in self.subset:
#            print(x, type(x), ra_stack.where_stack, type(ra_stack.where_stack))
            temp = x + " is not null"
            ra_stack.where_stack = ra_stack.where_stack + [temp]
        return ra_stack

class RA_drop_labels(RA_operator):
    def __init__(self, orig, labels):
        self.orig  = orig
        self.labels = labels

    def _as_string(self, depth=0, indent=2):
        ret = 'radn '+str(depth)+'\n'
        ret += " "*depth*indent + 'drop by ' + str(self.labels) + '\n'
        ret += self.orig._as_string(depth+1) + '\n'
        return ret
    
    def generate_stack(self):
        ra_stack = self.orig.generate_stack()
        orig_table_name = ''
        dotted_labels= self.labels
        if len(ra_stack.from_stack) == 1:
            orig_table_name = ra_stack.from_stack[0]+'.'
            dotted_labels = set([orig_table_name+x for x in self.labels])
        
        ra_stack.select_stack = [x for x in ra_stack.select_stack if x not in dotted_labels]
        return ra_stack

class RA_stack():
    def __init__(self):
        self.from_stack = []
        self.select_stack = []
        self.where_stack = []
    def __add__(self, other):
        self.from_stack += other.from_stack
        self.select_stack += other.select_stack
        self.where_stack += other.where_stack
        self.adjust()
        return self
    def adjust(self):
        self.from_stack = unique_original_order(self.from_stack).tolist()
        self.select_stack = unique_original_order(self.select_stack).tolist()
        self.where_stack = unique_original_order(self.where_stack).tolist()
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
        return ret
    def __repr__(self):
        return self.generate_query()
        
def unique_original_order(x):
    sorted_unique, unique_reverse_index = np.unique(x , return_inverse=True)
    return sorted_unique[unique_reverse_index]
        
        