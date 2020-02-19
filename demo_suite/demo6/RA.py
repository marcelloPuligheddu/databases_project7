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
    def _as_string(self, depth=0, indent=2):
        ret = 'rajo '+str(depth)+'\n'
        ret += " "*depth*indent + 'Join ' + self.left_on + " " + self.right_on + '\n'
        ret += self.left._as_string(depth+1) + '\n'
        ret += self.right._as_string(depth+1) + '\n'
        return ret

class RA_sort(RA_operator):
    def __init__(self, data, keys):
        self.data  = data
        self.keys = keys
    def _as_string(self, depth=0, indent=2):
        ret = 'raso '+str(depth)+'\n'
        ret += " "*depth*indent + 'sort on ' + self.keys + '\n'
        ret += self.data._as_string(depth+1) + '\n'
        return ret

class RA_groupby(RA_operator):
    def __init__(self, data, keys):
        self.data  = data
        self.keys = keys
    def _as_string(self, depth=0, indent=2):
        ret = 'ragr '+str(depth)+'\n'
        ret += " "*depth*indent + 'group by ' + self.keys + '\n'
        ret += self.data._as_string(depth+1) + '\n'
        return ret
