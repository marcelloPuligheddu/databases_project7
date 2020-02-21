import RA


class DAG():
    def __init__(self):
        self.nodes = []
        self.isempty = True
    def add(self, operator):
        self.isempty = False
        assert isinstance(operator, RA.RA_operator)
        assert len(self.nodes) == 0, str(self.nodes)
        self.nodes.append(operator)
    def _as_string(self, depth=0, indent=2):
        ret = ""
        for node in self.nodes:
            if hasattr(node, '_as_string'):
                ret += node._as_string(depth+1) + '\n'
            else:
                ret += " "*depth*indent + str(node)
        return ret
        
    def __repr__(self):
        return self._as_string(depth=0, indent=2)

    def generate_stack(self, ra_stack=None):
        if ra_stack is None:
            ra_stack = RA.RA_stack()
        for node in self.nodes:
            ra_stack += node.generate_stack()
        if len(ra_stack.select_stack) == 0:
            ra_stack.select_stack = [' * ']
        return ra_stack
        
    def generate_query(self):
        ra_stack = self.generate_stack()
        return ra_stack.generate_query()
