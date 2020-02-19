class DAG():
    def __init__(self):
        self.nodes = []
        self.edges = []
        self.isempty = True
    def add(self, operator):
        self.isempty = False
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

