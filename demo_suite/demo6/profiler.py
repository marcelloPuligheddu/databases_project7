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