import pandas as pd
import numpy as np
import cProfile

class Profiler():
    """ ctxtM for old python without profile"""
    def __init__(self, nlines):
        self.nlines = nlines

    def __enter__(self):
        import cProfile
        self.pr = cProfile.Profile()
        self.pr.enable()

    def __exit__(self, *args):
        import pstats, io
        self.pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(self.pr, stream=s).sort_stats(sortby)
        ps.print_stats(10)
        print(s.getvalue())

## user parameters
np.random.seed(1234)
N = 2**23
minC = 4
maxC = 8

## create db
Ta = pd.DataFrame({
    'id':np.arange(N),
    'a':np.random.permutation(N)
    })
Tb = pd.DataFrame({
    'id':np.arange(N),
    'c':np.random.randint(0, N, N)
    })

## Query:
#project[id, c]
#  select[c<max]
#    join[Ta.id == Tb.id]

## pandas slow implementation
print("###### <SLOW> ########")
with Profiler(10):
    # join
    T_slow = Ta.set_index('id').join( Tb.set_index('id') )
    # select
    T_slow = T_slow[(T_slow.c<maxC)]
    # project
    T_slow = T_slow[['a', 'c']]
print("###### </SLOW> ########")

## pandas fast implementation
print("###### <FAST> ########")
with Profiler(10):
    # select 
    T_fast = Tb[(Tb.c<maxC)]
    # join
    T_fast = T_fast.set_index('id').join( Ta.set_index('id') )
    # project
    T_fast = T_fast[['a', 'c']]
print("###### </FAST> ########")

## Check
if T_fast.equals(T_slow):
    print("###### SUCCESS ######")
else:
    print()
    print("###### FAILED ######")
    print()
    print(T_slow)
    print()
    print(T_fast)







