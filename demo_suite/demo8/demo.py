
# coding: utf-8

# In[1]:


from pandas import pandas as pd
import numpy as np
import random
import csv, sqlite3
import logging
import cProfile
import mplib as mp
import matplotlib.pyplot as plt


# In[2]:


"""
sname = [(np.random.randint(Ns*Ns*100), '%030x' % random.randrange(16**30), np.random.normal()+3) for x in range(Ns)]
iname = [(4111, 'X'), (111, 'Y'), (11, 'Z'), (1, 'W')]
students = pd.DataFrame(columns=('id','name','GPA'), data=sname)
instructors = pd.DataFrame(columns=('id','name') ,data=iname)
students.to_csv('student.csv')
instructors.to_csv('instructors.csv')
a = []
for si in np.random.permutation(len(sname)):
    ii = np.random.choice(len(instructors))
    a.append((students['id'][si], instructors['id'][ii]))
advises = pd.DataFrame(a, columns=('s_id', 'i_id'))
advises.to_csv('advises.csv')
"""


# In[2]:


lz = mp.lazy_pandas()


# In[3]:


students = lz.read_csv('student.csv')
instructors = lz.read_csv('instructors.csv')
advises = lz.read_csv('advises.csv')


# In[4]:


advises


# In[5]:


## 3 relations, students, instructors, advises


# In[6]:


## select i.name, mean(s.GPA)
## from s, i, a
## where s.id == a.s_id and i.id == a.i_id
## group by i.id


# In[7]:


advises_plus_gpa = advises.merge( students, left_on='s_id', right_on='id')
ave_gpa = advises_plus_gpa.groupby('i_id').mean().reset_index()
ave_gpa['avg_GPA'] = ave_gpa['GPA']
ave_gpa_sorted = ave_gpa.sort_values('avg_GPA')
ave_gpa_sorted_plus_name = ave_gpa_sorted.merge( instructors, left_on='i_id', right_on='id' )


# In[8]:


ave_gpa_sorted_plus_name


# In[9]:


plt.plot(ave_gpa_sorted_plus_name['name'], ave_gpa_sorted_plus_name['avg_GPA'])
plt.show()

