
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sqlalchemy as sqlA
import sqlite3
import os
import matplotlib.pyplot as plt


# In[2]:


if os.path.exists('../DB_uni_large/uni_large.db'):
    os.remove('../DB_uni_large/uni_large.db')
# engine = sqlA.create_engine('sqlite:///DB_uni_large/uni_large.db')
conn = sqlite3.connect('../DB_uni_large/uni_large.db')
c = conn.cursor()
conn.commit()
conn


# In[3]:


with conn as con:
    for inputfilename in ['../DB_uni_large/uni_definition.sql', '../DB_uni_large/uni_relation_large.sql']:
        with open(inputfilename) as file:
                instructions = file.read().replace('\n','').replace('\t',' ').split(';')
                print('Reading', inputfilename, 'Instructions:', len(instructions), flush=True)
                for i, instr in enumerate(instructions):
                    instr = instr+';'
                    con.execute(instr)


# In[4]:


with conn as con:
    out = con.execute("""
select distinct student.name, student.dept_name, course.dept_name
from student, takes, course 
where 
    student.ID = takes.ID and
    takes.course_id = course.course_id and
    course.dept_name = 'Comp. Sci.'
; """
                     )
    print((out.fetchall()))


# In[8]:


with conn as con:
    out = con.execute("""
; """
                     )
    print((out.fetchall()))


# In[9]:


with conn as con:
    out = con.execute("""
select title from course where course.dept_name = 'Comp. Sci.' and course.credits = 3
; """)
    for x in out.fetchall():
        print(x)


# In[10]:


with conn as con:
    out = con.execute("""
select student.ID, student.name, teaches.course_id, instructor.name
from instructor, teaches, student, takes
where 
    student.ID = takes.ID and
    takes.course_id = teaches.course_id and
    instructor.ID = teaches.ID and
    instructor.name = 'Luo'
    
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[11]:


with conn as con:
    out = con.execute("""
select max(salary)
from instructor
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[12]:


with conn as con:
    out = con.execute("""
select *
from instructor, teaches
where
    instructor.ID = teaches.ID
    and salary = (select max(salary) from instructor)

; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)

print()

with conn as con:
    out = con.execute("""
select *
from instructor, advisor, student
where instructor.ID = advisor.i_ID
and student.ID = advisor.s_ID
and salary = (select max(salary) from instructor)

; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[11]:


#  select instructor.name, count(*)
with conn as con:
    out = con.execute("""
select *
from instructor, advisor, student
where instructor.ID = advisor.i_ID
and student.ID = advisor.s_ID
group by instructor.ID
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[7]:


print()
with conn as con:
    out = con.execute("""
select avg(number_of_slave), min(number_of_slave), max(number_of_slave), count(number_of_slave), sum(number_of_slave)
from (
    select count(*) as number_of_slave
    from instructor, advisor, student
    where instructor.ID = advisor.i_ID
    and student.ID = advisor.s_ID
    group by instructor.ID
)
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[14]:


with conn as con:
    out = con.execute("""
select dept_name, avg(salary)
from instructor
group by dept_name
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[15]:


with conn as con:
    out = con.execute("""
select * 
from instructor natural join teaches
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[16]:


out = None
with conn as con:
    out = con.execute("""
select dept_name, avg(salary) as average_salary
from instructor
group by dept_name
; """)
label = [description[0] for description in out.description]
res = np.array(out.fetchall()).T

plt.figure(figsize=(20, 10))
plt.bar(res[0], [float(x) for x in res[1]])
plt.xlabel(label[0])
plt.ylabel(label[1])
plt.show()


# In[22]:


with conn as con:
    out = con.execute("""
select instructor.name, avg(student.tot_cred) as ave_cred_slave
from instructor, advisor, student
where instructor.ID = advisor.i_ID
and student.ID = advisor.s_ID
group by instructor.ID
order by ave_cred_slave desc
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[8]:


with conn as con:
    out = con.execute("""
select * from student
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)

