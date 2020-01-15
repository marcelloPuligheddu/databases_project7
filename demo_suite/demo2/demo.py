
# coding: utf-8

# In[1]:


import pandas as pd
import sqlite3


# In[2]:


#get_ipython().system('rm DB_uni_large/uni_large.db')
#get_ipython().system('ls DB_uni_large/')
conn = sqlite3.connect('DB_uni_large/uni_large.db')
c = conn.cursor()
conn.commit()
conn


# In[3]:


with conn as con:
    for inputfilename in ['DB_uni_large/uni_definition.sql', 'DB_uni_large/uni_relation_large.sql']:
        with open(inputfilename) as file:
                instructions = file.read().replace('\n','').replace('\t',' ').split(';')
                print(inputfilename, len(instructions), flush=True)
                for i, instr in enumerate(instructions):
                    instr = instr+';'
                    try:
                        con.execute(instr)
                    except: #likely the database is already there
                        pass


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


# In[5]:


with conn as con:
    out = con.execute("""
; """
                     )
    print((out.fetchall()))


# In[6]:


with conn as con:
    out = con.execute("""
select title from course where course.dept_name = 'Comp. Sci.' and course.credits = 3
; """)
    for x in out.fetchall():
        print(x)


# In[7]:


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


# In[8]:


with conn as con:
    out = con.execute("""
select max(salary)
from instructor
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[21]:


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


# In[38]:


with conn as con:
    out = con.execute("""
select instructor.name, count(*) as number_of_slave
from instructor, advisor, student
where instructor.ID = advisor.i_ID
and student.ID = advisor.s_ID
group by instructor.ID
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)

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


# In[39]:


with conn as con:
    out = con.execute("""
select dept_name, avg(salary)
from instructor
group by dept_name
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)


# In[40]:


with conn as con:
    out = con.execute("""
select * 
from instructor natural join teaches
; """)
    print([description[0] for description in out.description])
    for x in out.fetchall():
        print(x)

