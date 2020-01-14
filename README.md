# Github repo for project 7 of CMSC 33550 W20

## Project 7: Lazy Evaluation of Python Pandas

In many cases, Pandas is used within Python environments in an ad-hoc way: users submitting pandas commands as needed. As the analysis mature, these ad-hoc programs make their way into scripts and processing pipelines. There's an opportunity to increase the performance of those pipelines if the processing 'plan' was known a priori. The goal of this project is to define what is a good format for such processing plan and then build an artifact that can extract processing plans from imperative-style Python programs.

## Initial scope:

Pandas pipelines, even in a single file or ipython notebook, can be rather complicated and intermixed with call to other libraries reading and wrinting data to and from series and dataframes. 
In this project, we wish to define a wrapper around the pandas library. It should capture access to data both in read and write and operations on data, such as group, jon, merge, selection, etc. Once the data are requested, the relevant operation should be indentified, sent to an optimizer for sequencing/transformation and then returned


## See:

https://pandas.pydata.org/pandas-docs/stable/getting_started/comparison/comparison_with_sql.html
