github repo for project 7 of CMSC 33550 W20

Project 7: Lazy Evaluation of Python Pandas

In many cases, Pandas is used within Python environments in an ad-hoc way: 
users submitting pandas commands as needed. As the analysis mature, these 
ad-hoc programs make their way into scripts and processing pipelines. 
There's an opportunity to increase the performance of those pipelines 
if the processing 'plan' was known a priori. The goal of this project is
to define what is a good format for such processing plan and then build an artifact
that can extract processing plans from imperative-style Python programs.
