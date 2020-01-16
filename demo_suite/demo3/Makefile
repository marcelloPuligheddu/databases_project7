# Makefile

include ../../make.inc

default: title all

all: run 

run:
	jupyter nbconvert --to script demo.ipynb;
	${PARA_PREFIX} ${PYT} demo.py

clean:
	- /bin/rm -rf *out *yml

title:
	@echo " "
	@echo "##################################"
	@echo "### demo3 : pandas to sqlite3  ###"
	@echo "##################################"
	@echo " "
