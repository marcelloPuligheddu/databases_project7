# Makefile

include ../../make.inc

default: title all

all: run 

run:
	jupyter nbconvert --to script demo.ipynb;
	${PARA_PREFIX} ${PYT} demo.py > demo.out

clean:
	- /bin/rm -rf *out *yml

title:
	@echo " "
	@echo "##############################"
	@echo "### demo2 : Basic sqlite3  ###"
	@echo "##############################"
	@echo " "
