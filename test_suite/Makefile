# Makefile
#
testdirs:= test1

SHELL := /bin/bash

default: title all
all:
	for d in $(testdirs); do cd $$d; $(MAKE) ; [[ $$? != 0 ]] && exit -1; echo 'done'; cd ..; done
clean:
	for d in $(testdirs); do cd $$d; $(MAKE) clean; cd ..; done

title :
	@echo "                "
	@echo "################"
	@echo "## Test-suite ##"
	@echo "################"
	@echo "                "
