# Makefile
#
demodirs:= demo1 demo2 demo3 demo4 demo5 demo6

SHELL := /bin/bash

default: title all
all:
	for d in $(demodirs); do cd $$d; $(MAKE) ; [[ $$? != 0 ]] && exit -1; echo 'done'; cd ..; done
clean:
	for d in $(demodirs); do cd $$d; $(MAKE) clean; cd ..; done

title :
	@echo "                "
	@echo "################"
	@echo "## Demo-suite ##"
	@echo "################"
	@echo "                "
