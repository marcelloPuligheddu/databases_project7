include make.inc

all: install

install: 
	@echo "                 "
	@echo "#################"
	@echo "## < INSTALL > ##"
	@echo "#################"
	${PYT} setup.py install --user --record files.txt --prefix= 
	@echo "#################"
	@echo "## </ INSTALL > #"
	@echo "#################"

doc: 
	@echo "                 "
	@echo "#################"
	@echo "#### < DOC > ####"
	@echo "#################"
	if test -d documentation ; then ( cd documentation ; make html ) ; fi
	@echo Look at documentation/_build/html/index.html
	@echo "#################"
	@echo "#### </ DOC > ###"
	@echo "#################"

cleandoc: 
	if test -d doc ; then ( cd doc ; make clean ) ; fi

cleantest: 
	if test -d test_suite ; then ( cd test_suite ; make clean ) ; fi

test: 
	@echo "                 "
	@echo "#################"
	@echo "#### < TEST > ###"
	@echo "#################"
	if test -d test_suite ; then ( cd test_suite ; make ) ; fi
	@echo "#################"
	@echo "#### </ TEST > ##"
	@echo "#################"

clean: 
	@echo "                 "
	@echo "#################"
	@echo "## < CLEAN > ####"
	@echo "#################"
	rm -rf dist
	rm -rf build
	rm -rf files.txt
	make cleantest
	make cleandoc
	@echo "#################"
	@echo "## </ CLEAN > ###"
	@echo "#################"
