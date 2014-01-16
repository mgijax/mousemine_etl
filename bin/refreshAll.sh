#!/bin/sh

##
./refreshPanther.sh
e=$?; if [ $e -ne 0 ]; then exit $e; fi

##
./refreshHomologene.sh
e=$?; if [ $e -ne 0 ]; then exit $e; fi

##
./refreshEntrezResolver.sh
e=$?; if [ $e -ne 0 ]; then exit $e; fi

##
./refreshObo.sh
e=$?; if [ $e -ne 0 ]; then exit $e; fi

##
./refreshItems.sh
e=$?; if [ $e -ne 0 ]; then exit $e; fi
