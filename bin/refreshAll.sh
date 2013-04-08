#!/bin/sh

##
./refreshPanther.sh
if [ $? -ne 0 ]
then
     exit 1
fi

##
./refreshHomologene.sh
if [ $? -ne 0 ]
then
     exit 1
fi

##
./refreshEntrezResolver.sh
if [ $? -ne 0 ]
then
     exit 1
fi

##
./refreshObo.sh
if [ $? -ne 0 ] 
then
    exit 1 
fi

##
./refreshItems.sh
if [ $? -ne 0 ] 
then
    exit 1 
fi
