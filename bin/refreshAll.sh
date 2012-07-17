#!/bin/sh

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
