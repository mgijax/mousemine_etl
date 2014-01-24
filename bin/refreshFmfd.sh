#!/bin/sh -v

# BASE
BASEDIR=..

# Main output directory
ODIR=$BASEDIR/output/fmfd
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi

#DEBUG=-D

# Yeast
python libdump/fmfd.py -d $ODIR -o yeast $DEBUG
e=$?; if [ $e -ne 0 ]; then exit $e; fi

# ZebraFish
python libdump/fmfd.py -d $ODIR -o zebrafish $DEBUG
e=$?; if [ $e -ne 0 ]; then exit $e; fi

# Rat
python libdump/fmfd.py -d $ODIR -o rat $DEBUG
e=$?; if [ $e -ne 0 ]; then exit $e; fi

# Fly
python libdump/fmfd.py -d $ODIR -o fly $DEBUG
e=$?; if [ $e -ne 0 ]; then exit $e; fi

# Worm
python libdump/fmfd.py -d $ODIR -o worm $DEBUG
e=$?; if [ $e -ne 0 ]; then exit $e; fi

