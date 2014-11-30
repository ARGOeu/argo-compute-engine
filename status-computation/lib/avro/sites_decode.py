#!/usr/bin/env python

import sys

# avro related imports 
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

### EXTRACT SCHEMA FROM FILE

if len(sys.argv) < 2:
	print "Speficy filename arg"
	exit(1)


file_in=sys.argv[1]
file_out = file_in+".dec"

defaultOutputFileFieldFormat = '%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\r\n'

reader = DataFileReader(open(file_in, "r"), DatumReader())

outFile = open(file_out, 'w')

for i,row in enumerate(reader):
	outFile.write(defaultOutputFileFieldFormat % ( row['hostname'],row['service'],row['production'],row['monitored'],row['scope'],row['site'],row['roc'],row['infrastructure'],row['certification'],row['site_scope']))