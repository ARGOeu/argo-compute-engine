#!/bin/bash

# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
# 
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
# 
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323) 

#CHECK TO SEE IF WE HAVE SERIALIZATION
SERIALZ=$(grep --only-matching --perl-regex "(?<=serialization\=).*" /etc/ar-compute-engine.conf)
echo "serialization:" $SERIALZ
if [[ $SERIALZ = "avro" ]]
then
  EXT="avro" #look for avro extension
else
  EXT="out" #look for simple text extension
fi

cd /var/lib/ar-sync/

RUN_DATE=$1
RUN_DATE_UNDER=`echo $RUN_DATE | sed 's/-/_/g'`
RUN_DATE_M1D=$(date -d "$RUN_DATE -1 day" +%Y-%m-%d)
RUN_DATE_M1D_UNDER=`echo $RUN_DATE_M1D | sed 's/-/_/g'`

mongoDBServer=$2
# Variables that control the local or cluster excecution
LOCAL_FLAG=$3
LOCAL_PATH=$4

### prepare MongoDB by cleaning the collections
echo "Delete $RUN_DATE from MongoDB"
/usr/libexec/ar-compute/lib/mongo-date-delete.py $RUN_DATE

### prepare poems
echo "Prepare poems for $RUN_DATE"
POEM_FILE=poem_sync_$RUN_DATE_UNDER.$EXT
if [ -e $POEM_FILE ] 
then
  echo "Found Poem file for date $RUN_DATE"
else
  echo "Could not locate a valid Poem file for date $RUN_DATE"
  POEM_FILE=poem_sync_$RUN_DATE_M1D_UNDER.$EXT
  echo "Trying to use Poem file for date $RUN_DATE_M1D"
  if [ -e $POEM_FILE ]
  then
    echo "Found Poem file for date $RUN_DATE_M1D"
  else
    echo "Could not locate a valid Poem file"
    exit 1
  fi
fi

# If avro serialization is enabled select the avro file and decode it 
if [[ $SERIALZ = "avro" ]]
then
  /usr/libexec/ar-compute/lib/avro/poem_decode.py $POEM_FILE
  POEM_FILE=$POEM_FILE.dec
  cat $POEM_FILE \
    | sort -u \
    | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' \
    | gzip -c \
    | base64 \
    | awk 'BEGIN {ORS=""} {print $0}' \
    > poem_sync_$RUN_DATE_UNDER.zip

 else
  cat $POEM_FILE \
    | cut -d $(echo -e '\x01') --output-delimiter=$(echo -e '\x01') -f "3 4 5 6" \
    | sort -u \
    | awk 'BEGIN {ORS="|"; RS="\n"} {print $0}' \
    | gzip -c \
    | base64 \
    | awk 'BEGIN {ORS=""} {print $0}' \
    > poem_sync_$RUN_DATE_UNDER.zip

fi



### prepare topology
echo "Prepare topology for $RUN_DATE"
TOPO_FILE=sites_$RUN_DATE_UNDER.$EXT
if [ -e $TOPO_FILE ]
then
  echo "Found Topology file for date $RUN_DATE"
else
  echo "Could not locate a valid Topology file for date $RUN_DATE"
  TOPO_FILE=sites_$RUN_DATE_M1D_UNDER.$EXT
  echo "Trying to use Topology file for date $RUN_DATE_M1D"
  if [ -e $TOPO_FILE ]
  then
    echo "Found Topology file for date $RUN_DATE_M1D"
  else
    echo "Could not locate a valid Topology file"
    exit 1
  fi
fi

# If avro serialization is enabled select the avro file and decode it 
if [[ $SERIALZ = "avro" ]]
then
  /usr/libexec/ar-compute/lib/avro/sites_decode.py $TOPO_FILE
  TOPO_FILE=$TOPO_FILE.dec 
fi

cat $TOPO_FILE | sort -u > sites_$RUN_DATE_UNDER.out.clean
cat sites_$RUN_DATE_UNDER.out.clean | sed 's/\x01/ /g' | grep " SRM " | sed 's/ SRM / SRMv2 /g' | sed 's/ /\x01/g' >> sites_$RUN_DATE_UNDER.out.clean
cat sites_$RUN_DATE_UNDER.out.clean | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' | gzip -c | base64 | awk 'BEGIN {ORS=""} {print $0}' > sites_$RUN_DATE_UNDER.zip
rm -f sites_$RUN_DATE_UNDER.out.clean
split -b 30092 sites_$RUN_DATE_UNDER.zip sites_$RUN_DATE_UNDER.
rm -f sites_$RUN_DATE_UNDER.zip



### prepare downtimes


echo "Prepare downtimes for $RUN_DATE"
/usr/libexec/ar-sync/downtime-sync -d $RUN_DATE
DOWN_FILE=downtimes_$RUN_DATE.$EXT

# If avro serialization is enabled select the avro file and decode it 
if [[ $SERIALZ = "avro" ]]
then
  /usr/libexec/ar-compute/lib/avro/downtimes_decode.py $DOWN_FILE
  DOWN_FILE=$DOWN_FILE.dec 
fi

cat $DOWN_FILE \
    | sed 's/\x01/ /g' \
    | grep " SRM " \
    | sed 's/ SRM / SRMv2 /g' \
    | sed 's/ /\x01/g' \
    > downtimes_cache_$RUN_DATE.out
cat $DOWN_FILE >> downtimes_cache_$RUN_DATE.out
cat downtimes_cache_$RUN_DATE.out \
    | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' \
    | gzip -c \
    | base64 \
    | awk 'BEGIN {ORS=""} {print $0}' \
    > downtimes_$RUN_DATE.zip
rm -f downtimes_cache_$RUN_DATE.out 

### prepare weights

echo "Prepare HEPSPEC for $RUN_DATE"
HEPS_FILE=hepspec_sync_$RUN_DATE_UNDER.$EXT
if [ -e $HEPS_FILE ]
then
  echo "Found Hepspec file for date $RUN_DATE"
else
  echo "Could not locate a valid Hepspec file for date $RUN_DATE"
  HEPS_FILE=hepspec_sync_$RUN_DATE_M1D_UNDER.$EXT
  echo "Trying to use Hepspec file for date $RUN_DATE_M1D"
  if [ -e $HEPS_FILE ]
  then
    echo "Found Hepspec file for date $RUN_DATE_M1D"
  else
    echo "Could not locate a valid Hepspec file"
    exit 1
  fi
fi

# If avro serialization is enabled select the avro file and decode it
if [[ $SERIALZ = "avro" ]]
then
  /usr/libexec/ar-compute/lib/avro/hepspec_decode.py $HEPS_FILE
  HEPS_FILE=$HEPS_FILE.dec
fi


cat $HEPS_FILE \
    | awk 'BEGIN {ORS="|"; RS="\r\n"} {print $0}' \
    | gzip -c \
    | base64 \
    | awk 'BEGIN {ORS=""} {print $0}' \
    > hepspec_sync_$RUN_DATE_UNDER.zip

### run calculator.pig
pig ${LOCAL_FLAG} -useHCatalog -param in_date=$RUN_DATE \
    -param mongoServer=$mongoDBServer \
    -param weights_file=hepspec_sync_$RUN_DATE_UNDER.zip \
    -param downtimes_file=downtimes_$RUN_DATE.zip \
    -param poem_file=poem_sync_$RUN_DATE_UNDER.zip \
    -param topology_file1=sites_$RUN_DATE_UNDER.aa \
    -param topology_file2=sites_$RUN_DATE_UNDER.ab \
    -param topology_file3=sites_$RUN_DATE_UNDER.ac \
    -param input_path=/var/lib/ar-sync/prefilter_ \
    -f /usr/libexec/ar-compute/pig/${LOCAL_PATH}calculator.pig


rm -f poem_sync_$RUN_DATE_UNDER.zip 
rm -f downtimes_$RUN_DATE.zip 
rm -f hepspec_sync_$RUN_DATE_UNDER.zip 
rm -f sites_$RUN_DATE_UNDER.aa 
rm -f sites_$RUN_DATE_UNDER.ab 
rm -f sites_$RUN_DATE_UNDER.ac 
rm -f *.dec

