#!/usr/bin/env bash

# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"
# Need this for relative symlinks.
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done
SAVED="`pwd`"
cd "`dirname \"$PRG\"`/.." >&-
POC_HOME="`pwd -P`"
cd "$SAVED" >&-

echo $POC_HOME
DATA_HOME=${POC_HOME}/data

export CLASSPATH=

mkdir -p ${DATA_HOME}/${1}/locator > /dev/null 2>&1

echo Starting locator using properties file $1

gfsh start locator --name=locator_${1}  --enable-cluster-configuration=false --port=${2} --initial-heap=128m --max-heap=128m --dir=${DATA_HOME}/${1}/locator --properties-file=${POC_HOME}/config/${1}.properties
