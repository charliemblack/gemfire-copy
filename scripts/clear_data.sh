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



DATA_HOME=${POC_HOME}/data

find ${DATA_HOME} -type f -delete
mkdir -p ${DATA_HOME}/client
mkdir -p ${DATA_HOME}/locator
mkdir -p ${DATA_HOME}/server1
mkdir -p ${DATA_HOME}/server2
mkdir -p ${DATA_HOME}/server3
