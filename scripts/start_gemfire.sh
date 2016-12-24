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



${POC_HOME}/scripts/start_locator.sh site_a 10334

for i in {1..2}
do
	${POC_HOME}/scripts/start_server.sh site_a ${i} &
done

wait

${POC_HOME}/scripts/start_locator.sh site_b 10344


for i in {1..2}
do
	${POC_HOME}/scripts/start_server.sh site_b ${i} &
done
wait

