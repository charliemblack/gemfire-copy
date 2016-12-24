#!/usr/bin/env bash



gfsh << ENDGFSH
connect --locator=${locatorArray[0]}[10334]
shutdown --include-locators=true --time-out=30
Y
ENDGFSH
