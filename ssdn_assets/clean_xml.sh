#!/bin/bash

command -v xmlstarlet >/dev/null 2>&1 || { echo >&2 "I require xmlstarlet but it's not installed.  Aborting."; exit 1; }

clean_xml_file () {
  xmlstarlet tr "$dirname"/xslt/stripEmptyElements.xsl .back_up/"$1" >> "$1"
  if [[ $? -gt 0 ]]
    then echo "Cleaning failed: $1" && return 1
    else return 0
  fi
}

rewind_dir () {
  mv .back_up/"$1" "$1"
}

dirname=$(dirname "$0")
#echo "$dirname"

cd "$1" || exit
if [ -d .back_up ]
  then rm -Rf .back_up
fi
mkdir .back_up
for f in $( ls *.xml ); do
  echo "Moving & cleaning $f"
  mv "$f" .back_up/
  clean_xml_file "$f"
  if [[ $? -gt 0 ]]
    then echo "Rewinding $f" && rewind_dir "$f" && exit 1
  fi
done

rm -rf .back_up
