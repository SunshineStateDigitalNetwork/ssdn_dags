#!/bin/bash

repo_update () {
  cd $1 && git remote update -p 1> /dev/null ;
  git merge --ff-only @{u} 1> /dev/null
  if [[ $? -gt 0 ]]
    then echo "Repo path: $1" && exit 1
    else exit 0
  fi
}

for repo in "$@"
do
  repo_update "$repo"
done