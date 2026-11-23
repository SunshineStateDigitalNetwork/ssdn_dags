#!/bin/bash

# Airflow sometimes launches with HOME unset.
# Git safe.directory entries are stored in ~/.gitconfig.
if [[ -z "${HOME}" ]]; then
    export HOME=$(getent passwd "$(whoami)" | cut -d: -f6)
fi

# Debugging output
echo "HOME=$HOME"
git config --show-origin --get-all safe.directory

repo_update () {

    local repo="$1"

    cd "$repo" || {
        echo "Unable to access repo: $repo"
        return 1
    }

    git remote update -p >/dev/null
    rc=$?

    if [[ $rc -ne 0 ]]; then
        echo "Repo path: $repo"
        return $rc
    fi

    git merge --ff-only @{u} >/dev/null
    rc=$?

    if [[ $rc -ne 0 ]]; then
        echo "Repo path: $repo"
        return $rc
    fi

    return 0
}

for repo in "$@"
do
    repo_update "$repo" || exit $?
done

exit 0