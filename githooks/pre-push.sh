#!/bin/sh

# This hooks checks if version number in several places is equal to
# the current one taken from the latest tag
#
# This hook is called with the following parameters:
#
# $1 -- Name of the remote to which the push is being done
# $2 -- URL to which the push is being done
#
# If pushing without using a named remote those arguments will be equal.
#
# Information about the commits which are being pushed is supplied as lines to
# the standard input in the form:
#
#   <local ref> <local sha1> <remote ref> <remote sha1>

last_version=$(git describe --abbrev=0)
[ -z "$last_version" ] && exit 0  # No tags was added, so nothing to check
last_version=$(echo "$last_version" | sed -r 's/^v(.+)$/\1/')

# Trigger only on master branch
read local_ref local_sha remote_ref remote_sha
[ "$remote_ref" != "refs/heads/master" ] && exit 0

grep --silent -E "^\s*version\s*=\s*[\"']${last_version}[\"']" setup.py \
  && grep --silent -E "^\s*version\s*=\s*[\"']${last_version}[\"']" pyproject.toml
if [ $? -ne 0 ]; then
  echo >&2 "Version $last_version has not been updated in setup.py or pyproject.toml"
  exit 1
fi

grep --silent "^## \[${last_version}\]" CHANGELOG.md
if [ $? -ne 0 ]; then
  echo >&2 "Changes of version $last_version has not been added to changelog"
  exit 1
fi
