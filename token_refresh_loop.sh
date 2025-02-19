#!/bin/bash

db_name=kk-dcache-prod
empty_file=empty

export OIDC_AGENT=$(which oidc-agent)
eval `oidc-agent-service use`
echo >$empty_file
oidc-add $db_name --pw-file=$empty_file
while true; do
    oidc-token $db_name >${HOME}/.cta_token
    sleep 10
done
