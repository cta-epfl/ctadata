#!/bin/bash

db_name=kk-dcache-prod
client_secret=`cat ${HOME}/.secret`
empty_file=empty


export OIDC_AGENT=$(which oidc-agent)
eval `oidc-agent-service use`
echo >$empty_file
oidc-add $db_name --pw-file=$empty_file
if [ $? != 0 ]
then
    oidc-gen $db_name --iss https://keycloak.cta.cscs.ch/realms/master/ --client-id=dcache-cta-cscs-ch-users --redirect-url http://localhost:8282 --client-secret $client_secret --no-url-call --scope "openid profile offline_access lst dcache-dev-audience email" --pw-file=$empty_file
    if [ $? != 0 ]
    then
        exit 1
    fi
fi

oidc-token $db_name | tee ${HOME}/.cta_token

nohup ./token_refresh_loop.sh 1>token_refresh.log 2>&1 &
