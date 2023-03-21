import os
import logging
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

# https://daskhub.dev.ctaodc.ch/services/downloadservice/fetch/pnfs/cta.cscs.ch/lst/DL1/20230226/v0.9_calib12100/tailcut84

hubbase = "http://hub:5000"
dsbase = urljoin(hubbase, "services/downloadservice/fetch/pnfs/cta.cscs.ch/")
altdsbase = urljoin(hubbase, "fetch/pnfs/cta.cscs.ch/")


def list_dir(url):
    fullurl = urljoin(dsbase, url)
    
    r = requests.get(
                fullurl,
                params = {'token': os.getenv("JUPYTERHUB_API_TOKEN")},
            )
    try:
        return [u.replace(dsbase, "").replace(altdsbase, "") for u in r.json()['urls']]
    except Exception as e:
        return r.text[:1000]



def fetch_and_save_file(url, fn=None):
    total_wrote = 0

    if fn is None:
        fn = url.split("/")[-1]

    with open(fn, "wb") as outf:
        with requests.get(
            urljoin(dsbase, url),
            params = {'token': os.getenv("JUPYTERHUB_API_TOKEN")},
            stream=True
        ) as f:          
            f.raise_for_status()
            for r in f.iter_content(chunk_size=1024*1024):
                outf.write(r)                
                total_wrote += len(r)

    return total_wrote
        
    