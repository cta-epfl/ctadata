import os
import logging
from urllib.parse import urljoin
import click

import requests

__version__ = ""

logger = logging.getLogger(__name__)

default_downloadservice = os.getenv("CTADS_URL", "http://hub:5000/services/downloadservice/")


fetch_endpoint = "fetch/"
data_root = "pnfs/cta.cscs.ch/"

optional_url_parts = ["services/downloadservice/"]


def get(url, token=None, downloadservice=None, stream=False):
    if token is None:
        token = os.getenv("JUPYTERHUB_API_TOKEN")

    if downloadservice is None:
        downloadservice = default_downloadservice

    fullurl = urljoin(urljoin(urljoin(downloadservice, fetch_endpoint), data_root), url)

    logger.info("full url: %s", fullurl)
    logger.debug("token: %s", token)
    
    params = {'token': token, 'ctadata_version': __version__}

    return requests.get(fullurl, params=params, stream=stream)


def list_dir(url, token=None, downloadservice=None):
    r = get(url, token, downloadservice)
    
    try:
        if downloadservice is None:
            downloadservice = default_downloadservice.replace("http://", "https://")

        urls = []
        for u in r.json()['urls']:
            u = u.replace("http://", "https://")
            u = u.replace(downloadservice + fetch_endpoint + data_root, "")
            u = u.replace(downloadservice, "")
            for optional_url_part in optional_url_parts:
                p = downloadservice.replace(optional_url_part, "")
                u = u.replace(p + fetch_endpoint + data_root, "")
                u = u.replace(p, "")                

            if u not in ['', 'fetch//', 'fetch/pnfs/']:
                urls.append(u)

        return list(sorted(set(urls)))
    
    except Exception as e:
        return r.text[:1000]



def fetch_and_save_file(url, fn=None, token=None, downloadservice=None):
    total_wrote = 0

    if fn is None:
        fn = url.split("/")[-1]

    with open(fn, "wb") as out_file:
        # with get(url, token, downloadservice, stream=True) as f:          
        f = get(url, token, downloadservice, stream=True)
        logger.info("got response %s", f)
        f.raise_for_status()
        for r in f.iter_content(chunk_size=1024*1024):
            logger.info("total wrote %s Mb", total_wrote/1024/1024)
            out_file.write(r)                
            total_wrote += len(r)

    return total_wrote
        

@click.group
def main():
    logging.basicConfig(level='INFO')

@main.command("list")
@click.argument("path", type=str)
def list_path(path):
    r = list_dir(path)

    if isinstance(r, list):
        for fn in r:
            click.echo(fn)
    else:
        logger.warning("problem listing files: %s", r)

@main.command("get")
@click.argument("path", type=str)
def get_path(path):
    fetch_and_save_file(path)