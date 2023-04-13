import os
from urllib.parse import urljoin
import requests
import logging
from .util import urljoin_multipart
from . import __version__


logger = logging.getLogger(__name__)

class StorageException(Exception):
    pass

# TODO: move to bravado and rest
class APIClient:
    __export_functions__ = ['list_dir', 'fetch_and_save_file', 'upload_file']
    __class_args__ = ['token', 'downloadservice', 'data_root', 'optional_url_parts', 'chunk_size']

    downloadservice = os.getenv("CTADS_URL", "http://hub:5000/services/downloadservice/")
    optional_url_parts = ["services/downloadservice/"]
    
    chunk_size = 10 * 1024 * 1024

    @property
    def token(self):
        if not hasattr(self, "_token"):
            self._token = os.getenv("JUPYTERHUB_API_TOKEN")
        
        return self._token
    
    @token.setter
    def token(self, value):
        self._token = value


    def construct_endpoint_url(self, endpoint, path):        
        # downloadservice = self.downloadservice.replace("http://", "https://")
        downloadservice = self.downloadservice

        return urljoin_multipart(downloadservice, endpoint, path)
    

    def get_endpoint(self, endpoint, path, stream=False, chunk_size=None):
        full_url = self.construct_endpoint_url(endpoint, path)
            
        logger.info("full url: %s", full_url)
        
        params = {'token': self.token, 'ctadata_version': __version__, 'chunk_size': chunk_size}

        return requests.get(full_url, params=params, stream=stream)
        

    def list_dir(self, path, token=None, downloadservice=None):
        r = self.get_endpoint('list', path)

        if r.status_code != 200:
            logger.error("error: %s", r.text)
            raise StorageException(r.text)

        
        try:
            return r.json()                        
        except Exception as e:
            logger.error("error: %s", e)
            raise


    def fetch_and_save_file(self, path, save_to_fn=None):
        total_wrote = 0

        if save_to_fn is None:
            save_to_fn = path.split("/")[-1]

        with open(save_to_fn, "wb") as out_file:
            # with get(url, token, downloadservice, stream=True) as f:          
            f = self.get_endpoint('fetch', path, stream=True, chunk_size=self.chunk_size)

            logger.info("got response %s", f)
            f.raise_for_status()
            i_chunk = 0
            
            for r in f.iter_content(chunk_size=self.chunk_size):
                logger.info("wrote %s Mb in %s chunks", total_wrote/1024/1024, i_chunk)
                out_file.write(r)                
                total_wrote += len(r)
                i_chunk += 1

        return total_wrote
    

    def upload_file(self, local_fn, path):
        url = self.construct_endpoint_url('upload', path)
        logger.info("uploading %s to %s", local_fn, url)

        with open(local_fn, "rb") as f:
            r = requests.post(url, data=f, params={'token': self.token, 'ctadata_version': __version__}, stream=True)
            logger.info("upload result: %s %s", r, r.json())

        if r.status_code != 200:
            logger.error("error: %s", r.text)
            raise StorageException(r.text)

        return r.json()

api_client = APIClient()
