# Client for CTA Data Download Service 

## Purpose

CTA and LST data are/will be stored at CTAO-CH data center at CSCS.
These data should be accessible for selected external users.
In addition, these files should be available within interactive analysis platform at CSCS.
This client presents an API access to these services and data.
## Installation

```bash
$ pip install 'ctadata>=0.1.14'
```

## Usage from within CTA CSCS JupyterHub platform

From within CTA CSCS JupyterHub platform, selected authorized users are able to access the "data download service", like so:

```python
import ctadata

for url in ctadata.list_dir("lst/DL1/20241114/v0.1/headcut"):
    if 'datacheck' not in url and '.0100' in url and '11111' in url:
        print("stored", ctadata.fetch_and_save_file(url)/1024/1024, "Mb")
        print("found keys", h5py.File(url.split("/")[-1]).keys())
```

## From outside (possibly another jupyterhub)

You need to get yourself a jupyterhub token, it will be used to authenticate to the download service.

If you are in the session, navigate to the hub control panel this way:

![image](https://user-images.githubusercontent.com/3909535/227050172-35318c23-c138-40cb-b6ce-d2f6e780fa06.png)

Request a token:

![image](https://user-images.githubusercontent.com/3909535/227050281-2b012c15-ab84-4d75-a961-85057440fcf4.png)

The rest is similar to the previous case:

```python
import os
os.environ["CTADS_URL"] = "DATA-DISTRIBUTING-JUPYTERHUB/services/downloadservice/"
os.environ["JUPYTERHUB_API_TOKEN"] = "INSERT-YOUR-TOKEN-HERE"

for url in ctadata.list_dir("lst/DL1/20241114/v0.1/headcut"):
    if 'datacheck' not in url and '.0100' in url and '11111' in url:
        print("stored", ctadata.fetch_and_save_file(url)/1024/1024, "Mb")
        print("found keys", h5py.File(url.split("/")[-1]).keys())
```

