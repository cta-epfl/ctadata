# Client for CTA Data Download Service 

## Purpose

CTA and LST data are/will be stored at CTAO-CH data center at CSCS.
These data should be accessible for selected external users.
In addition, these files should be available within interactive analysis platform at CSCS.
This client presents an API access to these services and data.
## Installation

```bash
$ pip install 'ctadata>=0.1.12'
```

## Basic Usage

From within CTA CSCS JupyterHub platform, selected authorized users are able to access the "data download service", like so:

```python
for url in ctadata.list_dir("lst/DL1/20241114/v0.1/headcut"):
    if 'datacheck' not in url and '.0100' in url and '11111' in url:
        print("stored", ctadata.fetch_and_save_file(url)/1024/1024, "Mb")
        print("found keys", h5py.File(url.split("/")[-1]).keys())
```


## Known Issues

* directory listing shows some not useful information
  * shows also top level directories and other links, this can be misleading
  * shows some duplicates
