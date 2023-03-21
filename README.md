## Installation

```bash
$ pip install 'ctadata>=0.1.12'
```

## Basic Usage

```python
for url in ctadata.list_dir("lst/DL1/20241114/v0.1/headcut"):
    if 'datacheck' not in url and '.0100' in url and '11111' in url:
        print(url)
        print("stored", ctadata.fetch_and_save_file(url))
```