import logging
import click

from .api import list_dir, fetch_and_save_file



logger = logging.getLogger(__name__)

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