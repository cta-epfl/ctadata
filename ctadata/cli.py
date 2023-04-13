import logging
import os
import click

from .api import APIClient


logger = logging.getLogger(__name__)

@click.group
@click.pass_context
def cli(ctx):
    ctx.obj['api'] = APIClient()
    ctx.obj['api'].token = os.getenv("JUPYTERHUB_API_TOKEN")
    ctx.obj['api'].downloadservice = os.getenv("CTADS_URL", "http://hub:5000/services/downloadservice/")
    logging.basicConfig(level='INFO')


@cli.command("list")
@click.pass_context
@click.argument("path", type=str)
def list_path(ctx, path):
    r = ctx.obj['api'].list_dir(path)

    if isinstance(r, list):
        for fn in r:
            click.echo(fn)
    else:
        logger.warning("problem listing files: %s", r)


@cli.command("get")
@click.pass_context
@click.argument("path", type=str)
def get_path(ctx, path):
    ctx.obj['api'].fetch_and_save_file(path)


@cli.command("put")
@click.pass_context
@click.argument("file", type=click.Path(exists=True))
@click.argument("path", type=str)
def put_path(ctx, file, path):
    ctx.obj['api'].upload_file(file, path)


def main():
    cli(obj={})