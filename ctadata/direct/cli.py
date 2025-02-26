import click
import logging
from ctadata.direct.api import APIClient, DirectApiError
import os

logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj['api'] = APIClient()


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
@click.option("--recursive", "-r", is_flag=True)
def get_path(ctx, path, recursive):
    ctx.obj['api'].fetch_and_save_file_or_dir(path, recursive=recursive)


@cli.command("put")
@click.pass_context
@click.argument("local_path", type=click.Path(exists=True))
@click.argument("path", type=str)
@click.option("--recursive", "-r", is_flag=True)
def put_path(ctx, local_path, path, recursive):
    if os.path.isfile(local_path):
        ctx.obj['api'].upload_file(local_path, path)
    else:
        if recursive:
            ctx.obj['api'].upload_dir(local_path, path)
        else:
            logger.error("can't upload directory without --recursive flag")


@cli.command("start-agent")
@click.pass_context
@click.option("--secret", "-s", type=str)
def start_agent(ctx, secret):
    if secret:
        ctx.obj['api'].secret = secret
    ctx.obj['api'].start_agent_daemon()


@cli.command("get-token")
@click.pass_context
@click.option("--secret", "-s", type=str)
def get_token(ctx, secret):
    if secret:
        ctx.obj['api'].secret = secret
    ctx.obj['api'].init_agent()


def main():
    try:
        cli(obj={})
    except DirectApiError as e:
        print(e)


if __name__ == "__main__":
    main()
