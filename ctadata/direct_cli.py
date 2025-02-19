import click
import logging
import os
from ctadata.direct_api import APIClient

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


@cli.command("start-agent")
@click.pass_context
def start_agent(ctx):
    ctx.obj['api'].start_agent_daemon()
    
    
@cli.command("get-token")
@click.pass_context
def get_token(ctx):
    ctx.obj['api'].init_agent()


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
