import click
import logging
from ctadata.api import APIClient, DirectApiError, ClientSecretNotFound
import os

logger = logging.getLogger(__name__)


@click.group()
@click.option("--dev", "-d", is_flag=True, help="use dev instance")
@click.pass_context
def cli(ctx, dev):
    ctx.obj['api'] = APIClient(dev_instance=dev)


@cli.command("list", help="list contents of a directory")
@click.pass_context
@click.argument("path", type=str)
def list_path(ctx, path):
    r = ctx.obj['api'].list_dir(path)

    if isinstance(r, list):
        for fn in r:
            click.echo(fn)
    else:
        logger.warning("problem listing files: %s", r)


@cli.command("get", help="download file or directory")
@click.pass_context
@click.argument("path", type=str)
@click.option("--recursive", "-r", is_flag=True)
def get_path(ctx, path, recursive):
    ctx.obj['api'].fetch_and_save_file_or_dir(path, recursive=recursive)


@cli.command("put", help="upload file or directory")
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


@cli.command("start-agent", help="start token refresh agent")
@click.pass_context
@click.option("--reset_secret", "-s", is_flag=True)
def start_agent(ctx, reset_secret):
    if not reset_secret:
        try:
            ctx.obj['api'].secret
        except ClientSecretNotFound:
            reset_secret = True
    if reset_secret:
        ctx.obj['api'].secret = input('Enter client secret\n')
    ctx.obj['api'].start_agent_daemon()


@cli.command("print-token", help="print token if it is available")
@click.pass_context
def get_token(ctx):
    ctx.obj['api'].print_token()


@cli.command("stop-agent", help="stop token refresh agent")
@click.pass_context
def stop_agent(ctx):
    ctx.obj['api'].request_stop_agent()


def main():
    try:
        cli(obj={})
    except DirectApiError as e:
        print(e)


if __name__ == "__main__":
    main()
