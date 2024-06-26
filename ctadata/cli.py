import click
import logging
import os
from .api import APIClient

logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj['api'] = APIClient()
    ctx.obj['api'].token = os.getenv("JUPYTERHUB_API_TOKEN")
    ctx.obj['api'].downloadservice = os.getenv(
        "CTADS_URL",
        "https://platform.cta.cscs.ch/services/downloadservice/")
    ctx.obj['api'].certificateservice = os.getenv(
        "CTACS_URL",
        "https://platform.cta.cscs.ch/services/certificateservice/")
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


@cli.command("upload-shared-certificate")
@click.pass_context
@click.argument("local_cert_path",
                type=click.Path(exists=True, dir_okay=False))
def upload_shared_certificate(ctx, local_cert_path):
    ctx.obj['api'].upload_shared_certificate(local_cert_path)


@cli.command("upload-personal-certificate")
@click.pass_context
@click.argument("local_cert_path",
                type=click.Path(exists=True, dir_okay=False))
@click.argument("certificate_key", type=str)
@click.option("-u", "--user", type=str)
def upload_personal_certificate(ctx, local_cert_path, certificate_key,
                                user=None):
    ctx.obj['api'].upload_personal_certificate(local_cert_path,
                                               certificate_key,
                                               user)


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
