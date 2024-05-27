import click
import datetime
import sys
sys.path.append("src")
from ndpprep.process_data import process_dataset, process_neps_dataset
from ndpprep.decryption import decrypt_table, decrypt_shp
from ndpprep.unmask import unmask_sensitive
from ndpprep.shp_file import refresh_shapefile
from ndpprep.cli.settings import *

@click.group()
def cli():
    pass

@cli.command()
@click.argument('db')
@click.argument('ds')
@click.argument('batch', required=False, default=datetime.date.today())
@click.option('--overwrite', is_flag=True, default=False, help="Whether to overwrite existing files.")
@click.option('--secretpath', default=None, help="Path to the secret file")
def put(db, ds, batch, overwrite, secretpath):
    if secretpath is None:
        click.echo(f"Secret key not provided. Using default secret key: {DEFAULT_SECRET_PATH}")
        secretpath = DEFAULT_SECRET_PATH
    try:
        if db == "raw_neps.db":
            process_neps_dataset(db, ds, batch, secretpath, TARGET_USER, TARGET_IP, TARGET_PATH)
            click.echo(f"Data from HDFS has been saved in {db}/{ds}/{batch} on CDSW.")
        else:
            process_dataset(db, ds, batch, secretpath, TARGET_USER, TARGET_IP, TARGET_PATH)
        if overwrite:
            click.echo(f"Data from {db}/{ds}/{batch} has been put to SFTP server with overwrite.")
        else:
            click.echo(f"Data from {db}/{ds}/{batch} has been put to SFTP server.")
    except Exception as e:
        click.echo(f"Failed to put data from {db}/{ds}/{batch} to the SFTP server: {e}")

@cli.command()
@click.argument('batch', required=False, default=datetime.date.today())
@click.option('--secretpath', default=None, help="Path to the secret file")
def shp(batch, secretpath):
    if secretpath is None:
        click.echo(f"Secret key not provided. Using default secret key: {DEFAULT_SECRET_PATH}")
        secretpath = DEFAULT_SECRET_PATH
    try:
        refresh_shapefile(batch, secretpath, TARGET_USER, TARGET_IP, TARGET_PATH)
        click.echo(f"Shapefile data for {batch} from EdgeNode has been saved in {shp} on CDSW/DMZ SFTP.")
    except Exception as e:
        click.echo(f"Failed to process data: {e}")

@cli.command()
@click.argument('db')
@click.argument('ds')
@click.argument('batch', required=False, default=datetime.date.today())
@click.option('--secretpath', default=None, help="Path to the secret file")
def get(db, ds, batch, secretpath):
    if secretpath is None:
        click.echo(f"Secret key not provided. Using default secret key: {DEFAULT_SECRET_PATH}")
        secretpath = DEFAULT_SECRET_PATH
    try:
        decrypt_table(db, ds, batch, secretpath, TARGET_USER, TARGET_IP)
        click.echo(f"Data from SFTP server has been saved and decrypted {db}/{ds}/{batch} on CDSW.")
    except Exception as e:
        click.echo(f"Failed to decrypt data: {e}")

@cli.command()
@click.argument('db')
@click.argument('ds')
@click.argument('shp')
@click.option('--secretpath', default=None, help="Path to the secret file")
def getshp(db, ds, batch, secretpath):
    if secretpath is None:
        click.echo(f"Secret key not provided. Using default secret key: {DEFAULT_SECRET_PATH}")
        secretpath = DEFAULT_SECRET_PATH
    try:
        decrypt_shp(db, ds, shp, secretpath, TARGET_USER, TARGET_IP)
        click.echo(f"Data from SFTP server has been saved and decrypted {db}/{ds}/{batch} on CDSW.")
    except Exception as e:
        click.echo(f"Failed to decrypt data: {e}")

@cli.command()
@click.argument('db')
@click.argument('ds')
@click.argument('batch', required=False, default=datetime.date.today())
@click.option('--secretpath', default=None, help="Path to the secret file")
def unmask(db, ds, batch, secretpath):
    mapping_table_folder = f"/home/cdsw/PIPELINE/OUTPUT/mapping_table/{db}"
    if secretpath is None:
        click.echo(f"Secret key not provided. Using default secret key: {DEFAULT_SECRET_PATH}")
        secretpath = DEFAULT_SECRET_PATH
    try:
        decrypted_tables, decrypted_files = decrypt_table(db, ds, batch, secretpath, TARGET_USER, TARGET_IP)
        unmask_sensitive(decrypted_tables, decrypted_files, mapping_table_folder)
        click.echo(f"Data from SFTP server has been saved and unmasked {db}/{ds}/{batch} on CDSW.")
    except Exception as e:
        click.echo(f"Failed to decrypt and unmask data: {e}")

##unmask for shp not yet done
def main():
    cli()