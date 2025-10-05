import typer
from zeus_backend import download
from zeus_backend.ingest import cnpj as ingest_cnpj, cvm as ingest_cvm
from zeus_backend.normalize import cnpj as norm_cnpj, cvm as norm_cvm
from zeus_backend.warehouse import loader

app = typer.Typer(help="Zeus local ETL CLI")

a_ingest = typer.Typer(help="Ingest raw files → bronze")
a_normalize = typer.Typer(help="Normalize bronze → silver")
a_load = typer.Typer(help="Load silver → warehouse")
a_build = typer.Typer(help="Build gold views")
a_download = typer.Typer(help="Download raw datasets")

app.add_typer(a_ingest, name="ingest")
app.add_typer(a_normalize, name="normalize")
app.add_typer(a_load, name="load")
app.add_typer(a_build, name="build")
app.add_typer(a_download, name="download")

@a_ingest.command("cnpj")
def ingest_cnpj_cmd(to: str = typer.Option("bronze")):
    ingest_cnpj.run(to)

@a_ingest.command("cvm")
def ingest_cvm_cmd(to: str = typer.Option("bronze")):
    ingest_cvm.run(to)

@a_normalize.command("cnpj")
def normalize_cnpj_cmd():
    norm_cnpj.run()

@a_normalize.command("cvm")
def normalize_cvm_cmd():
    norm_cvm.run()

@a_load.command("sqlite")
def load_sqlite_cmd(db_path: str = "data/warehouse.sqlite"):
    loader.load_sqlite(db_path)

@a_build.command("gold")
def build_gold_cmd():
    typer.echo("TODO: materialize gold views with DuckDB into data/gold/")


@a_download.command("cnpj")
def download_cnpj_cmd(
    month: str | None = typer.Option(
        None,
        "--month",
        "-m",
        help="Target YYYY-MM directory; defaults to the most recent available.",
    ),
    pattern: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--pattern",
        "-p",
        help="Optional fnmatch-style pattern(s) to filter remote filenames.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        "-n",
        help="Maximum number of files to download after filtering.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Re-download files even if they already exist locally.",
    ),
):
    download.download_cnpj(month=month, patterns=pattern, limit=limit, overwrite=overwrite)


@a_download.command("cvm")
def download_cvm_cmd(
    doc: str = typer.Option(
        "itr",
        "--doc",
        "-d",
        help="Which CVM document set to pull (itr, dfp, ...).",
    ),
    pattern: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--pattern",
        "-p",
        help="Optional fnmatch-style pattern(s) to filter remote filenames.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        "-n",
        help="Maximum number of files to download after filtering.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Re-download files even if they already exist locally.",
    ),
):
    download.download_cvm(doc=doc, patterns=pattern, limit=limit, overwrite=overwrite)

if __name__ == "__main__":
    app()
