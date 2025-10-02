import typer
from zeus_backend.ingest import cnpj as ingest_cnpj, cvm as ingest_cvm
from zeus_backend.normalize import cnpj as norm_cnpj, cvm as norm_cvm
from zeus_backend.warehouse import loader

app = typer.Typer(help="Zeus local ETL CLI")

a_ingest = typer.Typer(help="Ingest raw files → bronze")
a_normalize = typer.Typer(help="Normalize bronze → silver")
a_load = typer.Typer(help="Load silver → warehouse")
a_build = typer.Typer(help="Build gold views")

app.add_typer(a_ingest, name="ingest")
app.add_typer(a_normalize, name="normalize")
app.add_typer(a_load, name="load")
app.add_typer(a_build, name="build")

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

if __name__ == "__main__":
    app()
