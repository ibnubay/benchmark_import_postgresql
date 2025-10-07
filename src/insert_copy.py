import logging
import time
import zipfile
from asyncio import run
from csv import reader, writer
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
from httpx import AsyncClient, Limits, Timeout
from psycopg import AsyncConnection as ac
from psycopg.errors import Error


async def pg_connect():
    dsn = getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
    try:
        logging.info(f"Connecting to database with dsn: {dsn}")
        conn = await ac.connect(
            dsn,
            connect_timeout=60,
            autocommit=False,
            sslmode="disable",
        )

        return conn
    except Error as err:
        raise Exception(f"Error while connect database \n{repr(err)}")


async def prepare_data(file_name: str):
    await create_table()
    await download_data(file_name)
    unzip_data()


async def create_table():
    sql_file = Path(__file__).absolute().parent.joinpath("create_table.sql")
    if sql_file.exists():
        with open(sql_file, "r") as f:
            sql = f.read()
            conn = await pg_connect()
            async with conn.cursor() as cur:
                await cur.execute(sql)
                await conn.commit()
                await conn.close()


async def download_data(file_name: str):
    zip_url = f"https://excelbianalytics.com/wp/wp-content/uploads/{file_name}"
    zip_file = Path(__file__).absolute().parent.joinpath("sales.zip")

    timeout = Timeout(60.0, connect=10.0)
    limits = Limits(max_connections=10, max_keepalive_connections=5)
    async with AsyncClient(http2=False, timeout=timeout, limits=limits) as client:
        async with client.stream("GET", zip_url) as response:
            response.raise_for_status()
            with zip_file.open("wb") as f:
                async for chunk in response.aiter_bytes(1_048_576):
                    f.write(chunk)


def unzip_data():
    csv_folder = Path(__file__).absolute().parent
    zip_file = csv_folder.joinpath("sales.zip")
    csv_file = csv_folder.joinpath("sales.csv")

    with zipfile.ZipFile(str(zip_file), "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        temp_csv = zf.extract(csv_files[0], path=str(csv_folder))
        temp_path = csv_folder.joinpath(temp_csv)
        temp_path.rename(csv_file)

    zip_file.unlink(missing_ok=True)


async def read_csv():
    csv_file = Path(__file__).absolute().parent.joinpath("sales.csv")
    batch_csv_file = Path(__file__).absolute().parent.joinpath("sales_batch.csv")
    total = 0
    if csv_file.exists():
        counter = 0
        batch = []
        conn = await pg_connect()
        async with conn.cursor() as cur:
            with csv_file.open("r", newline="") as fo:
                r = reader(fo)
                header = next(r)

                for row in r:
                    batch.append(row)

                    counter += 1
                    if counter == 10000:
                        total += counter
                        create_batch_file(batch_file=batch_csv_file, header=header, rows=batch)
                        await load_data(cur, batch_csv_file)
                        await conn.commit()
                        counter = 0
                        batch.clear()
                        batch_csv_file.unlink(missing_ok=True)

            if counter > 1:
                total += counter
                create_batch_file(batch_file=batch_csv_file, header=header, rows=batch)
                await load_data(cur, batch_csv_file)
                await conn.commit()
                batch.clear()
                batch_csv_file.unlink(missing_ok=True)

    print(f"Total {total} rows inserted")


def create_batch_file(batch_file: Path, header: list, rows: list):
    with batch_file.open("w", newline="") as fo:
        w = writer(fo)
        w.writerow(header)
        w.writerows(rows)


async def load_data(cur, file_path: Path):
    batch_commit = 1024 * 1024  # 1 MB
    sql = "COPY sales FROM STDIN WITH CSV HEADER"
    with open(file_path, mode="r") as fo:
        async with cur.copy(sql) as copy:
            while data := fo.read(batch_commit):
                await copy.write(data)


async def main(file_name: str):
    await prepare_data(file_name)

    start = time.perf_counter()

    await read_csv()

    end = time.perf_counter()
    print(f"Execution time of {file_name}: {end - start:.6f} seconds")

    delete_file()
    await cleanup_data()


async def cleanup_data():
    sql = "TRUNCATE TABLE sales"
    conn = await pg_connect()
    async with conn.cursor() as cur:
        await cur.execute(sql)
        await conn.commit()


def delete_file():
    csv_file = Path(__file__).absolute().parent.joinpath("sales.csv")
    csv_file.unlink(missing_ok=True)


if __name__ == "__main__":
    load_dotenv()

    run(main("2017/07/1000-Sales-Records.zip"))
    run(main("2017/07/10000-Sales-Records.zip"))
    run(main("2017/07/100000-Sales-Records.zip"))
    run(main("2017/07/1000000%20Sales%20Records.zip"))
    run(main("2020/09/5m-Sales-Records.zip"))
