import logging
import time
from asyncio import run
from csv import reader, writer
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
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


async def prepare_data():
    await create_table()


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


async def read_csv(file_name: str):
    csv_file = Path(__file__).absolute().parent.parent.joinpath(file_name.replace(".zip", ".csv"))
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
    await prepare_data()

    start = time.perf_counter()

    await read_csv(file_name)

    end = time.perf_counter()
    print(f"Execution time of {file_name}: {end - start:.6f} seconds")

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

    run(main("1000-Sales-Records.zip"))
    run(main("10000-Sales-Records.zip"))
    run(main("100000-Sales-Records.zip"))
    run(main("1000000-Sales-Records.zip"))
    run(main("5m-Sales-Records.zip"))
