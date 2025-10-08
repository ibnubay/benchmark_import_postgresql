import logging
import time
from asyncio import run
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
    csv_file = Path(__file__).absolute().parent.parent.joinpath(file_name)
    total = 0
    if csv_file.exists():
        conn = await pg_connect()
        async with conn.cursor() as cur:
            await load_data(cur, csv_file)
            await conn.commit()

    print(f"Total {total} rows inserted")


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


if __name__ == "__main__":
    load_dotenv()

    run(main("1000-Sales-Records.csv"))
    run(main("10000-Sales-Records.csv"))
    run(main("100000-Sales-Records.csv"))
    run(main("1000000-Sales-Records.csv"))
    run(main("5m-Sales-Records.csv"))
