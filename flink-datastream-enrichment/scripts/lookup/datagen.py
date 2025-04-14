import argparse
import psycopg2
from faker import Faker
from datetime import datetime
import logging


class LookupDataGenerator:
    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        schema="public",
    ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema = schema
        self.faker = Faker()
        self.conn = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Connected to the database.")
        except Exception as e:
            self.logger.error(f"Error connecting to the database: {e}")
            raise

    def create_table(self):
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INTEGER NOT NULL,
                address TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_table_query)
            self.conn.commit()
            self.logger.info("Table created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
            self.conn.rollback()
            raise

    def generate_and_insert_data(self, num_records: int):
        try:
            self.cursor.execute(
                f"TRUNCATE TABLE {self.schema}.customers RESTART IDENTITY;"
            )
            self.conn.commit()

            insert_query = f"""
            INSERT INTO {self.schema}.customers (name, age, address, created_at)
            VALUES (%s, %s, %s, %s)
            """

            for i in range(1, num_records + 1):
                name = self.faker.name()
                age = self.faker.random_int(min=18, max=80)
                address = self.faker.address().replace("\n", ", ")
                created_at = datetime.now()
                self.cursor.execute(insert_query, (name, age, address, created_at))

                if i % 100 == 0 or i == num_records:
                    self.conn.commit()
                    self.logger.info(f"Inserted {i} records into the database.")

            self.logger.info("All records inserted successfully.")
        except Exception as e:
            self.logger.error(f"Error inserting data: {e}")
            self.conn.rollback()
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed.")

    def run(self, num_records: int):
        try:
            self.connect()
            self.create_table()
            self.generate_and_insert_data(num_records)
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            self.close()
            self.logger.info("Data generation process completed.")


def main():
    parser = argparse.ArgumentParser(
        description="Generate and insert customers informatino for lookup"
    )
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--dbname", required=True, help="PostgreSQL database name")
    parser.add_argument("--user", required=True, help="PostgreSQL username")
    parser.add_argument("--password", required=True, help="PostgreSQL password")
    parser.add_argument("--schema", default="public", help="PostgreSQL schema")
    parser.add_argument(
        "--num-records", type=int, required=True, help="Number of records to generate"
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    generator = LookupDataGenerator(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        schema=args.schema,
    )
    generator.run(args.num_records)


if __name__ == "__main__":
    main()
