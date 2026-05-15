import argparse
import clickhouse_connect


class ClickHouseTables:
    def __init__(self, host: str, port: int, username: str, password: str):
        self.client = clickhouse_connect.get_client(
            host=host, port=port, username=username, password=password
        )

    def create_tables(self) -> None:
        self.client.command(
            """
            create table if not exists drivers(
                meeting_key UInt32,
                session_key UInt32,
                driver_number UInt8,
                name_acronym String,
                full_name String,
                team_name String,
                headshot_url String
            )
            engine = MergeTree()
            order by (meeting_key, session_key, driver_number)
            """
        )

        self.client.command(
            """
            create table if not exists car_data(
                date DateTime64(3, 'UTC'),
                driver_number UInt8,
                session_key UInt32,
                meeting_key UInt32,
                speed UInt16,
                n_gear UInt8,
                rpm UInt16,
                throttle UInt16,
                brake UInt16,
                full_name String,
                name_acronym String,
                team_name String,
                headshot_url String,
                version UInt64
            )
            engine = ReplacingMergeTree(version)
            partition by (meeting_key, session_key)
            order by (meeting_key, session_key, driver_number, date)
            """
        )

        self.client.command(
            """
            create table if not exists location(
                date DateTime64(3, 'UTC'),
                driver_number UInt8,
                session_key UInt32,
                meeting_key UInt32,
                x Int32,
                y Int32,
                z Int32,
                full_name String,
                name_acronym String,
                team_name String,
                headshot_url String,
                version UInt64
            )
            engine = ReplacingMergeTree(version)
            partition by (meeting_key, session_key)
            order by (meeting_key, session_key, driver_number, date)
            """
        )

        self.client.command(
            """
            create table if not exists position(
                date DateTime64(3, 'UTC'),
                session_key UInt32,
                meeting_key UInt32,
                driver_number UInt8,
                position UInt8,
                full_name String,
                name_acronym String,
                team_name String,
                headshot_url String,
                version UInt64
            )
            engine = ReplacingMergeTree(version)
            partition by (meeting_key, session_key)
            order by (meeting_key, session_key, driver_number, date)
            """
        )

    def drop_tables(self) -> None:
        self.client.command("drop table if exists drivers")
        self.client.command("drop table if exists car_data")
        self.client.command("drop table if exists location")
        self.client.command("drop table if exists position")


def main():
    parser = argparse.ArgumentParser(
        description=("Utility to manage ClickHouse tables for the F1 data producer. ")
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--create", action="store_true", help="Create the tables in ClickHouse."
    )
    target.add_argument(
        "--drop", action="store_true", help="Drop the tables in ClickHouse."
    )

    args = parser.parse_args()
    ch_tables = ClickHouseTables(
        host="localhost",
        port=8123,
        username="default",
        password="test123",
    )
    if args.create:
        ch_tables.create_tables()
        print("Tables created successfully.")
    elif args.drop:
        ch_tables.drop_tables()
        print("Tables dropped successfully.")


if __name__ == "__main__":
    main()
