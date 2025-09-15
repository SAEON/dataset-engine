import logging

from psycopg2 import extras
from sqlalchemy import text

from db import conn, curr, Session, Base

logger = logging.getLogger(__name__)


def create_table(attributes, table_name: str):
    table_class_name = snake_to_camel(table_name)
    dynamic_table = type(table_class_name, (Base,), attributes)
    dynamic_table.__table__.create(Session.connection(), checkfirst=True)
    Session.commit()


def table_exists(table_name: str):
    return Session.bind.dialect.has_table(Session.connection(), table_name)


def create_empty_mirrored_temp_table(
        source_table_name: str,
        temp_table_name: str
):
    create_table_sql = f"CREATE TABLE {temp_table_name} (LIKE {source_table_name} INCLUDING ALL);"
    Session.execute(text(create_table_sql))
    Session.commit()


def switch_tables(
        current_table_name: str,
        temp_table_name: str
):
    """
    This function will drop the current table and rename the temp table to the current one.
    Refer to the switch_tables function in functions.sql.
    """
    switch_tables_sql = "SELECT switch_tables(:current_table_name, :temp_table_name);"
    Session.execute(
        text(switch_tables_sql),
        {"current_table_name": current_table_name, "temp_table_name": temp_table_name}
    )
    Session.commit()


class BulkInserter:
    insert_sql = ''
    batch_size = 50000
    records_to_insert = []
    total_inserted_records = 0

    def __init__(self, insert_sql, batch_size):
        self.insert_sql = insert_sql
        self.batch_size = batch_size

    def add_record(self, record):
        self.records_to_insert.append(record)

    def insert_batch_records(self):
        if len(self.records_to_insert) >= self.batch_size:
            self.insert_records()

    def flush(self):
        self.insert_records()

    def insert_records(self):
        extras.execute_values(curr, self.insert_sql, self.records_to_insert, page_size=self.batch_size)
        conn.commit()
        self.total_inserted_records += len(self.records_to_insert)
        logger.info(f"Inserted {len(self.records_to_insert)} records. Total: {self.total_inserted_records}")
        self.records_to_insert = []


def snake_to_camel(snake_case_string):
    """
    Converts a snake_case string to camelCase.
    Useful for generating a table class name from a table name
    """
    words = snake_case_string.split('_')
    camel_case_words = [words[0]] + [word.capitalize() for word in words[1:]]
    return ''.join(camel_case_words)
