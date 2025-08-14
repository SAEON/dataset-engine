import logging

from psycopg2 import extras

from db import conn, curr

logger = logging.getLogger(__name__)


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
        curr.close()
        conn.close()

    def insert_records(self):
        extras.execute_values(curr, self.insert_sql, self.records_to_insert, page_size=self.batch_size)
        conn.commit()
        self.total_inserted_records += len(self.records_to_insert)
        logger.info(f"Inserted {len(self.records_to_insert)} records. Total: {self.total_inserted_records}")
        self.records_to_insert = []
