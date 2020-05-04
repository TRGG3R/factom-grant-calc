import sqlite3
from factom import Factomd

factomd = Factomd(
    host= "https://api.factomd.net/v2"
)


def create_sqlite_database():
    """Creates a SQLite3 database for storing all Factom Fblock data"""
    db_file = "fblock-scan.sqlite3"
    try:
        print(f'checking for database at {db_file}')
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        print("sqlite connection open")

        create_address_sqlite_table = """CREATE TABLE IF NOT EXISTS "address" (
                    "id"      INTEGER PRIMARY KEY,
                    "balance" INTEGER NOT NULL,
                    "adr"     TEXT NOT NULL UNIQUE,
                    "memo"    TEXT
            )"""

        create_address_transaction_sqlite_table = """CREATE TABLE IF NOT EXISTS "address_transaction" (
                    "tx_id" INT NOT NULL,  -- "transaction"."id"
                    "adr_id" INT NOT NULL, -- "address"."id"
                    "amount" INT NOT NULL, -- may be negative, if input
                    PRIMARY KEY("tx_id", "adr_id"),
                    FOREIGN KEY("tx_id") REFERENCES "transaction"("id"),
                    FOREIGN KEY("adr_id") REFERENCES "address"("id")
            )"""

        create_fblock_sqlite_table = """CREATE TABLE IF NOT EXISTS "fblock"(
                    "height" INTEGER PRIMARY KEY,
                    "timestamp" INT NOT NULL,
                    "tx_count" INT NOT NULL,
                    "ec_exchange_rate" INT NOT NULL,
                    "price" REAL, -- Denoted in USD
                    "key_mr" BLOB NOT NULL,
                    "data" BLOB NOT NULL
            )"""

        create_transaction_sqlite_table = """CREATE TABLE IF NOT EXISTS "transaction" (
                    "id"      INTEGER PRIMARY KEY,
                    "height" INT NOT NULL,    -- "fblock"."height"
                    "fb_offset" INT NOT NULL, -- index of tx data within "fblock"."data"
                    "size" INT NOT NULL,      -- length of tx data in bytes
                    "timestamp" INT NOT NULL,
                    -- amounts
                    "total_fct_in"  INT NOT NULL, -- denoted in factoshis
                    "total_fct_out" INT NOT NULL, -- denoted in factoshis
                    "total_ec_out"  INT NOT NULL, -- denoted in factoshis
                    "hash" BLOB NOT NULL, -- hash of tx ledger data
                    "memo" TEXT,
                    FOREIGN KEY("height") REFERENCES "fblock"("height")
            )"""

        idx_fblock_key_mr = """CREATE INDEX IF NOT EXISTS "idx_key_mr"
                    ON "fblock"("key_mr")"""
        idx_transaction_id = """CREATE INDEX IF NOT EXISTS "idx_transaction_id"
                    ON "transaction"("hash")"""

        c.execute(create_address_sqlite_table)
        c.execute(create_address_transaction_sqlite_table)
        c.execute(create_fblock_sqlite_table)
        c.execute(create_transaction_sqlite_table)
        c.execute(idx_fblock_key_mr)
        c.execute(idx_transaction_id)

        # Save (commit) the changes
        conn.commit()

        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        conn.close()

    except sqlite3.Error as error:
        print("Error while creating a sqlite table", error)

    finally:
        if conn:
            conn.close()
            print("sqlite connection is closed")


def fblock_count():
    """Loops through all Factom Fblock data starting at 0"""
    def sqlite_fblock_entry():
        """Parses response from Factomd fblock_by_height call and enters it into the created SQLite3 database"""
        db_file = "fblock-scan.sqlite3"
        try:
            conn = sqlite3.connect(db_file)
            c = conn.cursor()
            sqlite_insert_with_param = """INSERT INTO fblock
                                      (height, timestamp, tx_count, ec_exchange_rate, price, key_mr, data)
                                      VALUES (?, ?, ?, ?, ?, ?, ?);"""

            data_tuple = (fblock_height(), fblock_timestamp(), fblock_tx_count(), fblock_ec_exchange_rate(), fblock_price(), fblock_key_mr(), fblock_data())
            c.execute(sqlite_insert_with_param, data_tuple)
            conn.commit()

        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)

        finally:
            if conn:
                conn.close()
                print("sqlite connection is closed")

    dbheight = 0
    while True:
        fblock = factomd.factoid_block_by_height(dbheight)

        def fblock_height():
            """Returns the block height for the given fblock"""
            height = fblock['fblock']['dbheight']
            return height

        def fblock_timestamp():
            """Returns the timestamp of the first transaction in the given fblock"""
            timestamp = fblock['fblock']['transactions'][0]['millitimestamp']
            return timestamp

        def fblock_tx_count():
            """Returns the number of transactions for the given fblock"""
            tx_count = len(fblock['fblock']['transactions'])
            return tx_count

        def fblock_ec_exchange_rate():
            """Returns the exchange rate in satoshis for the given fblock"""
            ec_exchange_rate = fblock['fblock']['exchrate']
            return ec_exchange_rate

        def fblock_transactions():
            """Returns the transactions for the given fblock"""
            transactions = fblock['fblock']['transactions']
            return transactions

        def fblock_price():
            """Returns the price in USD for the given fblock
            --Note-- Currently returns None but will add in future"""
            return None

        def fblock_key_mr():
            """Returns the keymr for the given fblock"""
            key_mr = bytearray.fromhex(fblock['fblock']['keymr'])
            return key_mr

        def fblock_data():
            """Returns the raw data for the fblock"""
            data = bytearray.fromhex(fblock['rawdata'])
            return data

        sqlite_fblock_entry()
        dbheight += 1
        print(f'height: {fblock_height()} timestamp: {fblock_timestamp()} tx_count: {fblock_tx_count()} '
              f'ec_exchange_rate: {fblock_ec_exchange_rate()} price: {fblock_price()} key_mr: {fblock_key_mr()} '
              f'data: {fblock_data()}')


if __name__ == "__main__":
    create_sqlite_database()
    fblock_count()
