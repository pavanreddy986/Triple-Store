import happybase

def connect_to_hbase():
    """
    Connects to HBase and returns the connection object.
    """
    try:
        connection = happybase.Connection()
        connection.open()
        print("Connected to HBase")
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        return None

def create_hbase_table(connection, table_name, column_families):
    """
    Creates a table in HBase with the given name and column families.
    """
    try:
        connection.create_table(table_name, column_families)
        print(f"Table '{table_name}' created successfully")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

def get_hbase_table(connection, table_name):
    """
    Gets a reference to the specified table in HBase.
    """
    try:
        table = connection.table(table_name)
        print(f"Got reference to HBase table '{table_name}'")
        return table
    except Exception as e:
        print(f"Error getting reference to table '{table_name}': {e}")
        return None

def insert_data_into_hbase(table, data_file):
    """
    Reads data from the given file and inserts it into the specified HBase table.
    """
    try:
        with open(data_file, "r") as tsvfile:
            index = 0
            for line in tsvfile:
                parts = line.strip().split(maxsplit=2)
                if len(parts) != 3:
                    print(f"Error: Unable to split line into 3 parts: {line}")
                    continue

                subject = parts[0].strip("<>").strip()
                predicate = parts[1].strip("<>").strip()
                obj = parts[2].strip("<>").strip()

                row_key = str(index)
                data = {
                    b'a:subject': subject.encode(),
                    b'a:predicate': predicate.encode(),
                    b'a:object': obj.encode()
                }
                table.put(row_key.encode(), data)
                index += 1
    except Exception as e:
        print(f"Error reading data file '{data_file}': {e}")

def close_hbase_connection(connection):
    """
    Closes the connection to HBase.
    """
    try:
        connection.close()
        print("Connection to HBase closed")
    except Exception as e:
        print(f"Error closing connection to HBase: {e}")

def main():
    # Connect to HBase
    connection = connect_to_hbase()
    if connection is None:
        return

    # Create HBase table if it doesn't exist
    table_name = 'yago_db'  # Replace with your actual table name
    column_families = {'a': dict()}  # Define column families
    create_hbase_table(connection, table_name, column_families)

    # Get a reference to the HBase table
    table = get_hbase_table(connection, table_name)
    if table is None:
        close_hbase_connection(connection)
        return

    # Insert data into HBase
    data_file = "/home/gowtham/Downloads/yago_full_clean.tsv"
    insert_data_into_hbase(table, data_file)

    # Close the connection to HBase
    close_hbase_connection(connection)

if __name__ == "__main__":
    main()
