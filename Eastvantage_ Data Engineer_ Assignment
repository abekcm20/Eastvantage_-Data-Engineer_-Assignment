import sqlite3
import pandas as pd

# Function to connect to SQLite3 database
def connect_to_database(database):
    conn = sqlite3.connect(database)
    return conn

# Solution using SQL
def extract_data_sql(conn):
    query = """
    SELECT c.customer_ID, c.age, i.item_name, SUM(o.Quantity) AS total_quantity
    FROM Customer c
    INNER JOIN Sales s ON c.customer_ID = s.customer_ID
    INNER JOIN OrderTable o ON s.sales_ID = o.sales_ID
    INNER JOIN items i ON o.item_ID = i.item_ID
    WHERE c.age BETWEEN 18 AND 35 AND o.Quantity IS NOT NULL
    GROUP BY c.customer_ID, i.item_name
    ORDER BY c.customer_ID
    """
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    return rows

# Solution using Pandas
def extract_data_pandas(conn):
    query = """
    SELECT c.customer_ID, c.age, i.item_name, o.Quantity
    FROM Customer c
    INNER JOIN Sales s ON c.customer_ID = s.customer_ID
    INNER JOIN OrderTable o ON s.sales_ID = o.sales_ID
    INNER JOIN items i ON o.item_ID = i.item_ID
    WHERE c.age BETWEEN 18 AND 35 AND o.Quantity IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    df['Quantity'] = df['Quantity'].astype(int)
    grouped = df.groupby(['customer_ID', 'age', 'item_name']).sum().reset_index()
    return grouped.values.tolist()

# Function to store data to CSV
def store_to_csv(data, filename):
    df = pd.DataFrame(data, columns=['Customer', 'Age', 'Item', 'Quantity'])
    df.to_csv(filename, sep=';', index=False)

def main():
    database = 'company_xyz.db'
    conn = connect_to_database(database)

    # SQL Solution
    sql_data = extract_data_sql(conn)
    store_to_csv(sql_data, 'output_sql.csv')

    # Pandas Solution
    pandas_data = extract_data_pandas(conn)
    store_to_csv(pandas_data, 'output_pandas.csv')

    conn.close()

if __name__ == "__main__":
    main()
