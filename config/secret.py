import os

# Retrieve the environment variables
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
servername = os.getenv('DB_SERVERNAME')

# Construct the connection string using the environment variables
connection_string = f'mssql+pyodbc://{username}:{password}@{servername}/dreamEX'

# Print the connection string (for demonstration purposes, don't print sensitive info in real applications)
print(connection_string)

# Example function to connect to the database (replace with actual connection code)
def connect_to_database(conn_string):
    # Implement your database connection logic here
    print(f"Connecting to the database with connection string: {conn_string}")

# Call the function with the constructed connection string
connect_to_database(connection_string)
