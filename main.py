import psycopg2
from config import config
import pandas as pd
import random


def main():
    conn = connect()

    # read the data
    dt = pd.read_csv('data/train.csv')

    ''' 
     - get the passengers list with survived and cabin
     - insert the data in the database
     - for 2 times because there are 2 databases:
        - passengers1
        - passengers2
     '''
    for i in range(2):
        # create random number between 1 and 100
        random_number = random.randint(1, 100)

        print(f"Random number generated: {random_number}")
        random_sample = dt.sample(n=random_number)[
            ['PassengerId', 'Survived', 'Cabin']]

        # turn the data into a list of tuples
        passengers_list = []
        for passenger in random_sample.itertuples():
            passengers_list.append(
                (passenger.PassengerId, passenger.Survived, passenger.Cabin))

        # insert the data into the database
        insert_data(conn, passengers_list, 'passengers' + str(i + 1))
        get_data(conn, 'passengers' + str(i + 1))

    deconnect(conn)


def insert_data(conn, passengers_list, table):
    cur = conn.cursor()
    sql = f"INSERT INTO {table} (passenger_id, survived, cabin) VALUES (%s, %s, %s)"

    # execute the INSERT statement
    cur.executemany(sql, passengers_list)

    # commit the changes to the table
    conn.commit()

    # close the communication with the PostgreSQL
    cur.close()


def get_data(conn, table):
    cur = conn.cursor()
    sql = f"SELECT * FROM {table}"

    cur.execute(sql)
    rows = cur.fetchall()
    print("The number of passengers: ", cur.rowcount)

    for row in rows:
        print(row)

    cur.close()


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def deconnect(connection):
    # close connection with postgresql
    if connection is not None:
        connection.close()
        print('Database connection closed.')


if __name__ == '__main__':
    main()
