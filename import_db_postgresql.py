import sqlite3
import psycopg2
import re
import datetime

# Import NetPlan -> plans
def import_plans (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    plan_id = 1
    for row in source_db_connection.execute("SELECT Name, Speed, Price, Tx, Rx FROM NetPlan"):
        plan_id = plan_id + 1
        speed_unit = re.split('(\d+)',row['Speed'])
        if len(speed_unit)>1:
            speed = int(speed_unit[1])
            unit = speed_unit[2]
        else:
            speed = 0
            unit = ""
        try:
            tx = int(row['Tx'])
        except ValueError:
            tx=0
        try:
            rx = int(row['Rx'])
        except ValueError:
            rx=0
        price = int(row['Price'])
        destination_db_cursor.execute("INSERT INTO public.plan (id, name, description, speed, unit, price, tx, rx) select %s, %s, %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.plan WHERE name=%s AND speed=%s AND unit=%s)", 
            (plan_id, row['Name'], row['Name'] + " " + str(speed) + " " + unit, int(speed), unit, int(price)*100, row['Tx'],row['Rx'], row['Name'], int(speed), unit,)
        )
        destination_db_connection.commit()

# Import GroupManger -> groupings
def import_groupings (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    for row in source_db_connection.execute("SELECT Name FROM GroupManager"):
        destination_db_cursor.execute("INSERT INTO public.grouping (name) select %s WHERE NOT EXISTS (SELECT 1 FROM public.grouping WHERE name=%s)", 
            (row['Name'],row['Name'])
        )
        destination_db_connection.commit()

# Import Servers -> server
def import_servers (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    for row in source_db_connection.execute("SELECT Location, Address, Port, User, Password FROM Servers"):
        destination_db_cursor.execute(
            """
                INSERT INTO public.server (name, location, host, port, username, password)
                    select %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (SELECT 1 FROM public.server WHERE location=%s)
            """, 
            (row['Location'], row['Location'], row['Address'], row['Port'], row['User'], row['Password'], row['Location'])
        )
        destination_db_connection.commit()

# Import client -> subscription
def import_clients (source_db_connection, destination_db_connection, client_servers=[]):
    source_db_cursor = source_db_connection.cursor()
    client_count = 0
    for row in source_db_cursor.execute("SELECT AccountName, ClientName, ServerLocation, Address, Facebook, ContactNumber, Email, DateEntry, NetPlan, AmountDue, DueDate, LatestReceipt, Status, PaymentStatus, Note, LatestBilling, SubscriptionCover, IPaddress, Balance, Password, Grouping, ConnectionType, AccountNumber, Profile, DisconnectionMode, SubscriptionMode, AuthenticationMode FROM client"):
        destination_db_cursor = destination_db_connection.cursor()
        try:
            destination_db_cursor.execute("SELECT id FROM public.subscription_status WHERE status=%s", (row["Status"],))
            tmp = destination_db_cursor.fetchone()
            status_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            status_id=0

        try:
            print(row["Profile"])
            destination_db_cursor.execute("SELECT id FROM public.plan WHERE name ILIKE %s", (row["Profile"],))
            tmp = destination_db_cursor.fetchone()
            plan_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            plan_id=0

        try:
            destination_db_cursor.execute("SELECT id FROM public.subscription_mode WHERE name=%s", (row["SubscriptionMode"],))
            tmp = destination_db_cursor.fetchone()
            subscription_mode_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            subscription_mode_id=0

        try:
            destination_db_cursor.execute("SELECT id FROM public.disconnection_mode WHERE name=%s", (row["DisconnectionMode"],))
            tmp = destination_db_cursor.fetchone()
            disconnection_mode_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            disconnection_mode_id=0

        try:
            destination_db_cursor.execute("SELECT id FROM public.connection_type WHERE name=%s", (row["ConnectionType"],))
            tmp = destination_db_cursor.fetchone()
            connection_type_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            connection_type_id=0

        destination_db_cursor.close()
        if client_servers and row["ServerLocation"] not in client_servers:
            continue

        try:
            date_entry = datetime.datetime.strptime(row['DateEntry'], '%m/%d/%Y')
        except ValueError:
            try:
                date_entry = datetime.datetime.strptime(row['DateEntry'], '%m/%d/%y')
            except ValueError:
                try:
                    date_entry = datetime.datetime.strptime(row['DateEntry'], '%m-%d-%Y')
                except ValueError:
                    try:
                        date_entry = datetime.datetime.strptime(row['DateEntry'], '%m-%d-%y')
                    except ValueError:
                        date_entry = datetime.datetime.now()
        destination_db_cursor = destination_db_connection.cursor()
        destination_db_cursor.execute(
            """
                INSERT INTO subscription (
                    status_id,
                    plan_id,
                    connection_type_id,
                    subscription_mode_id,
                    disconnection_mode_id,
                    name,
                    note,
                    server_name,
                    grouping_name,
                    registration_date,
                    activation_date
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (id) DO NOTHING
                RETURNING subscription.id
            """,
            (   
                status_id,
                plan_id,
                connection_type_id,
                subscription_mode_id,
                disconnection_mode_id,
                row['ClientName'],
                row['Note'],
                row['ServerLocation'],
                row['Grouping'],
                row['DateEntry'],
                row['DateEntry']
            )
        )
        subscription_id = destination_db_cursor.fetchone()[0]

        destination_db_cursor.execute(
            """
                INSERT INTO subscriber_detail (
                    id,
                    email,
                    mobile,
                    facebook_url,
                    address
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                RETURNING subscriber_detail.id
            """,
            (   
                subscription_id,
                row['Email'],
                row['ContactNumber'],
                row['Facebook'],
                row['Address']
            )
        )
        destination_db_cursor.execute(
            """
                INSERT INTO subscription_profile (
                    id,
                    mode,
                    username,
                    password
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                RETURNING subscription_profile.id
            """, 
            (
                subscription_id,
                row['AuthenticationMode'],
                row['AccountName'],
                row['Password']
            )
        )
        destination_db_cursor.execute(
            """
            INSERT INTO subscription_installation (
                id,
                date,
                address
            )
            VALUES (
                %s,
                %s,
                %s
            )
            RETURNING subscription_installation.id
            """, 
            (
                subscription_id,
                row['DateEntry'],
                row['Address'],
            )
        )
        client_count = client_count + 1
        
        destination_db_connection.commit()

if __name__ == '__main__':

    client_servers = [] # ['VIA VERDE', 'RIVERLANE']

    destination_db_connection = psycopg2.connect(
        host="localhost",
        database="sekyurpay_db",
        user="postgres",
        password="sekyurpay2022+")
    destination_db_cursor= destination_db_connection.cursor()

    def dict_factory(cursor, row):
        col_names = [col[0] for col in cursor.description]
        return {key: value for key, value in zip(col_names, row)}

    source_db_connection = sqlite3.connect("Data.db")
    source_db_connection.row_factory = dict_factory
    source_db_cursor = source_db_connection.cursor()

    import_servers(source_db_connection, destination_db_connection)
    import_groupings(source_db_connection, destination_db_connection)
    import_plans(source_db_connection, destination_db_connection)
    
    import_clients(source_db_connection, destination_db_connection, client_servers)
    print("Done!")
    destination_db_cursor.close()
    destination_db_connection.close()
    source_db_cursor.close()
    source_db_connection.close()