import sqlite3
import psycopg2
import re
import datetime

# Import NetPlan -> plans
def import_plans (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    plan_id = 1
    destination_db_cursor.execute("INSERT INTO public.plan (id, name, description, speed, unit, price, tag, tx, rx) select %s, %s, %s, %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.plan WHERE name=%s AND speed=%s AND unit=%s)", 
        (plan_id, 'UNKNOWN', 'UNKNOWN', 0, 'Mbps', 0 ,'UNKNOWN','0','0', 'UNKNOWN', 0, 'Mbps')
    )

    destination_db_connection.commit()
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
        destination_db_cursor.execute("INSERT INTO public.plan (id, name, description, speed, unit, price, tag, tx, rx) select %s, %s, %s, %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.plan WHERE name=%s AND speed=%s AND unit=%s)", 
            (plan_id, row['Name'], row['Name'], speed, unit, int(price)*100 ,row['Name'], row['Tx'],row['Rx'],row['Name'], speed, unit)
        )
        destination_db_connection.commit()

# Import GroupManger -> groupings
def import_groupings (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    grouping_id = 1
    destination_db_cursor.execute("INSERT INTO public.grouping (id, name) select %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.grouping WHERE name=%s)", 
        (grouping_id, 'UNKNOWN', 'UNKNOWN')
    )
    destination_db_connection.commit()
    for row in source_db_connection.execute("SELECT GroupCode, Name FROM GroupManager"):
        grouping_id = grouping_id + 1
        destination_db_cursor.execute("INSERT INTO public.grouping (id, name) select %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.grouping WHERE name=%s)", 
            (grouping_id, row['Name'],row['Name'])
        )
        destination_db_connection.commit()

# Import Servers -> server
def import_servers (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    server_id = 1
    destination_db_cursor.execute("INSERT INTO public.server (id, location, host_addr, port, \"user\", password) select %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.server WHERE location=%s)", 
            (server_id, 'UNKNOWN', 'UNKNOWN', '0000', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN')
        )
    destination_db_connection.commit()
    for row in source_db_connection.execute("SELECT Location, Address, Port, User, Password FROM Servers"):
        server_id = server_id + 1
        destination_db_cursor.execute("INSERT INTO public.server (id, location, host_addr, port, \"user\", password) select %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.server WHERE location=%s)", 
            (server_id, row['Location'], row['Address'], row['Port'], row['User'], row['Password'], row['Location'])
        )
        destination_db_connection.commit()

# Import client -> subscription
def import_clients (source_db_connection, destination_db_connection, client_servers=[]):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    client_count = 0
    for row in source_db_cursor.execute("SELECT AccountName, ClientName, ServerLocation, Address, Facebook, ContactNumber, Email, DateEntry, NetPlan, AmountDue, DueDate, LatestReceipt, Status, PaymentStatus, Note, LatestBilling, SubscriptionCover, IPaddress, Balance, Password, Grouping, ConnectionType, AccountNumber, Profile, DisconnectionMode, SubscriptionMode, AuthenticationMode FROM client"):
        try:
            destination_db_cursor.execute("SELECT id FROM public.subscription_status WHERE status=%s", (row["Status"],))
            tmp = destination_db_cursor.fetchone()
            status_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            status_id=8

        try:
            print(row["Profile"])
            destination_db_cursor.execute("SELECT id FROM public.plan WHERE name ILIKE %s", (row["Profile"],))
            tmp = destination_db_cursor.fetchone()
            plan_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            plan_id=1

        if client_servers and row["ServerLocation"] not in client_servers:
            continue

        try:
            destination_db_cursor.execute("SELECT id FROM public.server WHERE location=%s", (row["ServerLocation"],))
            tmp = destination_db_cursor.fetchone()
            server_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            server_id=1

        try:
            destination_db_cursor.execute("SELECT id FROM public.grouping WHERE name=%s", (row["Grouping"],))
            grouping_id = tmp[0]
            tmp = destination_db_cursor.fetchone()
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            grouping_id=1
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
        destination_db_cursor.execute("INSERT INTO public.subscription (id, email, billing_address, fb_link, cpnum_1, status_id, date_created, date_updated, reg_date, installation_address, plan_id, server_id, grouping_id, name, sub_mode, dc_mode, conn_type, password, note, acc_name) \
            values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE \
            SET \
            email = excluded.email, \
            billing_address = excluded.billing_address, \
            fb_link = excluded.fb_link, \
            cpnum_1 = excluded.cpnum_1, \
            status_id = excluded.status_id, \
            date_created = excluded.date_created, \
            date_updated = excluded.date_updated, \
            reg_date = excluded.reg_date, \
            installation_address = excluded.installation_address, \
            plan_id = excluded.plan_id, \
            server_id = excluded.server_id, \
            grouping_id = excluded.grouping_id, \
            name = excluded.name, \
            sub_mode = excluded.sub_mode, \
            dc_mode = excluded.dc_mode, \
            conn_type = excluded.conn_type, \
            password = excluded.password, \
            note = excluded.note, \
            acc_name = excluded.acc_name", 
            (   
                row['AccountNumber'], row['Email'], row['Address'],
                row['Facebook'], row['ContactNumber'],
                status_id, date_entry,
                date_entry, date_entry,
                row['Address'], plan_id, server_id,
                grouping_id, row['ClientName'],
                row['SubscriptionMode'], row['DisconnectionMode'],
                row['ConnectionType'], row['Password'],
                row['Note'], row['AccountName']
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

    import_plans(source_db_connection, destination_db_connection)
    import_groupings(source_db_connection, destination_db_connection)
    import_servers(source_db_connection, destination_db_connection)
    import_clients(source_db_connection, destination_db_connection, client_servers)

    destination_db_cursor.close()
    destination_db_connection.close()
    source_db_cursor.close()
    source_db_connection.close()