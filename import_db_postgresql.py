import sqlite3
import psycopg2
import re
import datetime
import openpyxl

# Make a regular expression
# for validating an Email
regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
# Define a function for
# for validating an Email
def check(email):
 
    # pass the regular expression
    # and the string into the fullmatch() method
    if(re.fullmatch(regex, email)):
        return True
 
    else:
        return False

# Import NetPlan -> plans
def import_plans (source_db_connection, destination_db_connection):
    source_db_cursor = source_db_connection.cursor()
    destination_db_cursor = destination_db_connection.cursor()
    plan_id = 1
    for row in source_db_connection.execute("SELECT Name, Speed, Price, Tx, Rx FROM NetPlan"):
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
        plan_id = plan_id + 1

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
    wb = openpyxl.Workbook()
    sheet = wb.active
    excel_row_header = ['AccountNumber', 'AccountName', 'ClientName', 'with ñ', 'valid mobile', 'valid mobile alt', 'valid email']
    sheet.append(excel_row_header)
    source_db_cursor = source_db_connection.cursor()
    client_count = 0
    for row in source_db_cursor.execute("SELECT AccountName, ClientName, ServerLocation, Address, Facebook, ContactNumber, Email, DateEntry, NetPlan, AmountDue, DueDate, LatestReceipt, Status, PaymentStatus, Note, LatestBilling, SubscriptionCover, IPaddress, Balance, Password, Grouping, ConnectionType, AccountNumber, Profile, DisconnectionMode, SubscriptionMode, AuthenticationMode FROM client"):
        if (row['ClientName'].count('ñ')>0):
            withNye = True
        else:
            withNye = False

        destination_db_cursor = destination_db_connection.cursor()

        try:
            destination_db_cursor.execute("SELECT id FROM public.subscription_status WHERE status=%s", (row["Status"],))
            tmp = destination_db_cursor.fetchone()
            status_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            status_id=1

        try:
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
            subscription_mode_id=1

        try:
            destination_db_cursor.execute("SELECT id FROM public.disconnection_mode WHERE name=%s", (row["DisconnectionMode"],))
            tmp = destination_db_cursor.fetchone()
            disconnection_mode_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            disconnection_mode_id=1

        try:
            destination_db_cursor.execute("SELECT id FROM public.connection_type WHERE name=%s", (row["ConnectionType"],))
            tmp = destination_db_cursor.fetchone()
            connection_type_id = tmp[0]
        except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
            connection_type_id=1

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
                    id,
                    status_id,
                    plan_id,
                    connection_type_id,
                    subscription_mode_id,
                    disconnection_mode_id,
                    name,
                    note,
                    server_name,
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
                row['AccountNumber'],
                status_id,
                plan_id,
                connection_type_id,
                subscription_mode_id,
                disconnection_mode_id,
                row['ClientName'],
                row['Note'],
                row['ServerLocation'],
                row['DateEntry'],
                row['DateEntry']
            )
        )
        contacts = re.split('; |, |/', row['ContactNumber'])
        mobileOK = True
        mobileAltOK = True
        if (len(contacts)>1):
            if (not (6<=len(contacts[0])<=15)):
                mobileOK = contacts[0]
                contacts[0] = None

            if (not (6<=len(contacts[1])<=15)):
                mobileAltOK = contacts[1]
                contacts[1] = None
        else:
            if (6<=len(row['ContactNumber'])<=15):
                mobileOK = True
                mobileAltOK = False
                contacts= [row['ContactNumber'], None]
            else:
                mobileOK = row['ContactNumber']
                mobileAltOK = False
                contacts = [None, None]
        if (check(row['Email'])):
            email = row['Email']
            emailOK = True
        else:
            email = "N/A"
            emailOK = row['Email']
        try:
            subscription_id = destination_db_cursor.fetchone()[0]
        except (TypeError):
            subscription_id = None

        if subscription_id is not None:
            destination_db_cursor.execute(
                """
                    INSERT INTO subscriber_detail (
                        id,
                        email,
                        mobile,
                        mobile_alt,
                        facebook_url,
                        address
                    )
                    VALUES (
                        %s,
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
                    email,
                    contacts[0],
                    contacts[1],
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
            destination_db_connection.commit()
        client_count = client_count + 1
        excel_row = [row['AccountNumber'], row['AccountName'], row['ClientName'], withNye, mobileOK, mobileAltOK, emailOK]
        sheet.append(excel_row)
        wb.save("import_logs.xlsx")
            

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
    import_plans(source_db_connection, destination_db_connection)
    
    import_clients(source_db_connection, destination_db_connection, client_servers)
    print("Done!")
    destination_db_cursor.close()
    destination_db_connection.close()
    source_db_cursor.close()
    source_db_connection.close()