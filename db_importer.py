import sqlite3
import psycopg2
import re
import datetime

conn = psycopg2.connect(
    host="localhost",
    database="sekyurpay_db",
    user="postgres",
    password="sekyurpay2022+")
sekyur_curs = conn.cursor()

def dict_factory(cursor, row):
    col_names = [col[0] for col in cursor.description]
    return {key: value for key, value in zip(col_names, row)}

con = sqlite3.connect("Data.db")
con.row_factory = dict_factory
cur = con.cursor()


# Import NetPlan -> plans
for row in con.execute("SELECT Name, Speed, Price, Tx, Rx FROM NetPlan"):
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
    sekyur_curs.execute("INSERT INTO public.plan (name, description, speed, unit, price, tag, tx, rx) select %s, %s, %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.plan WHERE name=%s AND speed=%s AND unit=%s)", 
        (row['Name'], row['Name'], speed, unit, price ,row['Name'], row['Tx'],row['Rx'],row['Name'], speed, unit)
    )
    conn.commit()

# Import GroupManger -> groupings
for row in con.execute("SELECT GroupCode, Name FROM GroupManager"):
    sekyur_curs.execute("INSERT INTO public.grouping (name) select %s WHERE NOT EXISTS (SELECT 1 FROM public.grouping WHERE name=%s)", 
        ( row['Name'],row['Name'])
    )
    conn.commit()

# Import Servers -> server
for row in con.execute("SELECT Location, Address, Port, User, Password FROM Servers"):
    sekyur_curs.execute("INSERT INTO public.server (location, host_addr, port, \"user\", password) select %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM public.server WHERE location=%s)", 
        ( row['Location'], row['Address'], row['Port'], row['User'], row['Password'], row['Location'])
    )
    conn.commit()

# Import client -> subscription
for row in cur.execute("SELECT AccountName, ClientName, ServerLocation, Address, Facebook, ContactNumber, Email, DateEntry, NetPlan, AmountDue, DueDate, LatestReceipt, Status, PaymentStatus, Note, LatestBilling, SubscriptionCover, IPaddress, Balance, Password, Grouping, ConnectionType, AccountNumber, Profile, DisconnectionMode, SubscriptionMode, AuthenticationMode FROM client"):
    try:
        sekyur_curs.execute("SELECT id FROM public.subscription_status WHERE status=%s", (row["Status"],))
        tmp = sekyur_curs.fetchone()
        status_id = tmp[0]
    except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
        status_id=8

    try:
        print(row["Profile"])
        sekyur_curs.execute("SELECT id FROM public.plan WHERE name ILIKE %s", (row["Profile"],))
        tmp = sekyur_curs.fetchone()
        plan_id = tmp[0]
    except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
        plan_id=1

    try:
        sekyur_curs.execute("SELECT id FROM public.server WHERE location=%s", (row["ServerLocation"],))
        tmp = sekyur_curs.fetchone()
        server_id = tmp[0]
    except (TypeError, ValueError, psycopg2.ProgrammingError) as e:
        server_id=1
    try:
        sekyur_curs.execute("SELECT id FROM public.grouping WHERE name=%s", (row["Grouping"],))
        grouping_id = tmp[0]
        tmp = sekyur_curs.fetchone()
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
    sekyur_curs.execute("INSERT INTO public.subscription (email, billing_address, fb_link, cpnum_1, status_id, date_created, date_updated, reg_date, installation_address, plan_id, account_num, server_id, grouping_id, name, sub_mode, dc_mode, conn_type, password, note, acc_name) \
        values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (account_num) DO UPDATE \
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
        (   row['Email'], row['Address'],
            row['Facebook'], row['ContactNumber'],
            status_id, date_entry,
            date_entry, date_entry,
            row['Address'], plan_id,
            row['AccountNumber'], server_id,
            grouping_id, row['ClientName'],
            row['SubscriptionMode'], row['DisconnectionMode'],
            row['ConnectionType'], row['Password'],
            row['Note'], row['AccountName']
        )
    )
    
conn.commit()
sekyur_curs.close()
conn.close()