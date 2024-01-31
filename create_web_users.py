import psycopg2
import re
import datetime

# SELECT crypt(%s, gen_salt('bf'));
def create_web_user(db_connection):
    query = """
    WITH created_user AS (
        INSERT INTO "user" (username, password, email, phone, verified)
        SELECT s.id, (SELECT crypt(s.id, gen_salt('bf'))), email, mobile, true
        FROM subscriber_detail s
        ON CONFLICT (username)
        DO NOTHING
        RETURNING "user".id as uid, username as sid
    ),
	inserted_user_subscription AS (
    INSERT INTO user_subscription (user_id, subscription_id)
        SELECT uid, sid
        FROM created_user
        ON CONFLICT (user_id, subscription_id)
        DO NOTHING
        RETURNING user_id, subscription_id
    )
    INSERT INTO user_role (user_id, role_id)
    SELECT user_id, 3
    FROM inserted_user_subscription
    ON CONFLICT (user_id, role_id)
    DO NOTHING
    RETURNING user_role,role_id
    """
    query_params = ()
if __name__ == '__main__':
    db_connection = psycopg2.connect(
        host="localhost",
        database="sekyurpay_db",
        user="postgres",
        password="sekyurpay2022+")
    db_cursor= db_connection.cursor()

    create_web_user(db_connection)

    db_cursor.close()
    db_connection.close()
    print("Done!")
