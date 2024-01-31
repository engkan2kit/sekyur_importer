import sqlite3
import re
import datetime
import openpyxl

# Make a regular expression
# for validating an Email
regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
# for validating an mobile number
regex_mobile = r'\b(\+?\d{2}?\s?\d{3}\s?\d{3}\s?\d{4})|([0]\d{3}\s?\d{3}\s?\d{4})\b'

# Define a function for
# for validating an Email
def check(email):
 
    # pass the regular expression
    # and the string into the fullmatch() method
    if(re.fullmatch(regex, email)):
        return True
 
    else:
        return False

# Define a function for
# for validating an Email
def check_mobile(mobile):
 
    # pass the regular expression
    # and the string into the fullmatch() method
    if(re.fullmatch(regex_mobile, mobile)):
        return True
 
    else:
        return False



def export_emails (source_db_connection, wb):
    source_db_cursor = source_db_connection.cursor()
    client_count = 0
    sheet = wb.active

    # Once have the Worksheet object,
    # one can get its name from the
    # title attribute.
    sheet.title = "No_emails"
    sheet_title = sheet.title
    excel_row_header = ['AccountNumber', 'AccountName', 'ClientName', 'Facebook', 'ContactNumber', 'Email']
    sheet.append(excel_row_header)
    for row in source_db_cursor.execute("SELECT AccountName, ClientName, ServerLocation, Address, Facebook, ContactNumber, Email, DateEntry, NetPlan, AmountDue, DueDate, LatestReceipt, Status, PaymentStatus, Note, LatestBilling, SubscriptionCover, IPaddress, Balance, Password, Grouping, ConnectionType, AccountNumber, Profile, DisconnectionMode, SubscriptionMode, AuthenticationMode FROM client"):
  
        # Get workbook active sheet  
        # from the active attribute. 

        
        print("active sheet title: " + sheet_title)
        if not check(row['Email']):
            excel_row = [row['AccountNumber'], row['AccountName'], row['ClientName'], row['Facebook'], row['ContactNumber'], row['Email']]
            sheet.append(excel_row)

    sheet = wb.create_sheet("No_phone")
    sheet_title = sheet.title
    sheet.append(excel_row_header)
    for row in source_db_cursor.execute("SELECT AccountName, ClientName, ServerLocation, Address, Facebook, ContactNumber, Email, DateEntry, NetPlan, AmountDue, DueDate, LatestReceipt, Status, PaymentStatus, Note, LatestBilling, SubscriptionCover, IPaddress, Balance, Password, Grouping, ConnectionType, AccountNumber, Profile, DisconnectionMode, SubscriptionMode, AuthenticationMode FROM client"):

        
        print("active sheet title: " + sheet_title)
        if not check_mobile(row['ContactNumber']):
            excel_row = [row['AccountNumber'], row['AccountName'], row['ClientName'], row['Facebook'], row['ContactNumber'], row['Email']]
            sheet.append(excel_row)

if __name__ == '__main__':

    client_servers = [] # ['VIA VERDE', 'RIVERLANE']

    def dict_factory(cursor, row):
        col_names = [col[0] for col in cursor.description]
        return {key: value for key, value in zip(col_names, row)}

    source_db_connection = sqlite3.connect("Data.db")
    source_db_connection.row_factory = dict_factory
    source_db_cursor = source_db_connection.cursor()
    wb = openpyxl.Workbook()
    export_emails(source_db_connection, wb)
    wb.save("account_emails.xlsx")
    print("Done!")
    source_db_cursor.close()
    source_db_connection.close()