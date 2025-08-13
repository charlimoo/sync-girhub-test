# start of tests/seed_database.py
# start of tests/seed_database.py

import pyodbc
import uuid
import random
from faker import Faker
from datetime import datetime, timedelta
from collections import defaultdict
import os
from dotenv import load_dotenv

# --- START OF NEW CONNECTION LOGIC ---

# Load environment variables from .env file at the project root
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

def build_pyodbc_connection_string_from_env():
    """
    Builds a pyodbc-compatible connection string from individual environment variables.
    """
    db_driver = os.environ.get('DB_DRIVER')
    db_server = os.environ.get('DB_SERVER')
    db_port = os.environ.get('DB_PORT')
    db_database = os.environ.get('DB_DATABASE')
    db_username = os.environ.get('DB_USERNAME')
    db_password = os.environ.get('DB_PASSWORD')

    if not all([db_driver, db_server, db_database, db_username, db_password]):
        raise ValueError("FATAL: One or more required DB_* environment variables are missing.")

    # This format is robust for special characters in the driver name and password.
    return (
        f"DRIVER={{{db_driver}}};"
        f"SERVER={db_server},{db_port};"
        f"DATABASE={db_database};"
        f"UID={db_username};"
        f"PWD={{{db_password}}};"
    )

# Dynamically build the connection string
CONNECTION_STRING = build_pyodbc_connection_string_from_env()


# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
NUM_UNIQUE_ENTITIES = 15
fake = Faker('fa_IR')

data_pools = {
    "organizations": [uuid.uuid4() for _ in range(5)],
    "creator_users": [uuid.uuid4() for _ in range(10)],
    "services": defaultdict(list), # Stores {service_vid: [list_of_idds]}
    "memberships": defaultdict(list), # Stores {person_vid: [list_of_member_vids]}
    "latest_member_vids": {}, # Stores {person_vid: latest_member_vid}
    # --- Data pools reflecting production data for receipts ---
    "transfer_bank_account_guids": [uuid.uuid4() for _ in range(2)],
    "cheque_bank_names": ['بانک سامان', 'بانک پارسیان', 'بانک کارآفرین'],
    "cash_receive_type_guids": [uuid.uuid4() for _ in range(2)] # GUIDs from ReceiveType column for cash
}

TABLE_NAMES = ['service', 'membership', 'invoiceHed', 'invoiceItem', 'ServiceInvoice', 'receipt']

# --- Table Definitions --- (Unchanged)
TABLE_DEFINITIONS = {
    'service': """
        CREATE TABLE [dbo].[service] (
            [serviceAid] INT NULL, [serviceVid] UNIQUEIDENTIFIER NULL, [type] INT NULL,
            [code] BIGINT NULL, [title] NVARCHAR(400) NULL, [unitref] INT NULL,
            [serviceGroup] NVARCHAR(400) NULL, [price] BIGINT NULL, [isChange] BIT NULL,
            [idd] BIGINT IDENTITY(1,1) NOT NULL,
            [fetchStatus] NVARCHAR(100) NULL, [fetchMessage] NVARCHAR(MAX) NULL
        );
    """,
    'membership': """
        CREATE TABLE [dbo].[membership] (
            [memberAid] INT NULL, [personVId] UNIQUEIDENTIFIER NULL, [memberVId] UNIQUEIDENTIFIER NULL,
            [Creator] INT NULL, [modifier] INT NULL, [MembershipCode] BIGINT NULL,
            [FinancialAccountCode] BIGINT NULL, [name] NVARCHAR(400) NULL, [lastname] NVARCHAR(400) NULL,
            [CodeMelli] NVARCHAR(400) NULL, [Address1] NVARCHAR(400) NULL, [TelNumber1] NVARCHAR(400) NULL,
            [MobilePhoneNumber1] NVARCHAR(400) NULL, [DebtorAmount] BIGINT NULL, [Description] NVARCHAR(1000) NULL,
            [gender] INT NULL, [jobpost] NVARCHAR(200) NULL, [Birthday] VARCHAR(19) NULL,
            [PersianMembershipDate] NVARCHAR(20) NULL, [RecognitionMethods] NVARCHAR(200) NULL,
            [isChange] BIT NULL, [wallet] INT NULL, [idd] BIGINT IDENTITY(1,1) NOT NULL,
            [fetchStatus] NVARCHAR(100) NULL, [fetchMessage] NVARCHAR(MAX) NULL
        );
    """,
    'invoiceHed': """
        CREATE TABLE [dbo].[invoiceHed] (
            [invoiceVID] UNIQUEIDENTIFIER NULL, [invoiceAID] INT NULL, [Title] NVARCHAR(40) NULL,
            [OrganizationID] UNIQUEIDENTIFIER NULL, [IssueDate] DATE NULL, [SellDate] DATE NULL,
            [PersonVID] UNIQUEIDENTIFIER NULL, [TaxPercent] NUMERIC(38, 2) NULL,
            [AdditionDeductionAmount] NUMERIC(38, 2) NULL, [CreatorUserVID] UNIQUEIDENTIFIER NULL,
            [isDelete] BIT NULL, [isChange] BIT NULL, [idd] BIGINT IDENTITY(1,1) NOT NULL,
            [fetchStatus] NVARCHAR(100) NULL, [fetchMessage] NVARCHAR(MAX) NULL
        );
    """,
    'invoiceItem': """
        CREATE TABLE [dbo].[invoiceItem] (
            [invoiceVID] UNIQUEIDENTIFIER NULL, [Title] NVARCHAR(200) NULL, [ItemAID] INT NULL,
            [itemVID] UNIQUEIDENTIFIER NULL, [ProducVtID] UNIQUEIDENTIFIER NULL, [count] BIGINT NULL,
            [UnitPrice] BIGINT NULL, [DiscountAmount] NUMERIC(38, 2) NULL, [ProductUnitVID] INT NULL,
            [ProductType] INT NULL, [index] BIGINT NULL, [isDelete] BIT NULL, [isChange] BIT NULL,
            [idd] BIGINT IDENTITY(1,1) NOT NULL,
            [fetchStatus] NVARCHAR(100) NULL, [fetchMessage] NVARCHAR(MAX) NULL
        );
    """,
    'ServiceInvoice': """
        CREATE TABLE [dbo].[ServiceInvoice] (
            [id] UNIQUEIDENTIFIER NULL, [invoiceAID] INT NULL, [title] VARCHAR(100) NULL,
            [OrganizationID] UNIQUEIDENTIFIER NULL, [IssueDate] DATETIME NULL, [SellDat] DATETIME NULL,
            [personid] UNIQUEIDENTIFIER NULL, [TaxPercent] NUMERIC(38, 2) NULL, [discount] NUMERIC(38, 2) NULL,
            [CreatorUser] UNIQUEIDENTIFIER NULL, [UnitPrice] NUMERIC(38, 2) NULL, [count] BIGINT NULL,
            [selTypeId] INT NULL, [ProducVtID] UNIQUEIDENTIFIER NULL, [ServiceTitle] NVARCHAR(200) NULL,
            [ProductUnitVID] INT NULL, [ProductType] INT NULL, [index] INT NULL, [isdelete] BIT NULL,
            [ischange] BIT NULL, [idd] BIGINT IDENTITY(1,1) NOT NULL,
            [fetchStatus] NVARCHAR(100) NULL, [fetchMessage] NVARCHAR(MAX) NULL
        );
    """,
    'receipt': """
        CREATE TABLE [dbo].[receipt] (
            [aID] INT NULL, [vID] UNIQUEIDENTIFIER NULL, [tarikh] DATE NULL,
            [personid] UNIQUEIDENTIFIER NULL, [fullname] NVARCHAR(600) NULL, [title] NVARCHAR(804) NULL,
            [Amount] BIGINT NULL, [modeldaryaft] NVARCHAR(60) NULL, [ReceiveType] UNIQUEIDENTIFIER NULL,
            [BankName] NVARCHAR(100) NULL, [ChequeNumber] NVARCHAR(60) NULL, [sarresidcheck] DATE NULL,
            [BankAccount] UNIQUEIDENTIFIER NULL, [Creator] INT NULL, [modifier] INT NULL,
            [isChange] BIT NULL, [isDelete] BIT NULL, [idd] BIGINT IDENTITY(1,1) NOT NULL,
            [fetchStatus] NVARCHAR(100) NULL, [fetchMessage] NVARCHAR(MAX) NULL
        );
    """
}

def get_db_connection():
    try:
        conn = pyodbc.connect(CONNECTION_STRING, autocommit=False)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def drop_existing_tables(cursor):
    print("Dropping existing business data tables...")
    for table_name in TABLE_NAMES:
        try:
            check_query = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'"
            if cursor.execute(check_query).fetchone():
                print(f"  -> Dropping table '{table_name}'...")
                cursor.execute(f"DROP TABLE dbo.{table_name}")
                cursor.commit()
            else:
                print(f"  -> Table '{table_name}' does not exist. Skipping drop.")
        except pyodbc.Error as e:
            print(f"Database error while dropping table {table_name}: {e}")
            cursor.rollback()
            raise
    print("Finished dropping tables.\n")

def create_tables(cursor):
    print("Creating tables...")
    for table_name, create_sql in TABLE_DEFINITIONS.items():
        try:
            print(f"  -> Creating table '{table_name}'...")
            cursor.execute(create_sql)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"Database error while creating table {table_name}: {e}")
            cursor.rollback()
            raise
    print("Table creation complete.\n")

# --- DATA POPULATION FUNCTIONS ---

def populate_services(cursor):
    """Generates test data for the 'service' table, including update scenarios."""
    print(f"Populating 'service' table with {NUM_UNIQUE_ENTITIES} stories...")
    sql = "INSERT INTO dbo.service (serviceAid, serviceVid, type, code, title, unitref, serviceGroup, price, isChange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
    
    for i in range(NUM_UNIQUE_ENTITIES):
        service_vid = uuid.uuid4()
        group = random.choice(['کلاس گروهی', 'کافه', 'فروشگاه', 'خدمات سالن'])
        base_title = f"{group} - {fake.word()}"
        base_price = random.randint(100, 1000) * 1000

        # Create a simple new service
        params = (None, service_vid, 2, random.randint(100, 999), base_title, 1, group, base_price, True)
        cursor.execute(sql, params)
        data_pools['services'][service_vid].append(cursor.execute("SELECT @@IDENTITY").fetchone()[0])
        
        # Occasionally, create an update for the service we just made
        if i % 3 == 0:
            print(f"  -> Creating an update for service '{base_title}'")
            # First, mark the original record as "not changed"
            cursor.execute("UPDATE dbo.service SET isChange = 0 WHERE idd = ?", data_pools['services'][service_vid][-1])
            
            # Now, insert the new, updated record with the same serviceVid
            updated_title = f"{base_title} (جدید)"
            updated_price = base_price + 50000
            params_update = (None, service_vid, 2, random.randint(100, 999), updated_title, 1, group, updated_price, True)
            cursor.execute(sql, params_update)
            data_pools['services'][service_vid].append(cursor.execute("SELECT @@IDENTITY").fetchone()[0])

    cursor.commit()
    print("-> Done.\n")


def populate_memberships(cursor):
    """Generates test data for the 'membership' table, including update scenarios with consistent IDs."""
    print(f"Populating 'membership' table with {NUM_UNIQUE_ENTITIES} stories...")
    sql = "INSERT INTO dbo.membership (memberAid, personVId, memberVId, name, lastname, gender, MobilePhoneNumber1, Address1, isChange, RecognitionMethods, PersianMembershipDate, Birthday) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

    for i in range(NUM_UNIQUE_ENTITIES):
        person_vid = uuid.uuid4() # The logical ID for the person
        member_vid = uuid.uuid4() # The unique ID for this specific record
        first_name, last_name = fake.first_name(), fake.last_name()
        gender = random.randint(0, 1)

        # Insert the initial record for the person
        params = (None, person_vid, member_vid, first_name, last_name, gender, fake.phone_number(), fake.address(), True, 'معرف', '1402/05/10', '1375/03/15')
        cursor.execute(sql, params)
        
        data_pools['memberships'][person_vid].append(member_vid)
        data_pools['latest_member_vids'][person_vid] = (member_vid, f"{first_name} {last_name}")

        # Occasionally, create an update for the person we just made
        if i % 3 == 0:
            print(f"  -> Creating an update for member '{first_name} {last_name}'")
            # Mark the previous record as "not changed"
            cursor.execute("UPDATE dbo.membership SET isChange = 0 WHERE memberVId = ?", member_vid)
            
            # Insert the new, updated record with the SAME personVId but a NEW memberVId
            new_member_vid = uuid.uuid4()
            params_update = (None, person_vid, new_member_vid, first_name, last_name, gender, fake.phone_number(), "آدرس جدید", True, 'اینستاگرام', '1401/01/01', '1380/01/01')
            cursor.execute(sql, params_update)
            
            data_pools['memberships'][person_vid].append(new_member_vid)
            data_pools['latest_member_vids'][person_vid] = (new_member_vid, f"{first_name} {last_name}")

    cursor.commit()
    print("-> Done.\n")

def populate_invoices(cursor):
    print(f"Populating 'invoiceHed' and 'invoiceItem' tables with {NUM_UNIQUE_ENTITIES} stories...")
    if not data_pools['latest_member_vids'] or not data_pools['services']:
        print("Skipping invoice population: Missing membership or service data.")
        return

    sql_hed = "INSERT INTO dbo.invoiceHed (invoiceVID, Title, OrganizationID, IssueDate, PersonVID, CreatorUserVID, isChange) VALUES (?, ?, ?, ?, ?, ?, ?);"
    sql_item = "INSERT INTO dbo.invoiceItem (invoiceVID, Title, itemVID, ProducVtID, count, UnitPrice, ProductUnitVID, ProductType, isChange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"

    for i in range(NUM_UNIQUE_ENTITIES):
        invoice_vid = uuid.uuid4()
        # Pick a random person and use their LATEST memberVId for the invoice
        person_vid = random.choice(list(data_pools['latest_member_vids'].keys()))
        latest_member_vid, _ = data_pools['latest_member_vids'][person_vid]
        
        org_id = random.choice(data_pools['organizations'])
        user_id = random.choice(data_pools['creator_users'])
        issue_date = datetime.now().date() - timedelta(days=random.randint(1, 30))

        cursor.execute(sql_hed, (invoice_vid, str(random.randint(1000, 9999)), org_id, issue_date, latest_member_vid, user_id, True))

        for j in range(random.randint(1, 3)):
            product_vid = random.choice(list(data_pools['services'].keys()))
            price = random.randint(50, 200) * 1000
            cursor.execute(sql_item, (invoice_vid, fake.bs(), uuid.uuid4(), product_vid, 1, price, 1, 1, True))
    
    cursor.commit()
    print("-> Done.\n")

def populate_service_invoices(cursor):
    print(f"Populating 'ServiceInvoice' table with {NUM_UNIQUE_ENTITIES} stories...")
    if not data_pools['latest_member_vids'] or not data_pools['services']:
        print("Skipping ServiceInvoice population: Missing membership or service data.")
        return
        
    sql = "INSERT INTO dbo.ServiceInvoice (id, title, OrganizationID, IssueDate, personid, CreatorUser, ProducVtID, ServiceTitle, UnitPrice, count, ProductUnitVID, ProductType, ischange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    
    for i in range(NUM_UNIQUE_ENTITIES):
        # Pick a random person and use their LATEST memberVId for the invoice
        person_vid = random.choice(list(data_pools['latest_member_vids'].keys()))
        latest_member_vid, _ = data_pools['latest_member_vids'][person_vid]
        
        product_vid = random.choice(list(data_pools['services'].keys()))
        org_id = random.choice(data_pools['organizations'])
        user_id = random.choice(data_pools['creator_users'])
        issue_date = datetime.now() - timedelta(days=random.randint(1, 30))
        price = random.randint(200, 500) * 1000
        
        params = (uuid.uuid4(), str(random.randint(10000, 99999)), org_id, issue_date, latest_member_vid, user_id, product_vid, fake.catch_phrase(), price, 1, 1, 2, True)
        cursor.execute(sql, params)
        
    cursor.commit()
    print("-> Done.\n")

def populate_receipts(cursor):
    """Generates test data for the 'receipt' table that mimics production data states."""
    print(f"Populating 'receipt' table with {NUM_UNIQUE_ENTITIES} stories...")
    if not data_pools['latest_member_vids']:
        print("Skipping receipt population: Missing membership data.")
        return

    sql = "INSERT INTO dbo.receipt (vID, tarikh, personid, fullname, title, Amount, modeldaryaft, ReceiveType, BankName, BankAccount, ChequeNumber, isChange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    
    for i in range(NUM_UNIQUE_ENTITIES):
        person_vid_key = random.choice(list(data_pools['latest_member_vids'].keys()))
        latest_member_vid, fullname = data_pools['latest_member_vids'][person_vid_key]
        issue_date = datetime.now().date() - timedelta(days=random.randint(1, 60))
        amount = random.randint(1000, 5000) * 1000
        
        receipt_type = random.choice(['حواله', 'چک', 'نقد'])
        
        bank_name, bank_account_guid, cheque_number, receive_type_guid = None, None, None, None

        if receipt_type == 'حواله':
            bank_account_guid = random.choice(data_pools['transfer_bank_account_guids'])
            print(f"  -> Creating 'حواله' (transfer) receipt. Mapping key will be BankAccount GUID: {bank_account_guid}")
        
        elif receipt_type == 'چک':
            bank_name = random.choice(data_pools['cheque_bank_names'])
            cheque_number = str(random.randint(100000, 999999))
            print(f"  -> Creating 'چک' (cheque) receipt. Mapping key will be BankName: {bank_name}")

        else: # 'نقد'
            receive_type_guid = random.choice(data_pools['cash_receive_type_guids'])
            print(f"  -> Creating 'نقد' (cash) receipt. Mapping key will be ReceiveType GUID: {receive_type_guid}")
            
        params = (uuid.uuid4(), issue_date, latest_member_vid, fullname, f"دریافت وجه از {fullname}", amount, receipt_type, receive_type_guid, bank_name, bank_account_guid, cheque_number, True)
        cursor.execute(sql, params)

    cursor.commit()
    print("-> Done.\n")


def populate_corrupted_data(cursor):
    print("Populating tables with intentionally corrupted data for testing...")
    
    # Membership Corruptions
    print("  -> Corrupting Memberships:")
    sql_member = "INSERT INTO dbo.membership (personVId, memberVId, name, lastname, gender, MobilePhoneNumber1, isChange) VALUES (?, ?, ?, ?, ?, ?, ?);"
    print("     - Adding member with missing last name...")
    cursor.execute(sql_member, (uuid.uuid4(), uuid.uuid4(), 'Corrupted - Missing LastName', None, 1, fake.phone_number(), True))
    print("     - Adding members with duplicate phone number...")
    dup_phone = '09123456789'
    cursor.execute(sql_member, (uuid.uuid4(), uuid.uuid4(), 'Corrupted - Duplicate Phone 1', 'UserA', 1, dup_phone, True))
    cursor.execute(sql_member, (uuid.uuid4(), uuid.uuid4(), 'Corrupted - Duplicate Phone 2', 'UserB', 0, dup_phone, True))
    print("     - Adding member with invalid gender mapping ID...")
    cursor.execute(sql_member, (uuid.uuid4(), uuid.uuid4(), 'Corrupted', 'Invalid Gender', 99, fake.phone_number(), True))

    # Service Corruptions
    print("  -> Corrupting Services:")
    sql_service = "INSERT INTO dbo.service (serviceVid, type, title, unitref, price, isChange) VALUES (?, ?, ?, ?, ?, ?);"
    print("     - Adding service with missing title...")
    cursor.execute(sql_service, (uuid.uuid4(), 2, None, 1, 10000, True))
    print("     - Adding service with invalid type mapping ID...")
    cursor.execute(sql_service, (uuid.uuid4(), 99, 'Corrupted - Invalid Type', 1, 20000, True))

    # Invoice Corruptions
    print("  -> Corrupting Invoices:")
    sql_service_inv = "INSERT INTO dbo.ServiceInvoice (id, title, OrganizationID, IssueDate, personid, CreatorUser, ProducVtID, ServiceTitle, UnitPrice, count, ischange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    person_keys = list(data_pools['latest_member_vids'].keys())
    service_keys = list(data_pools['services'].keys())
    org_id = data_pools['organizations'][0]
    user_id = data_pools['creator_users'][0]
    
    print("     - Adding orphaned service invoice (bad personid)...")
    cursor.execute(sql_service_inv, (uuid.uuid4(), 'Corrupted - Orphan Invoice', org_id, datetime.now(), uuid.uuid4(), user_id, service_keys[0], 'Test', 100, 1, True))
    
    print("     - Adding service invoice with non-existent product...")
    latest_member_vid, _ = data_pools['latest_member_vids'][person_keys[0]]
    cursor.execute(sql_service_inv, (uuid.uuid4(), 'Corrupted - Bad Product', org_id, datetime.now(), latest_member_vid, user_id, uuid.uuid4(), 'Test', 100, 1, True))
    
    print("     - Adding store invoice header with no items...")
    sql_hed = "INSERT INTO dbo.invoiceHed (invoiceVID, Title, OrganizationID, IssueDate, PersonVID, CreatorUserVID, isChange) VALUES (?, ?, ?, ?, ?, ?, ?);"
    cursor.execute(sql_hed, (uuid.uuid4(), 'Corrupted - No Items', org_id, datetime.now(), latest_member_vid, user_id, True))

    cursor.commit()
    print("-> Done.\n")
    
    
def run_seeding():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        drop_existing_tables(cursor)
        create_tables(cursor)
        
        populate_memberships(cursor)
        populate_services(cursor)
        populate_invoices(cursor)
        populate_service_invoices(cursor)
        populate_receipts(cursor)
        
        populate_corrupted_data(cursor)
        
        print("\nDatabase seeding completed successfully!")
        
    except (pyodbc.Error, ValueError) as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    run_seeding()
# end of tests/seed_database.py