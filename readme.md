# Asanito Data Sync Bridge

This Flask application serves as a robust and configurable bridge to synchronize business data from a legacy MSSQL database to the Asanito SaaS platform via its REST API. It includes a powerful web dashboard for monitoring, management, and diagnostics.

## Key Features

-   **Automated, Scheduled Synchronization:** Jobs run on a configurable schedule to sync new and updated records for:
    -   **Memberships** -> Asanito Contacts
    -   **Services** -> Asanito Products
    -   **Store & Service Invoices** -> Asanito Invoices
    -   **Receipts** -> Asanito Operating Incomes
-   **Robust Job Management:**
    -   **Concurrency Lock:** A database-level lock ensures only one sync job runs at a time, preventing race conditions.
    -   **Web UI Control:** Enable/disable, manually trigger, and gracefully terminate running jobs directly from the dashboard.
    -   **Live Log Streaming:** View real-time log output for manually triggered jobs.
-   **Intelligent & Scalable Sync Logic:**
    -   **Idempotent Processing:** The system tracks sync status (`fetchStatus`) and processes only the latest version of a record, making it safe to re-run jobs.
    -   **Efficient Batching:** Jobs process data in small, efficient batches to minimize database load, allowing the system to scale to millions of records.
-   **Powerful Admin & Diagnostic Tools:**
    -   **Inspect Dashboard:** Get an at-a-glance overview of the sync status for all tables (synced, pending, failed, skipped). Operators can retry failed records or ignore skipped ones individually or in bulk.
    -   **Secure Data Explorer:** A read-only tool for securely querying whitelisted tables and columns in the source database for quick diagnostics without direct DB access.
    -   **Data Seeding & Reset:** Utilities to seed the database with realistic fake data for testing or perform a hard reset of the application's state (logs, mappings, job configs).
-   **Flexible Configuration:**
    -   **Mappings UI:** A user-friendly interface to map source system IDs and values to their corresponding values in Asanito (e.g., gender codes, organization IDs, bank accounts).
    -   **Minimum IDD Filter:** Set a "sync start point" for any table to ignore historical data before a certain record ID.
-   **Automated Deal Creation:**
    -   Automatically create "Deals" (sales opportunities) in Asanito when an invoice containing a pre-configured "trigger product" is synced.
    -   Funnel and Funnel Level IDs can be configured on a per-product basis.

## Project Structure

```
.
├── app/                  # Main application source code
│   ├── jobs/             # Definitions for scheduled sync jobs
│   ├── services/         # Business logic, API clients, DB repositories
│   ├── static/           # CSS, JavaScript files
│   ├── templates/        # Jinja2 HTML templates
│   ├── utils/            # Helper utilities (e.g., date converters)
│   ├── __init__.py       # Application factory (create_app)
│   ├── models.py         # SQLAlchemy ORM models
│   └── routes.py         # Flask routes for the web dashboard
├── tests/                # Test scripts and database seeder
├── .env                  # Local environment variables (IMPORTANT: create this)
├── config.py             # Configuration classes (dev, prod)
├── requirements.txt      # Python dependencies
└── run.py                # Application entry point
```

## Getting Started

### 1. Prerequisites

-   Python 3.10+
-   Access to a Microsoft SQL Server instance.
-   ODBC Driver for SQL Server installed on your system.
-   Asanito API credentials.

### 2. Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-directory>
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install the required Python packages:**
    ```bash
    pip install -r requirements.txt
    ```

### 3. Environment Variable Configuration

This is the most important setup step. The application is configured using a `.env` file in the project root.

1.  **Create the `.env` file:** Copy the example file:
    ```bash
    cp example.env .env
    ```

2.  **Edit the `.env` file** with your specific credentials and settings:

    ```ini
    # .env

    # --- Flask Application Configuration ---
    # Used for session security. Change this to a random, secret string.
    SECRET_key=a_very_secret_and_random_key_here

    # --- Timezone Configuration ---
    # Set to your local timezone, e.g., Asia/Tehran
    APP_TIMEZONE=Asia/Tehran

    # --- Database Connection ---
    # IMPORTANT: Provide the details for your MSSQL source database.
    # The application uses these to build the connection string.
    DB_DRIVER=ODBC Driver 17 for SQL Server
    DB_SERVER=your_db_server_address
    DB_PORT=1433
    DB_DATABASE=your_database_name
    DB_USERNAME=your_db_username
    DB_PASSWORD=your_db_password

    # --- Asanito API Configuration ---
    # The base URL for the Asanito API, without a trailing slash.
    ASANITO_BASE_URL="https://demobak.asanito.com"

    # The credentials for authenticating with the Asanito API.
    ASANITO_MOBILE=your_asanito_mobile
    ASANITO_PASSWORD=your_asanito_password
    ASANITO_CUSTOMER_ID=your_asanito_customer_id

    # --- Job Testing Limit (Optional) ---
    # Set a number to limit records processed per run for testing.
    # Comment out or leave blank for production.
    # JOB_RECORD_LIMIT=5
    ```

### 4. Running the Application

1.  **Initialize the Database:**
    The first time you run the application, it will automatically create the necessary application-specific tables (`sync_log`, `job_config`, `mapping`, etc.) in the database specified in your `.env` file. It will also perform a lightweight migration to add tracking columns (`fetchStatus`, `fetchMessage`, etc.) to the existing business data tables.

2.  **Start the Flask Development Server:**
    ```bash
    flask run
    ```
    *Note: The `run.py` file is configured to disable the Flask reloader (`use_reloader=False`) to prevent the scheduler from running twice in debug mode.*

3.  **Access the Dashboard:**
    Open your web browser and navigate to `http://127.0.0.1:5000`.

    You should see the main dashboard with the list of discovered jobs. By default, they will be disabled. You can enable them, configure their schedules, and trigger them manually from this UI.

### 5. Running in Production (Gunicorn)

For a production deployment, use a WSGI server like Gunicorn.

```bash
gunicorn --workers 3 --bind 0.0.0.0:8000 "run:app"
```

The application's concurrency lock is designed to work safely with multiple Gunicorn workers.

## Usage Guide

-   **Dashboard:** The main page shows all scheduled jobs, their status, next run time, and recent sync logs.
-   **Job Actions:**
    -   **Run Now (Play Icon):** Manually triggers a job and streams its logs live to your browser.
    -   **Enable/Disable (Pause/Check Icon):** Toggles whether a job is active in the scheduler.
    -   **Terminate (Stop Icon):** Sends a graceful shutdown request to a running job.
    -   **Edit Schedule (Pencil Icon):** Opens a modal to change the job's cron schedule.
-   **Mappings:** Configure translations between source and target system values. Select a mapping type, click "Load & Discover," fill in the target Asanito values, and save.
-   **Deals:** Configure which products, when found in an invoice, should automatically create a Deal in Asanito.
-   **Inspect:** Diagnose sync issues. View counts of failed and skipped records for each table, and take action to retry or ignore them.
-   **Admin:**
    -   **Data Explorer:** Safely search for records in the source database.
    -   **Application Controls:** Reload the scheduler or perform a hard reset of all application data.
    -   **Seed Database:** **(DANGER ZONE)** Wipe all business data and replace it with fake test data. Only for development environments.

---

## Job Descriptions and Execution Order

The synchronization process is broken down into several distinct jobs. They are scheduled to run in a specific order to ensure that data dependencies are met (e.g., contacts must exist before invoices can be created for them).

The default schedule is designed to run the entire sync cycle daily in the early morning, but this can be changed from the dashboard.

---

### 1. `Pre-process: De-duplicate Members`

-   **Job ID:** `preprocess_deduplicate_members`
-   **Default Schedule:** Daily at 01:30
-   **Purpose:** This is a crucial preparatory job that runs **before** the main contact sync. Its goal is to clean up potential duplicate member records in the source database to prevent creating duplicate contacts in Asanito.
-   **Logic:**
    1.  It first finds all pending member records that share the same mobile phone number. It determines the "winner" based on which record has the most recent invoice activity. All other duplicates in the group are marked with `fetchStatus = 'SKIPPED'`.
    2.  It then performs a second pass, doing the same de-duplication logic but based on the national code (`CodeMelli`).
-   **Why it's first:** By running this first, it ensures the subsequent `sync_contacts_from_memberships` job wastes no time attempting to process records that are known duplicates.

---

### 2. `Sync: Contacts (from Memberships)`

-   **Job ID:** `sync_contacts_from_memberships`
-   **Default Schedule:** Daily at 02:00
-   **Purpose:** This is the primary job for synchronizing people. It reads from the `dbo.membership` table and creates or updates corresponding "Contact" records in Asanito.
-   **Dependencies:**
    -   Waits for the de-duplication job to finish.
    -   Other jobs (like invoice sync) depend on this job to run first, as a Contact must exist in Asanito before an invoice can be assigned to it.
-   **Logic:**
    -   Finds the latest version of each pending person (grouped by `personVId`).
    -   Builds a detailed payload including name, contact info, and custom fields (like membership code, job title, etc.).
    -   If the contact already exists in Asanito, it performs a series of update calls; otherwise, it creates a new contact.

---

### 3. `Sync: Services to Products`

-   **Job ID:** `sync_services_to_products`
-   **Default Schedule:** Daily at 02:30
-   **Purpose:** This job synchronizes business services from the `dbo.service` table into "Product" records in Asanito.
-   **Dependencies:** Invoice sync jobs depend on this, as an invoice item must link to an existing Product in Asanito.
-   **Logic:**
    -   Finds new or updated services.
    -   Dynamically finds or creates the corresponding "Product Category" in Asanito based on the `serviceGroup` field.
    -   Creates or updates the Product in Asanito with details like title, price, and type.

---

### 4. `Sync: Store Invoices (Hed/Item)` & `Sync: Service Invoices (Gym)`

These two jobs are functionally very similar but operate on different source tables. They run after contacts and products have been synced.

#### `Sync: Store Invoices (Hed/Item)`

-   **Job ID:** `sync_store_invoices`
-   **Default Schedule:** Daily at 03:00
-   **Source Tables:** `dbo.invoiceHed` (header) and `dbo.invoiceItem` (line items).

#### `Sync: Service Invoices (Gym)`

-   **Job ID:** `sync_service_invoices`
-   **Default Schedule:** Daily at 03:30
-   **Source Table:** `dbo.ServiceInvoice` (contains header and item info in one row).

-   **Purpose (for both):** To create or update "Invoice" records in Asanito.
-   **Dependencies:**
    -   The Contact corresponding to the invoice's person must already exist in Asanito.
    -   All Products listed as line items in the invoice must already exist in Asanito.
-   **Logic:**
    -   Finds pending invoices.
    -   Verifies that the associated Contact and all Product dependencies have been synced. If not, the invoice is marked as `SKIPPED` or `FAILED` with a descriptive message.
    -   Builds a complex invoice payload with customer details, line items, discounts, and taxes.
    -   Creates the invoice in Asanito.
    -   **Finalizes** the invoice by setting its status to "Confirmed."
    -   **(Conditional) Deal Creation:** If the invoice contains a pre-configured trigger product, this job will also automatically create a "Deal" in Asanito.

---

### 5. `Sync: Receipts to Operating Income`

-   **Job ID:** `sync_receipts_to_income`
-   **Default Schedule:** Daily at 04:00
-   **Purpose:** This job syncs payment records from the `dbo.receipt` table into "Operating Income" records in Asanito. This effectively adds payments to a customer's account/wallet.
-   **Dependencies:** The Contact corresponding to the receipt's person must exist in Asanito.
-   **Logic:**
    -   Finds pending receipts.
    -   Uses a sophisticated mapping logic to determine the correct target bank account in Asanito based on the payment method (`modeldaryaft`) in the source data (e.g., cash, cheque, transfer).
    -   Creates a new Operating Income record, which increases the customer's balance in Asanito.