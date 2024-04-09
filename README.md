# Microsoft Planner Data Extraction and Transformation

This Python package extracts data from the Microsoft Planner REST API, performs necessary transformations, and uploads it to a MySQL Database.

## Installation

Clone the repository and navigate to the project directory. Then, install the required dependencies using pip:

```bash
pip install -r requirements.txt


---Setup

Update the .env file with the credentials for the MySQL Database and Azure configuration.
Initialize the Azure AD app to acquire tokens for Microsoft Azure.

---Usage

Run the Python script main.py to execute the data extraction, transformation, and database upload processes. Make sure you have completed the setup steps mentioned above before running the script.

```bash
python main.py

---Workflow

Acquire token for Microsoft Azure using Azure AD app.
Fetch data from the Microsoft Planner API including buckets, categories(labels), and assignments.
Create a dataframe using Pandas and apply necessary transformations.
Unnest columns containing dictionaries as values and create additional tables in the SQL database.
Create the main table and tables for categories, buckets, and assignments in the MySQL Database.

---Configuration
Ensure the following configurations are properly set in the .env file:

DATABASE_HOST: Hostname of the MySQL Database.
DATABASE_USER: Username for accessing the MySQL Database.
DATABASE_PASSWORD: Password for accessing the MySQL Database.
DATABASE_PORT: Port of the MySQL Database.
DATABASE_NAME: Name of the MySQL Database.
TENANT_ID: Azure Tenant ID.
CLIENT_ID: Azure Client ID.
CLIENT_SECRET: Azure Client Secret.
PLAN_ID: ID of the plan.

---Contributing
Contributions are welcome!
