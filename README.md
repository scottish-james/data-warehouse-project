
# Redshift Data Warehouse Project

## Overview
This project builds an ETL pipeline for a Redshift-based data warehouse. The pipeline extracts, transforms, and loads data from S3 into Redshift, creating a data warehouse optimized for querying and analytics.

The key components include:
- Infrastructure-as-code (IaC) to automate the setup of Redshift clusters.
- SQL scripts to create, drop, and manage tables in Redshift.
- Python scripts to orchestrate the ETL pipeline.

## Project Structure
The repository contains the following files:

### **Configuration Files**
- **`dwh.cfg`**: Configuration file for Redshift cluster details, including cluster host, database credentials, and S3 bucket information.
- **`iac.cfg`**: Configuration file for infrastructure setup, including AWS IAM roles and Redshift parameters.

### **Python Scripts**
- **`iac.py`**: Automates the setup and teardown of AWS resources such as IAM roles and Redshift clusters.
- **`create_tables.py`**: Drops existing tables (if any) and creates new ones based on the provided schema.
- **`etl.py`**: Extracts data from S3, transforms it, and loads it into the Redshift tables.
- **`close.py`**: Cleans up Redshift resources to ensure proper termination of the cluster and related resources.
- **`sql_queries.py`**: Contains SQL queries for creating, dropping, and inserting data into tables.

### **Jupyter Notebook**
- **`main.ipynb`**: Provides an interactive environment to execute and test various components of the pipeline, such as ETL processes and table creation.

## Getting Started
### Prerequisites
Ensure you have the following installed and configured:
- Python 3.6 or higher
- AWS CLI with appropriate IAM permissions
- PostgreSQL driver (`psycopg2`)
- Boto3 (AWS SDK for Python)

### Installation:
#### 1. Clone the repository:
   ```bash
   git clone https://github.com/scottish-james/data-warehouse-project.git
  ```

#### 2.  Install required Python packages:
    
 
    pip install -r requirements.txt

    
#### 3.  Update the configuration files (`dwh.cfg` and `iac.cfg`) with your AWS credentials, Redshift cluster details, and S3 bucket paths.
    

### Usage

#### 1\. Setup Infrastructure

Run `iac.py` to create the Redshift cluster and associated resources:

```bash
python iac.py
```

#### 2\. Create Tables

Run `create_tables.py` to initialize the tables in the Redshift cluster:

```bash
python create_tables.py
```

#### 3\. Run the ETL Pipeline

Run `etl.py` to load data from S3 into the Redshift tables:

```bash
python etl.py
```

#### 4\. Terminate Resources

Run `close.py` to terminate the Redshift cluster and clean up resources:

```bash
python close.py
```

### Optional: Use Jupyter Notebook

Use `main.ipynb` for step-by-step execution and testing of the pipeline.

SQL Table Structure
-------------------

The tables in this project include:

*   **Staging Tables**: Temporary tables for raw data from S3.
    *   `staging_events`
    *   `staging_songs`
*   **Fact Table**:
    *   `songplays`
*   **Dimension Tables**:
    *   `users`
    *   `songs`
    *   `artists`
    *   `time`

Refer to `sql_queries.py` for specific SQL commands used to create these tables.

Troubleshooting
---------------

1.  **Connection Issues**: Ensure your `dwh.cfg` file has correct credentials and network settings.
2.  **IAM Role Errors**: Check that the IAM role has necessary permissions to access S3 and Redshift.
3.  **Table Creation Errors**: Review `create_tables.py` for table-specific error messages.
