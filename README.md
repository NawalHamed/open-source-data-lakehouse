
Building a Modern Data Lakehouse
This repository showcases the construction of a modern data lakehouse architecture using a suite of powerful open-source technologies. It provides a practical, hands-on example of how to integrate various components to create a scalable, flexible, and efficient data platform capable of handling diverse data workloads, from raw data ingestion to advanced analytics and visualization.

Technologies Used
This project leverages the following key open-source technologies:

MinIO: High-performance, S3 compatible object storage, serving as the data lake storage layer for raw and processed data.

Apache Spark: A unified analytics engine for large-scale data processing, used for transforming and analyzing data within the lakehouse.

Apache Iceberg: An open table format for huge analytic datasets, providing ACID transactions, schema evolution, and time travel capabilities over data stored in MinIO.

Project Nessie: A Git-like data catalog for Iceberg, enabling version control for data lake tables, branching, merging, and easy rollback.

Apache Airflow: A platform to programmatically author, schedule, and monitor workflows, orchestrating the entire data pipeline from ingestion to transformation.

Apache Superset: A modern data exploration and visualization platform, used for creating interactive dashboards and reports on the data stored in the lakehouse.

Features and Components
The data lakehouse architecture demonstrated in this repository includes:

Scalable Data Storage: MinIO provides a robust and scalable object storage solution for all your data.

Data Versioning and Governance: Apache Iceberg and Project Nessie work in tandem to offer version control, schema evolution, and transaction management for your data tables, enhancing data reliability and auditability.

ETL/ELT Workflows: Apache Spark is used for powerful data transformations, orchestrated by Apache Airflow, to build efficient data pipelines.

Workflow Orchestration: Airflow manages the end-to-end data lifecycle, ensuring data freshness and pipeline reliability.

Business Intelligence: Apache Superset connects to the processed data in Iceberg tables, allowing for rich data visualization and insights.

Modular Design: Each component is containerized, allowing for easy deployment and management.

Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

Prerequisites
Docker and Docker Compose: Essential for running all services as containerized applications.

Install Docker

Install Docker Compose

Git: For cloning the repository.

Setup
Clone the repository:

git clone https://github.com/NawalHamed/open-source-data-lakehouse.git
cd open-source-data-lakehouse

Start the services:
This command will build and start all the necessary containers for MinIO, Spark, Nessie, Airflow, and Superset.

docker-compose up --build -d

Note: The first time you run this, it might take a while to download all Docker images and build the custom ones.

Verify services:
You can check the status of your running containers with:

docker-compose ps

Usage
Once all services are up and running, you can access them via their respective web interfaces:

MinIO Console: http://localhost:9001 (Default credentials: minioadmin/minioadmin)

Project Nessie UI: http://localhost:19120

Apache Airflow UI: http://localhost:8080 (Default credentials: airflow/airflow)

You'll need to enable the example DAGs or upload your own to start orchestrating workflows.

Apache Superset: http://localhost:8088 (Default credentials: admin/admin)

You'll need to set up a database connection to Nessie/Spark to visualize your Iceberg tables.

Data Flow Example
Data Ingestion: Use Airflow DAGs to ingest raw data into MinIO.

Data Transformation: Spark jobs (triggered by Airflow) read raw data from MinIO, process it, and write it back as Iceberg tables managed by Nessie.

Data Analysis: Superset connects to the Nessie catalog to query the Iceberg tables and build dashboards.

Project Structure
.
├── docker-compose.yml           # Defines all services and their configurations
├── minio/                       # MinIO specific configurations (e.g., initial buckets)
├── airflow/                     # Airflow DAGs and configurations
│   ├── dags/                    # Your Airflow DAGs
│   └── config/                  # Airflow configuration files
├── spark/                       # Spark specific configurations and scripts
│   └── jobs/                    # Spark application scripts
├── superset/                    # Superset specific configurations
│   └── data/                    # Superset metadata volume
└── README.md                    # This file

Contributing
Contributions are welcome! If you have suggestions for improvements, new features, or bug fixes, please open an issue or submit a pull request.
