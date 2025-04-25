# Airflow Projects Repository

This repository contains several Airflow DAG projects maintained by the Analytics Engineering and DataOps teams. Each project lives in its own directory and serves a distinct operational need:

## Projects

1. **Lake-to-DWH Cross-Notification Pipeline**  
   Dynamically generates staging DAGs in the Data Warehouse Airflow instance to run dbt models whenever upstream Lake tables are refreshed. Uses Airflow `Dataset` for cross-instance triggers.

2. **Payment Volume Pacing DAG**  
   Runs daily to project end-of-month payment volumes per method based on business-day pacing logic, writes results to Redshift, and powers a live Tableau dashboard for operational decision-making.

3. **Airflow DagRun Export DAG**  
   Exports recent `DagRun` metadata from the Airflow metastore into Redshift tables, enabling easy historical query and SLA monitoring beyond the limitations of Airflow’s native metastore.

4. **DBT Drop Redshift View or Table DAG**  
   A manual-triggered DAG that drops specified Redshift tables or views via runtime config, allowing analytics engineers to manage schema cleanup without needing direct console access.

## Getting Started

1. Clone the repo.  
2. Configure Airflow Variables for each project (see individual README files).  
3. Deploy the DAG folders under your Airflow `dags/` directory.  
4. Trigger or schedule each DAG as documented.

## Directory Structure

```text
.
├── lake_to_dwh/
│   └── generate_stg_dags.py
│   └── README.md
├── payment_pacing/
│   └── payment_pacing_dag.py
│   └── README_pacing.md
├── dagrun_export/
│   └── airflow_dagrun_export.py
│   └── README_dagrun_export.md
├── drop_redshift/
│   └── dbt_drop_redshift.py
│   └── README_drop_redshift.md
└── README_repo.md        # ← This overview file
```

## Contributing

Feel free to open issues or pull requests for enhancements, bug fixes, or new DAG proposals.

## License

This code is licensed under the MIT License. See [LICENSE](LICENSE) for details.
