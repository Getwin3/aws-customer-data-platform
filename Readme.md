# AWS Customer Data Platform (CDP)

This project implements a scalable Customer Data Platform (CDP) built on AWS that consolidates customer data from multiple sources and builds a Unified Customer Profile.

The platform performs:

- Multi-source ingestion
- Identity resolution
- Deduplication
- Golden record generation
- Data quality validation
- PII masking
- Single Customer View generation in Redshift

---

## Architecture

Data Sources → Data Lake → Step Functions → Glue PySpark Jobs → UCP Generation → Redshift → CDP UI

---

## Technologies

- PySpark
- AWS Glue
- AWS Step Functions
- AWS Lambda
- Amazon S3
- Amazon Redshift
- EC2

---

## Key Features

- Customer identity resolution
- Golden record creation
- CDC-ready ingestion pipelines
- Data quality monitoring
- PII masking
- Scalable distributed processing