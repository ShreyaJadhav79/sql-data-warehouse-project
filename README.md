# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights **industry best practices** in data engineering and analytics.

---

## 🏗️ Data Architecture
1. **Bronze Layer**: Stores raw data *as-is* from the source systems. Data is ingested from CSV files into a SQL Server database.
2. **Silver Layer**: Includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Contains business-ready data modeled into a **star schema** for reporting and analytics.

---

## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a modern data warehouse using the **Medallion Architecture** (Bronze, Silver, and Gold layers).
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:
- SQL Development  
- Data Architecture  
- Data Engineering  
- ETL Pipeline Development  
- Data Modeling  
- Data Analytics  

---

## 🛠️ Important Links & Tools
- **[Datasets](datasets/):** Access the project dataset (CSV files).  
- **[SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads):** Lightweight SQL Server edition for hosting your database.  
- **[SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16):** GUI tool for managing and interacting with SQL databases.  

---

## 🚀 Project Requirements

### Building the Data Warehouse 

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM), provided as CSV files.  
- **Data Quality**: Cleanse and resolve data quality issues before analysis.  
- **Integration**: Combine both sources into a single, user-friendly data model optimized for analytical queries.  
- **Scope**: Focus on the latest dataset only; historical tracking is not required.  
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.  

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**  
- **Product Performance**  
- **Sales Trends**  

These insights empower stakeholders with key business metrics, enabling **data-driven strategic decision-making**.  

---

## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality assurance files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
```

---

## 🛡️ License
This project is licensed under the [MIT License](LICENSE).  

---

## 🌟 About Me
Hi there! I'm **Shreya Anil Jadhav**, a recent **IT Engineering graduate** passionate about **data analytics**.  
I'm on a mission to keep **learning**, **growing**, and making working with data both enjoyable and meaningful.
