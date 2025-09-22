# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a comprehensive data warehousing and analytics solution — from building a data warehouse to generating actionable insights.  
Designed as a portfolio project, it highlights **industry best practices** in data engineering and analytics.

---

## 🏗️ Data Architecture
1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV files into SQL Server Database.
2. **Silver Layer**: Cleanses, standardizes, and normalizes data to prepare it for analysis.
3. **Gold Layer**: Contains business-ready data modeled into a **star schema** for reporting and analytics.

---

## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a modern data warehouse using **Medallion Architecture** (Bronze, Silver, Gold).
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 **Key Skills Highlighted in This Project:**
- SQL Development  
- Data Architecture  
- Data Engineering  
- ETL Pipeline Development  
- Data Modeling  
- Data Analytics  

---

## 🛠️ Important Links & Tools
- **[Datasets](datasets/):** Raw datasets (CSV files) used for the project.
- **[SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads):** Lightweight SQL Server for hosting your database.
- **[SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16):** GUI for managing and interacting with databases.

---

## 🚀 Project Requirements

### **Building the Data Warehouse**

#### Objective
Develop a modern data warehouse using SQL Server to **consolidate sales data**, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources:** Import data from two source systems (ERP and CRM) provided as CSV files.  
- **Data Quality:** Cleanse and fix data quality issues before analysis.  
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.  
- **Scope:** Focus on the latest dataset only (no historization needed).  
- **Documentation:** Provide a clear, easy-to-follow structure for business and analytics teams.

---

### **Analytics & Reporting (BI)**

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**  
- **Product Performance**  
- **Sales Trends**

These insights help stakeholders make **data-driven decisions** and track key business metrics.

---

## 📂 Repository Structure
```
sql-data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for loading raw data into the Bronze layer
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytics-ready models
│
├── tests/                              # Quality check scripts for each layer
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

---

### ✅ Final Notes
- The **docs/** folder is skipped intentionally to keep the project simple and beginner-friendly.  
- You can run scripts step-by-step:
  1. **Bronze** → load raw data.  
  2. **Silver** → clean and standardize data.  
  3. **Gold** → build analytics-ready models.  
  4. Run **tests** to validate data quality.

---
