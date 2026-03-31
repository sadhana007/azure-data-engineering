# 🔄 Azure Data Factory — Pipeline Design

> Author: Sadhana S Kumar
> Description: ADF pipeline designs for automated
>              data mart refresh and ETL workflows

---

## 📋 Pipeline Overview

| Pipeline Name | Purpose | Schedule |
|---|---|---|
| pl_refresh_business_mart | Refresh business performance data mart | Daily 6 AM |
| pl_load_client_data | Load raw client data from source | Every 4 hours |
| pl_data_quality_check | Run data quality validations | After every load |
| pl_powerbi_refresh | Trigger Power BI dataset refresh | Daily 7 AM |

---

## 🏗️ Pipeline Architecture
```
Source Systems
      │
      ▼
[ADF Copy Activity]
      │
      ▼
Azure Data Lake (Raw Layer)
      │
      ▼
[Databricks Transformation]
      │
      ▼
Azure Data Warehouse (Curated Layer)
      │
      ▼
[ADF Stored Procedure Activity]
      │
      ▼
Data Marts (Reporting Layer)
      │
      ▼
Power BI Dashboards
```

---

## 🔧 Pipeline Components

### 1️⃣ Copy Activity — Raw Data Ingestion
```json
{
  "name": "CopyClientData",
  "type": "Copy",
  "source": {
    "type": "SqlServerSource",
    "sqlReaderQuery": "SELECT * FROM source_transactions WHERE load_date >= '@{pipeline().parameters.start_date}'"
  },
  "sink": {
    "type": "AzureSqlSink",
    "writeBehavior": "upsert",
    "upsertSettings": {
      "useTempDB": true,
      "keys": ["order_id", "client_id"]
    }
  }
}
```

### 2️⃣ Databricks Activity — Data Transformation
```json
{
  "name": "TransformData",
  "type": "DatabricksNotebook",
  "notebook": "/notebooks/data_transformation",
  "parameters": {
    "start_date": "@{pipeline().parameters.start_date}",
    "end_date": "@{pipeline().parameters.end_date}",
    "client_id": "@{pipeline().parameters.client_id}"
  }
}
```

### 3️⃣ Stored Procedure Activity — Refresh Data Mart
```json
{
  "name": "RefreshDataMart",
  "type": "SqlServerStoredProcedure",
  "storedProcedureName": "usp_refresh_business_mart",
  "storedProcedureParameters": {
    "load_date": "@{pipeline().parameters.start_date}"
  }
}
```

---

## ✅ Pipeline Best Practices

- Always use **parameters** instead of hardcoded values
- Add **retry logic** — set retry count to 3 with 2 min delay
- Use **pipeline variables** to track run status
- Send **email alerts** on pipeline failure
- Log every run to **audit table** for tracking
- Use **tumbling window triggers** for incremental loads

---

## 📈 Results
- ⬇️ Reduced manual data refresh steps by **73%**
- ⬆️ Increased automation efficiency by **35%**
- ⬇️ Reduced reporting latency by **40%**
