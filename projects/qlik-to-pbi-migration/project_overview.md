# 🔄 Project: Migration from Qlik to Power BI

> Enterprise-scale BI migration project built on 
> Medallion Architecture using Oracle DB, Azure Data 
> Factory, Azure Databricks and Power BI

---

## 📋 Project Summary

| Item | Details |
|---|---|
| 🎯 Goal | Migrate reporting from Qlik to Power BI |
| 🗄️ Source | Oracle Database |
| 🏗️ Architecture | Medallion (Raw → Silver → Gold) |
| ☁️ Cloud | Microsoft Azure |
| 📊 BI Tool | Power BI |
| 🔢 Silver Tables | 60 tables |
| 🥇 Gold Tables | 35 tables |

---

## 🏗️ Medallion Architecture
```
┌─────────────────────────────────────────────┐
│              SOURCE SYSTEM                   │
│           Oracle Database                    │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│              RAW LAYER                       │
│     Azure Data Lake Storage (ADLS)           │
│         Parquet Files (.parquet)             │
│                                             │
│  • Full load tables                         │
│  • Incremental load tables                  │
│  • Daily scheduled via ADF                  │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│             SILVER LAYER                     │
│          Azure Databricks                    │
│           Delta Tables (60)                  │
│                                             │
│  • Read from ADLS parquet files             │
│  • Written to Delta tables                  │
│  • Checkpoints defined                      │
│  • Job clusters for scheduling              │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│              GOLD LAYER                      │
│          Azure Databricks                    │
│       Transformed Tables (35)                │
│                                             │
│  • Built on top of silver tables            │
│  • Transformations per PBI team needs       │
│  • Serverless cluster for querying          │
│  • Ready for Power BI consumption           │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│           CONSUMPTION LAYER                  │
│             Power BI Dashboards              │
│                                             │
│  • Connected to Gold layer                  │
│  • Migrated from Qlik to Power BI           │
│  • Business KPI reporting                   │
└─────────────────────────────────────────────┘
```

---

## ⚙️ Azure Data Factory — Pipeline Design

### Pipeline Types

| Pipeline | Load Type | Trigger | Frequency |
|---|---|---|---|
| pl_full_load | Full Load | Schedule Trigger | Daily |
| pl_incremental_load | Incremental Load | Tumbling Window | Daily |

### Key Design Decisions
- ✅ Used **Copy Data Activity** only in ADF
- ✅ **Scheduling triggers** for full load tables
- ✅ **Tumbling Window triggers** for incremental tables
- ✅ Data written as **Parquet files** to ADLS
- ✅ Pipelines run **daily** to keep data fresh

### Why Parquet?
- 🚀 Faster read performance than CSV
- 📦 Compressed — uses less storage
- ✅ Schema preserved automatically
- ⚡ Optimized for Databricks processing

---

## ⚡ Azure Databricks — Workflow Design

### Cluster Strategy

| Cluster Type | Used For |
|---|---|
| Job Cluster | Scheduled workflows (Silver & Gold) |
| Serverless Cluster | Ad-hoc querying of Delta tables |

### Workflow Steps
```
1. Read Parquet files from ADLS (Raw Layer)
        ↓
2. Apply schema validation & cleaning
        ↓
3. Write to Delta Tables (Silver Layer - 60 tables)
        ↓
4. Apply transformations per PBI team requirements
        ↓
5. Write transformed tables (Gold Layer - 35 tables)
        ↓
6. Power BI connects to Gold layer for reporting
```

### Key Features
- ✅ **Checkpoints** defined for fault tolerance
- ✅ **Delta tables** for ACID transactions
- ✅ **Job clusters** for cost efficiency
- ✅ **Serverless clusters** for quick querying

---

## 📊 Data Flow Summary

| Layer | Storage | Format | Tables |
|---|---|---|---|
| Raw | ADLS Containers | Parquet | ~60 |
| Silver | Databricks Delta | Delta Tables | 60 |
| Gold | Databricks Delta | Delta Tables | 35 |

> 💡 Note: Oracle DB data = Raw Parquet files = Silver Delta tables
> (same data copy across layers for consistency)

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Oracle Database | Source system |
| Azure Data Factory | Pipeline orchestration |
| ADLS (Azure Data Lake) | Raw layer storage |
| Azure Databricks | Data transformation |
| Delta Lake | Silver & Gold layer storage |
| Power BI | Business reporting & dashboards |
| Parquet | File format for raw layer |

---

## 💡 Key Learnings

- Tumbling window triggers are ideal for **incremental loads** as they track processing windows
- **Checkpoints in Databricks** prevent reprocessing of already loaded data
- **Job clusters** are more cost effective than all-purpose clusters for scheduled jobs
- **Medallion architecture** gives clear separation of raw, clean and business-ready data
- **Parquet format** significantly reduces storage cost and improves read speed

---

## 🤝 Connect with Me
- 💼 [LinkedIn](https://linkedin.com/in/sadhana-s-kumar)
- 📧 sanasadhana07@gmail.com
- 🐙 [GitHub Profile](https://github.com/sadhana007)
