# ⚙️ ADF Pipeline Design — Qlik to PBI Migration

> Azure Data Factory pipelines for reading data from
> Oracle DB and writing Parquet files to ADLS

---

## 📋 Pipeline Overview

### Full Load Pipeline — `pl_oracle_full_load`
```json
{
  "name": "pl_oracle_full_load",
  "description": "Full load pipeline from Oracle DB to ADLS",
  "activities": [
    {
      "name": "CopyFullLoadTables",
      "type": "Copy",
      "source": {
        "type": "OracleSource",
        "oracleReaderQuery": "SELECT * FROM @{pipeline().parameters.table_name}",
        "partitionOption": "PhysicalPartitionsOfTable"
      },
      "sink": {
        "type": "ParquetSink",
        "storeSettings": {
          "type": "AzureBlobFSWriteSettings"
        },
        "formatSettings": {
          "type": "ParquetWriteSettings"
        }
      },
      "enableStaging": false
    }
  ],
  "parameters": {
    "table_name": {
      "type": "String"
    },
    "container_name": {
      "type": "String",
      "defaultValue": "raw"
    },
    "load_date": {
      "type": "String"
    }
  }
}
```

---

### Incremental Load Pipeline — `pl_oracle_incremental_load`
```json
{
  "name": "pl_oracle_incremental_load",
  "description": "Incremental load using Tumbling Window Trigger",
  "activities": [
    {
      "name": "CopyIncrementalData",
      "type": "Copy",
      "source": {
        "type": "OracleSource",
        "oracleReaderQuery": "SELECT * FROM @{pipeline().parameters.table_name} WHERE last_updated_date >= '@{pipeline().parameters.window_start}' AND last_updated_date < '@{pipeline().parameters.window_end}'"
      },
      "sink": {
        "type": "ParquetSink",
        "storeSettings": {
          "type": "AzureBlobFSWriteSettings"
        }
      }
    }
  ],
  "parameters": {
    "table_name": {
      "type": "String"
    },
    "window_start": {
      "type": "String"
    },
    "window_end": {
      "type": "String"
    }
  }
}
```

---

## ⏰ Trigger Design

### Schedule Trigger — Full Load
```json
{
  "name": "trigger_full_load_daily",
  "type": "ScheduleTrigger",
  "recurrence": {
    "frequency": "Day",
    "interval": 1,
    "startTime": "2024-01-01T02:00:00Z",
    "timeZone": "India Standard Time"
  }
}
```

### Tumbling Window Trigger — Incremental Load
```json
{
  "name": "trigger_incremental_tumbling",
  "type": "TumblingWindowTrigger",
  "recurrence": {
    "frequency": "Day",
    "interval": 1,
    "startTime": "2024-01-01T00:00:00Z"
  },
  "maxConcurrency": 1,
  "retryPolicy": {
    "count": 3,
    "intervalInSeconds": 300
  }
}
```

---

## 📂 ADLS Folder Structure
```
adls-container/
│
├── 📁 raw/
│   ├── 📁 full_load/
│   │   ├── 📁 table_name_1/
│   │   │   └── load_date=2024-01-01/
│   │   │       └── data.parquet
│   │   └── 📁 table_name_2/
│   │       └── load_date=2024-01-01/
│   │           └── data.parquet
│   │
│   └── 📁 incremental/
│       ├── 📁 table_name_1/
│       │   └── load_date=2024-01-01/
│       │       └── data.parquet
│       └── 📁 table_name_2/
│           └── load_date=2024-01-01/
│               └── data.parquet
```

---

## ✅ Best Practices Used

- ✅ **Parameterized pipelines** — one pipeline handles all tables
- ✅ **Tumbling window** for incremental to avoid data gaps
- ✅ **Parquet format** for efficient storage and fast reads
- ✅ **Partitioned folders** by load date for easy debugging
- ✅ **Retry policy** on triggers for fault tolerance
- ✅ **PhysicalPartitionsOfTable** for parallel Oracle reads
