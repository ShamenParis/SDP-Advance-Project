# Lakeflow Connect — Ingestion Reference

> Source: https://docs.databricks.com/aws/en/ingestion/overview

## What is Lakeflow Connect?

Lakeflow Connect is Databricks' managed ingestion service. It provides
pre-built connectors that automatically move data from external systems
into Delta Lake tables — without users writing ETL code for the Bronze layer.

It replaces the need for `cloudFiles` Auto Loader when the data source is
a SaaS application, database, or message bus.

---

## Supported Source Types

| Category | Examples |
|---|---|
| **SaaS Applications** | Salesforce, Workday, ServiceNow, HubSpot |
| **Databases** | PostgreSQL, MySQL, SQL Server, Oracle (via CDC) |
| **Event Streams** | Kafka (managed), Kinesis |
| **File-based** | S3, ADLS Gen2, GCS via External Locations / Volumes |

---

## When to use Lakeflow Connect vs Auto Loader

| Use Lakeflow Connect | Use Auto Loader (cloudFiles) |
|---|---|
| Source is a SaaS app / relational DB | Source is files dropped into cloud storage |
| You need low-latency CDC replication | Batch file ingestion (hourly / daily drops) |
| No custom code preferred | Need full control of schema evolution |
| Databricks manages connector auth | You manage the file landing zone |

---

## How it affects the SDP Architecture

When `source_type` is **`lakeflow_connect`** in `sdp_config.json`:

- **Bronze layer is connector-managed** — Lakeflow Connect writes directly
  to the raw Delta tables. No `raw_ingestion_batch.py` or `config.json` is generated.
- The SDP pipeline **starts at Silver** — the `datalake_clean.py` reads from
  the raw tables populated by the connector.
- The `sdp_config.json` still needs `raw_table` names so that Silver and Gold
  code can reference the correct source tables.

### Generated comment in Silver when using Lakeflow Connect

```python
# NOTE: Bronze layer (raw tables) managed by Lakeflow Connect.
# Ensure the connector is configured and running before executing this pipeline.
# Connector doc: https://docs.databricks.com/aws/en/ingestion/overview
```

---

## Using Volumes for File Ingestion

When `source_type` is **`files`**, the source path must be a Unity Catalog Volume
or an External Location registered in Databricks.

### Volume path format
```
/Volumes/<catalog>/<schema>/<volume_name>/<subfolder>/
```

Example:
```
/Volumes/data_team/raw/source/customers/
```

### External Location path format
```
abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/
s3://<bucket>/<path>/
gs://<bucket>/<path>/
```

> [!TIP]
> Volumes are recommended for new projects as they are managed by Unity Catalog
> and support fine-grained access control with no extra credential configuration.

---

## Auto Loader Schema Evolution

The generated Bronze code always includes:
```python
.option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
```

This means new columns in arriving files are **automatically added** to the
raw Delta table without pipeline failures. The Silver clean step will need to
be updated if new columns should be exposed in downstream layers.
