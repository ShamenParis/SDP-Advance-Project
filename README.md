# Databricks SDP Generator — AI Skill

> **An Antigravity AI Skill that generates complete Bronze → Silver → Gold Structured Data Pipelines for Databricks — through a guided conversation.**

---

## What is this?

The **SDP Generator** is an AI skill that interviews you and then scaffolds a full,
production-ready Databricks SDP project in seconds. No boilerplate. No copy-pasting.
Just answer questions and get working code.

It generates the **entire medallion pipeline**:

| Layer | What's generated |
|---|---|
| 🥉 **Bronze** | Auto Loader (`cloudFiles`) ingestion via `config.json`-driven loop |
| 🥈 **Silver** | SCD Type 2 deduplication + `expect_or_fail/drop` quality rules + type casts |
| 🥇 **Gold** | Materialized view dimensions with surrogate keys + fact tables with referential integrity |
| 📦 **Bundle** | `databricks.yml` (dev + prod targets) + pipeline YAML + `pyproject.toml` |

Supports two ingestion modes:
- **Files** — CSV, JSON, Parquet via Databricks Volumes or External Locations (Auto Loader)
- **Lakeflow Connect** — connector-managed Bronze; pipeline starts at Silver

---

## Skill Structure

```
SDP-Advance-Project/
├── README.md                                        ← You are here
├── Industry Level SDP/                              ← Working reference SDP project
└── .agents/
    └── skills/
        └── databricks-sdp-generator/
            ├── SKILL.md                             ← AI instructions (5-phase interview + generation rules)
            ├── examples/
            │   └── sdp_config.example.json         ← Full working example (retail/e-commerce domain)
            ├── references/
            │   ├── databricks_sdp_patterns.md      ← All Bronze/Silver/Gold code templates
            │   └── lakeflow_connect.md             ← Lakeflow Connect vs Auto Loader guidance
            └── resources/
                ├── sdp_config.schema.json           ← JSON Schema — master config (all 3 layers)
                └── config_json.schema.json          ← JSON Schema — Bronze config.json
```

---

## How to Use the Skill

### Step 1 — Invoke the Skill

In an Antigravity chat session, type:

```
Use the databricks-sdp-generator skill to create a new SDP project
```

The AI will read all reference files and begin the **5-phase interview**.

### Step 2 — Answer 5 Phases of Questions

| Phase | Topics |
|---|---|
| **Phase 1 — Ingestion** | Files or Lakeflow Connect? Source tables, paths, formats |
| **Phase 2 — Project Identity** | Catalog name, schema names, workspace host, owner email |
| **Phase 3 — Clean Rules** | Primary keys, SCD type, data quality expectations, type casts |
| **Phase 4 — Star Schema** | Dimension columns/keys, fact-to-dimension join mappings |
| **Phase 5 — Generate** | Review summary → AI generates all files |

### Step 3 — Review & Deploy

```bash
cd <your_project_name>

# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Run the pipeline
databricks bundle run <project_name>_etl
```

### Regenerating After Changes

When you add a new source table or change a schema, update `sdp_config.json` and run:

```
Use the databricks-sdp-generator skill to regenerate my SDP project
```

The AI reads your existing `sdp_config.json` and regenerates all code files, skipping
the interview.

---

## What Gets Generated

```
<project_name>/
├── databricks.yml                    ← DAB bundle (dev + prod)
├── pyproject.toml
├── .gitignore
├── README.md
├── sdp_config.json                   ← YOUR master config (maintain this)
├── resources/
│   └── <project_name>_pipeline.pipeline.yml
└── src/
    ├── 01_bronze/
    │   ├── config.json               ← Table definitions for Auto Loader
    │   └── raw_ingestion_batch.py    ← cloudFiles streaming ingestion
    ├── 02_silver/
    │   └── datalake_clean.py         ← SCD2 + expectations + casts
    └── 03_gold/
        ├── dimensions.py             ← Materialized views with surrogate keys
        └── facts.py                  ← Fact MVs with dim joins + coalesce guards
```

---

## The `sdp_config.json` — Your Master Config

This is the **single file you maintain**. All generated code is derived from it.

```jsonc
{
  "project": { "name": "...", "catalog": "...", "schemas": {...} },
  "ingestion": { "source_type": "files", "tables": [...] },   // Bronze
  "clean":     { "tables": [...] },                           // Silver
  "gold":      { "dimensions": [...], "facts": [...] }        // Gold
}
```

See [`sdp_config.example.json`](.agents/skills/databricks-sdp-generator/examples/sdp_config.example.json) for a
complete working example (retail domain with customers, orders, sales, products).

See [`sdp_config.schema.json`](.agents/skills/databricks-sdp-generator/resources/sdp_config.schema.json) for the
full field reference with descriptions and validation rules.

---

## Sharing via Unity Catalog

Unity Catalog natively supports AI Skills as governed data assets, allowing you to publish, govern, and share this skill with your entire team across workspaces without needing Volumes or manual file distribution.


### Team Discovery & Workspace Usage

Once published to Unity Catalog:
- **Discoverable**: The skill appears in Catalog Explorer under your schema alongside tables, volumes, and functions.
- **Direct Workspace Integration**: Team members and Databricks Genie Code / AI Assistant can discover and invoke `<catalog>.<schema>.databricks_sdp_generator` directly across any connected workspace.
- **Centralized Versioning**: Updates made to the cataloged skill asset are immediately available to all workspace users without manual syncing.

---

## Data Quality Expectations Reference

| Decorator | Behaviour | When to use |
|---|---|---|
| `@dp.expect_or_fail` | **Blocks** pipeline on violation | Critical NOT NULL columns (primary keys) |
| `@dp.expect` | **Warns** only — pipeline continues | Soft checks, audit logging |
| `@dp.expect_or_drop` | **Drops** offending rows silently | Optional enrichment columns |
| `@dp.expect_all(dict)` | Batch **warn** rules | Multiple warn rules in one block |
| `@dp.expect_all_or_drop(dict)` | Batch **drop** rules | Multiple drop rules in one block |

---

## SCD Type Selection Guide

| Use SCD Type 2 | Use SCD Type 1 (Streaming) |
|---|---|
| Customers, products, warehouses | Orders, sales, events, transactions |
| Data changes slowly | High-volume append-only |
| History tracking required | Latest state only needed |
| Generates `create_auto_cdc_from_snapshot_flow` | Generates `@dp.table` + `dp.read_stream` |

---

## Links

- [Databricks SDP Documentation](https://docs.databricks.com/aws/en/dlt/index.html)
- [Lakeflow Connect Overview](https://docs.databricks.com/aws/en/ingestion/overview)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [SDP Quality Expectations](https://docs.databricks.com/aws/en/dlt/expectations.html)

---

## Contributing

1. Update the skill files locally under `.agents/skills/databricks-sdp-generator/`
2. Test by invoking the skill in a fresh conversation
3. Push changes to Git and re-upload to the shared Unity Catalog Volume
4. Notify the team so they can pull the latest version

---
