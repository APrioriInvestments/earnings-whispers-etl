# earnings-whispers-etl

Template repo for Databricks ETL projects at A Priori, cloned by `dbxkit init`.

Two planes, and the split never moves. **Containers** — catalogs, schemas, volumes,
identities and grants — belong to
[databricks-account-iac](https://github.com/APrioriInvestments/databricks-account-iac).
**Jobs** belong here, in a Databricks Asset Bundle. A bundle must never be able to destroy
a catalog, which is why this template has no notebook that creates one.

## Before this repo can deploy

A `ProjectSpec` for the project must be merged into `databricks-account-iac` and deployed.
That creates the catalogs, the schemas and volumes, the per-environment ETL service
principal, and the `<project>-<env>` GitHub Environment holding `DEPLOY_CLIENT_ID`.

Then replace the three `REPLACE_ME_DEPLOY_PRINCIPAL` values in `databricks.yml` with each
environment's `DEPLOY_CLIENT_ID`. They cannot be known before that deploy, and until they
are filled in `bundle validate -t stg` fails on the placeholder:

```
unable to create directory at /Workspace/Users/REPLACE_ME_DEPLOY_PRINCIPAL/...
```

`dbxkit lint` reports the same thing without needing a workspace.

## Layout

- `databricks.yml`: the bundle — targets, variables, artifacts
- `resources/*.job.yml`: one file per job
- `earnings_whispers_etl/`: package code, installed on the clusters as a wheel
- `pins/pins.<target>.yaml`: pin config, synced and named by `EARNINGS_WHISPERS_ETL_PINS_CONFIG`
- `cluster_requirements.txt`, `metrics.py`: synced to the workspace, not uploaded to a volume
- `.github/workflows/*.yml.template`: the deploy workflows. Suffixed so they do not run
  in this repo; `dbxkit init` strips the suffix
- `vendor/databricks-monitoring`: submodule supplying the shared monitoring job

Clone with `--recurse-submodules`, or run `git submodule update --init` after checkout.

## Deploying

```bash
databricks bundle validate            # -t dev is the default
databricks bundle plan -t stg         # `create` for something live means a bind is missing
databricks bundle deploy              # dev, from your machine
```

Staging deploys from a pull request, production on merge to `main`. Neither needs a tag or
a local `pulumi up`.

Three jobs: `etl`, `decommissioning` (drops the example *tables*, never the containers) and
`monitoring`, included from the submodule rather than copied so a fleet change arrives by
bumping a pointer.

## How a job knows where it is

Every cluster carries `DEPLOY_ENV: ${bundle.target}`, and `constants.DEFAULT_CATALOG`
derives the catalog from it. Environment is a property of the workspace, so one value is
correct on a shared cluster as well as on this bundle's own. `DATABRICKS_CATALOG` remains an
override for pointing one run at another catalog; it is not how environment is resolved.

`pause_status` is never stated in a job. Each target's `trigger_pause_status` preset sets
it — `PAUSED` in dev and stg, `UNPAUSED` in prod — and a value in the job would win over the
preset, which is how a dev job ends up running for real.

## Updating cluster dependencies

The pinned cluster dependencies live in `cluster_requirements.txt`, compiled from the
`[dependency-groups] cluster` section in `pyproject.toml`:

```bash
dbxkit dependencies compile
```

Commit the regenerated file; don't hand-edit it.

## Example jobs

`resources/etl.job.yml` runs a small medallion example:

1. bronze product ingest
2. silver curation
3. gold category summary

The notebooks are deliberately simple, but they show the repo contract we want
future ETLs to inherit.

## Pin usage auditing

The example ETL registers an audit table pin at `eng.pin_audit` and records pin dereferences from the bronze, silver, and gold
notebooks.

The template passes stable task metadata through job parameters so
`datapins.record_pin_usage(...)` does not depend solely on Databricks runtime
introspection:

- `audit_pipeline`
- `audit_task_key`
- `audit_notebook_path`

That metadata is merged with Databricks job/run metadata inferred from Spark
conf when available.
