"""Simple Delta merge helper for batch ETL notebooks."""

from __future__ import annotations

from uuid import uuid4


def upsert_table(
    *,
    df,
    table_name: str,
    key_columns: list[str],
    full_refresh: bool = False,
) -> None:
    """Overwrite or merge a DataFrame into a Delta table."""
    spark = df.sparkSession

    if full_refresh or not spark.catalog.tableExists(table_name):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        return

    temp_view_name = f"_upsert_{uuid4().hex}"
    df.createOrReplaceTempView(temp_view_name)

    merge_predicate = " AND ".join(
        f"target.{column} = source.{column}" for column in key_columns
    )
    update_assignments = ", ".join(
        f"{column} = source.{column}" for column in df.columns
    )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {temp_view_name} AS source
        ON {merge_predicate}
        WHEN MATCHED THEN UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    spark.catalog.dropTempView(temp_view_name)
