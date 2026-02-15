# Multi-backend catalog support
# Only DuckDB and SQLite backends are tested since they need no external infra.

# --- Input validation ---

test_that("attach_ducklake requires catalog_connection_string for non-DuckDB backends", {
  skip_if_not_installed("duckdb")

  expect_error(
    attach_ducklake("test", backend = "postgres"),
    "catalog_connection_string"
  )
  expect_error(
    attach_ducklake("test", backend = "sqlite"),
    "catalog_connection_string"
  )
  expect_error(
    attach_ducklake("test", backend = "mysql"),
    "catalog_connection_string"
  )
})

test_that("attach_ducklake requires lake_path for non-DuckDB backends", {
  skip_if_not_installed("duckdb")

  expect_error(
    attach_ducklake(
      "test",
      backend = "sqlite",
      catalog_connection_string = "test.sqlite"
    ),
    "lake_path"
  )
  expect_error(
    attach_ducklake(
      "test",
      backend = "postgres",
      catalog_connection_string = "dbname=test"
    ),
    "lake_path"
  )
})

test_that("attach_ducklake rejects invalid backend values", {
  skip_if_not_installed("duckdb")

  expect_error(
    attach_ducklake("test", backend = "oracle")
  )
})

# --- Backend metadata ---

test_that("get_ducklake_backend returns 'duckdb' by default", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  expect_equal(get_ducklake_backend(), "duckdb")

  cleanup_temp_ducklake(lake)
})

# --- SQL generation ---

test_that("build_attach_sql generates correct SQL for DuckDB backend", {
  # Without lake_path
  sql <- ducklake:::build_attach_sql("my_lake", NULL, "duckdb", NULL, FALSE)
  expect_true(grepl("ducklake:my_lake.ducklake", sql, fixed = TRUE))
  expect_false(grepl("DATA_PATH", sql))
  expect_false(grepl("READ_ONLY", sql))

  # With lake_path
  sql <- ducklake:::build_attach_sql("my_lake", "/data", "duckdb", NULL, FALSE)
  expect_true(grepl("ducklake:", sql))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))

  # With read_only
  sql <- ducklake:::build_attach_sql("my_lake", NULL, "duckdb", NULL, TRUE)
  expect_true(grepl("READ_ONLY", sql))
})

test_that("build_attach_sql generates correct SQL for PostgreSQL backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "postgres",
    "dbname=catalog host=localhost",
    FALSE
  )
  expect_true(grepl(
    "ducklake:postgres:dbname=catalog host=localhost",
    sql,
    fixed = TRUE
  ))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql generates correct SQL for SQLite backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "sqlite",
    "metadata.sqlite",
    FALSE
  )
  expect_true(grepl("ducklake:sqlite:metadata.sqlite", sql, fixed = TRUE))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql generates correct SQL for MySQL backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "mysql",
    "db=catalog host=localhost",
    FALSE
  )
  expect_true(grepl(
    "ducklake:mysql:db=catalog host=localhost",
    sql,
    fixed = TRUE
  ))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql combines DATA_PATH and READ_ONLY options", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "postgres",
    "dbname=catalog",
    TRUE
  )
  expect_true(grepl("DATA_PATH", sql))
  expect_true(grepl("READ_ONLY", sql))
})

test_that("build_attach_sql includes OVERRIDE_DATA_PATH when requested", {
  sql <- ducklake:::build_attach_sql(
    "my_lake", "/backup/data", "duckdb", NULL, FALSE,
    override_data_path = TRUE
  )
  expect_true(grepl("OVERRIDE_DATA_PATH TRUE", sql, fixed = TRUE))
  expect_true(grepl("DATA_PATH", sql))

  # Not included by default
  sql2 <- ducklake:::build_attach_sql("my_lake", "/data", "duckdb", NULL, FALSE)
  expect_false(grepl("OVERRIDE_DATA_PATH", sql2))
})

# --- SQLite backend end-to-end ---

test_that("SQLite backend: create table, query, and time travel", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  temp_dir <- tempfile("test_sqlite")
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  sqlite_catalog <- file.path(temp_dir, "metadata.sqlite")
  sqlite_data <- file.path(temp_dir, "data")
  dir.create(sqlite_data, showWarnings = FALSE, recursive = TRUE)

  tryCatch(
    {
      attach_ducklake(
        "test_sqlite_backend",
        backend = "sqlite",
        catalog_connection_string = sqlite_catalog,
        lake_path = sqlite_data
      )

      expect_equal(get_ducklake_backend(), "sqlite")

      # Create a table
      with_transaction(
        create_table(mtcars, "cars"),
        author = "Test",
        commit_message = "Initial load"
      )

      result <- get_ducklake_table("cars") |> dplyr::collect()
      expect_equal(nrow(result), 32)
      expect_true("mpg" %in% names(result))

      expect_true(file.exists(sqlite_catalog))

      snapshots <- list_table_snapshots("cars")
      expect_equal(nrow(snapshots), 1)

      # Replace table and check second snapshot
      with_transaction(
        {
          get_ducklake_table("cars") |>
            dplyr::mutate(kpl = mpg * 0.425144) |>
            replace_table("cars")
        },
        author = "Test",
        commit_message = "Add kpl column"
      )

      result2 <- get_ducklake_table("cars") |> dplyr::collect()
      expect_true("kpl" %in% names(result2))

      snapshots2 <- list_table_snapshots("cars")
      expect_equal(nrow(snapshots2), 2)

      # Time travel: version 1 should not have kpl
      v1 <- get_ducklake_table_version("cars", snapshots2$snapshot_id[1]) |>
        dplyr::collect()
      expect_false("kpl" %in% names(v1))

      # Full shutdown so SQLite state doesn't leak into other tests
      detach_ducklake("test_sqlite_backend", shutdown = TRUE)
    },
    finally = {
      unlink(temp_dir, recursive = TRUE)
    }
  )
})
