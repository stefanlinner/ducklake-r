#' Create or attach a ducklake
#'
#' Wrapper for the ducklake [ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting) command.
#' Creates a new DuckLake if the specified name does not exist, or connects to
#' an existing one. The connection is stored in the package environment and can
#' be closed with [detach_ducklake()].
#'
#' By default DuckDB is used as the catalog database. Alternative backends
#' (PostgreSQL, SQLite, MySQL) can be selected with the `backend` parameter,
#' which enables concurrent multi-client access.
#' See \url{https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database}.
#'
#' @param ducklake_name Name for the ducklake, used as the database alias in DuckDB
#' @param lake_path Directory path where the lake lives. For `"duckdb"` this is
#'   where the catalog file and Parquet data are stored. For other backends this
#'   sets the Parquet data location (DuckLake's `DATA_PATH`).
#' @param backend Catalog backend: `"duckdb"` (default), `"postgres"`,
#'   `"sqlite"`, or `"mysql"`.
#' @param catalog_connection_string Backend-specific connection string:
#'   \describe{
#'     \item{`"duckdb"`}{Not required. Defaults to `{ducklake_name}.ducklake`.}
#'     \item{`"postgres"`}{libpq string, e.g. `"dbname=mydb host=localhost"`.}
#'     \item{`"sqlite"`}{Path to the SQLite file, e.g. `"metadata.sqlite"`.}
#'     \item{`"mysql"`}{MySQL connection string, e.g. `"db=mydb host=localhost"`.}
#'   }
#' @param read_only Attach in read-only mode (default `FALSE`).
#' @param override_data_path Override the stored DATA_PATH in the catalog
#'   (default `FALSE`). Needed when restoring a backup to a different location.
#'
#' @details
#' For credential management with PostgreSQL or MySQL, consider DuckDB's
#' built-in secrets manager instead of embedding credentials in the connection
#' string:
#'
#' \preformatted{conn <- get_ducklake_connection()
#' DBI::dbExecute(conn, "CREATE SECRET (
#'     TYPE postgres,
#'     HOST '127.0.0.1',
#'     PORT 5432,
#'     DATABASE ducklake_catalog,
#'     USER 'analyst',
#'     PASSWORD 'secret'
#' )")}
#'
#' Then pass an empty or partial `catalog_connection_string`; DuckDB fills in
#' the rest from the secret. See
#' \url{https://duckdb.org/docs/stable/configuration/secrets_manager}.
#'
#' **Windows limitation:** The `postgres` and `mysql` DuckDB extensions are not
#' available on Windows (MinGW toolchain). Only `duckdb` and `sqlite` backends
#' work there. Use Linux, macOS, or WSL for PostgreSQL/MySQL backends.
#' See \url{https://github.com/duckdb/duckdb/issues/7892}.
#'
#' @returns NULL
#' @export
#'
#' @seealso [detach_ducklake()], [install_ducklake()]
#'
#' @examples
#' \dontrun{
#' # DuckDB catalog (default)
#' attach_ducklake("my_lake", lake_path = "~/data/lake")
#'
#' # PostgreSQL catalog
#' attach_ducklake(
#'   "my_lake",
#'   backend = "postgres",
#'   catalog_connection_string = "dbname=ducklake_catalog host=localhost",
#'   lake_path = "/shared/lake/data/"
#' )
#'
#' # SQLite catalog
#' attach_ducklake(
#'   "my_lake",
#'   backend = "sqlite",
#'   catalog_connection_string = "metadata.sqlite",
#'   lake_path = "data_files/"
#' )
#'
#' # MySQL catalog
#' attach_ducklake(
#'   "my_lake",
#'   backend = "mysql",
#'   catalog_connection_string = "db=ducklake_catalog host=localhost",
#'   lake_path = "data_files/"
#' )
#' }
attach_ducklake <- function(ducklake_name, lake_path,
                             backend = c("duckdb", "postgres", "sqlite", "mysql"),
                             catalog_connection_string = NULL,
                             read_only = FALSE,
                             override_data_path = FALSE) {
  backend <- match.arg(backend)
  
  if (missing(lake_path) || is.null(lake_path)) {
    cli::cli_abort(c(
      "A {.arg lake_path} is required.",
      "i" = "This specifies the directory where the catalog and Parquet data files are stored.",
      "i" = "Example: {.code attach_ducklake(\"{ducklake_name}\", lake_path = \"path/to/lake\")}"
    ))
  }
  
  # Non-DuckDB backends also need a connection string
  if (backend != "duckdb") {
    if (is.null(catalog_connection_string)) {
      cli::cli_abort(c(
        "A {.arg catalog_connection_string} is required for the {.val {backend}} backend.",
        "i" = "See {.url https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database} for connection string formats."
      ))
    }
  }
  
  if (backend == "mysql") {
    cli::cli_warn(c(
      "MySQL has known issues as a DuckLake catalog backend.",
      "i" = "See {.url https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database#mysql} for details."
    ))
  }
  
  # Get connection - if it's invalid/closed, create a new one
  conn <- get_ducklake_connection()
  
  # Check if connection is valid, if not create a new one
  is_valid <- tryCatch({
    DBI::dbIsValid(conn)
  }, error = function(e) FALSE)
  
  if (!is_valid) {
    # Create a completely new DuckDB connection
    conn <- DBI::dbConnect(duckdb::duckdb())
    set_ducklake_connection(conn, backend = backend,
                            catalog_connection_string = catalog_connection_string)
  } else {
    # Don't store the connection -- it may be duckplyr's singleton which
    # we must not own. Just update the backend metadata.
    .ducklake_env$backend <- backend
    .ducklake_env$catalog_connection_string <- catalog_connection_string
  }
  
  # Check if this ducklake is already attached to avoid conflicts
  # Query the list of attached databases
  attached <- tryCatch({
    DBI::dbGetQuery(conn, "SELECT database_name FROM duckdb_databases();")$database_name
  }, error = function(e) character(0))
  
  if (ducklake_name %in% attached) {
    # Already attached - just switch to it
    ducklake_db_exec(sprintf("USE %s;", ducklake_name))
    return(invisible(NULL))
  }
  
  # Load required extensions (ducklake + backend-specific)
  ensure_extensions(backend)
  
  # Build and run the ATTACH command
  attach_sql <- build_attach_sql(ducklake_name, lake_path, backend,
                                  catalog_connection_string, read_only,
                                  override_data_path)
  ducklake_db_exec(attach_sql)
  ducklake_db_exec(sprintf("USE %s;", ducklake_name))
  
  invisible(NULL)
}

#' Install and load required DuckDB extensions for a given backend
#'
#' @param backend Catalog backend type
#' @keywords internal
ensure_extensions <- function(backend) {
  tryCatch({
    ducklake_db_exec("LOAD ducklake;")
  }, error = function(e) {
    ducklake_db_exec("INSTALL ducklake;")
    ducklake_db_exec("LOAD ducklake;")
  })
  
  ext <- switch(backend,
    postgres = "postgres",
    sqlite = "sqlite",
    mysql = "mysql",
    NULL
  )
  
  if (!is.null(ext)) {
    tryCatch({
      ducklake_db_exec(sprintf("LOAD %s;", ext))
    }, error = function(e) {
      ducklake_db_exec(sprintf("INSTALL %s;", ext))
      ducklake_db_exec(sprintf("LOAD %s;", ext))
    })
  }
}

#' Build the ATTACH SQL for a DuckLake
#'
#' @param ducklake_name Name for the ducklake alias
#' @param lake_path Path for data files
#' @param backend Catalog backend type
#' @param catalog_connection_string Backend-specific connection string
#' @param read_only Whether to attach in read-only mode
#' @param override_data_path Whether to add OVERRIDE_DATA_PATH TRUE
#'
#' @returns A SQL ATTACH statement string
#' @keywords internal
build_attach_sql <- function(ducklake_name, lake_path, backend,
                              catalog_connection_string, read_only,
                              override_data_path = FALSE) {
  connection_string <- switch(backend,
    duckdb = {
      ducklake_path <- file.path(lake_path, paste0(ducklake_name, ".ducklake"))
      sprintf("ducklake:%s", ducklake_path)
    },
    postgres = sprintf("ducklake:postgres:%s", catalog_connection_string),
    sqlite = sprintf("ducklake:sqlite:%s", catalog_connection_string),
    mysql = sprintf("ducklake:mysql:%s", catalog_connection_string)
  )
  
  options <- character()
  
  if (!is.null(lake_path)) {
    options <- c(options, sprintf("DATA_PATH '%s'", lake_path))
  }
  
  if (read_only) {
    options <- c(options, "READ_ONLY")
  }

  if (override_data_path) {
    options <- c(options, "OVERRIDE_DATA_PATH TRUE")
  }
  
  if (length(options) > 0) {
    options_str <- paste(options, collapse = ", ")
    sprintf("ATTACH '%s' AS %s (%s);", connection_string, ducklake_name, options_str)
  } else {
    sprintf("ATTACH '%s' AS %s;", connection_string, ducklake_name)
  }
}
