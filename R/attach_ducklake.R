#' Create or attach a ducklake
#'
#' This function is a wrapper for the ducklake [ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting) command.
#' It will create a new DuckLake if the specified name does not exist, or connect to
#' the existing DuckLake if it does exist. The connection is stored in the package
#' environment and can be closed with \code{detach_ducklake()}.
#'
#' By default, DuckDB is used as the catalog database. Alternative catalog backends
#' (PostgreSQL, SQLite, MySQL) can be selected via the \code{backend} parameter.
#' Using an external catalog backend enables multiple clients to read and write to
#' the same DuckLake concurrently, which is not possible with the DuckDB backend.
#'
#' See \url{https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database}
#' for guidance on choosing a catalog backend.
#'
#' @param ducklake_name Name for the ducklake, used as the database alias in DuckDB
#' @param lake_path Optional directory path for the ducklake. For the default
#'   \code{"duckdb"} backend, this is where the catalog database file and Parquet
#'   data files will be stored. For other backends, this specifies the storage
#'   location for Parquet data files (corresponding to DuckLake's \code{DATA_PATH}
#'   parameter).
#' @param backend Catalog database backend to use. One of \code{"duckdb"} (default),
#'   \code{"postgres"}, \code{"sqlite"}, or \code{"mysql"}. Non-DuckDB backends
#'   allow multiple clients to access the DuckLake concurrently.
#' @param catalog_connection_string Backend-specific connection string for the
#'   catalog database. Interpretation depends on \code{backend}:
#'   \describe{
#'     \item{\code{"duckdb"}}{Not required. Defaults to \code{"{ducklake_name}.ducklake"}.}
#'     \item{\code{"postgres"}}{A libpq connection string, e.g.,
#'       \code{"dbname=ducklake_catalog host=localhost port=5432 user=analyst"}.
#'       The target database must already exist in PostgreSQL.}
#'     \item{\code{"sqlite"}}{Path to the SQLite file, e.g., \code{"metadata.sqlite"}.}
#'     \item{\code{"mysql"}}{A MySQL connection string, e.g.,
#'       \code{"db=ducklake_catalog host=localhost user=analyst"}.
#'       The target database must already exist in MySQL.}
#'   }
#' @param read_only Logical. Whether to attach in read-only mode (default \code{FALSE}).
#'
#' @section Credential Management:
#' For secure credential management with PostgreSQL or MySQL backends, consider
#' using DuckDB's built-in secrets manager instead of embedding credentials in the
#' connection string. Secrets can be created via \code{DBI::dbExecute()} on the
#' DuckLake connection before calling \code{attach_ducklake()}:
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
#' Then pass an empty or partial \code{catalog_connection_string} — DuckDB will
#' fill in the missing values from the secret. Persistent secrets (using
#' \code{CREATE PERSISTENT SECRET}) survive across sessions.
#'
#' See \url{https://duckdb.org/docs/stable/configuration/secrets_manager} for details.
#'
#' @returns NULL
#' @export
#'
#' @seealso [detach_ducklake()] to close the connection, [install_ducklake()] to
#'   install required DuckDB extensions
#'
#' @examples
#' \dontrun{
#' # Default: DuckDB catalog (single-client)
#' attach_ducklake("my_lake")
#'
#' # DuckDB catalog with custom data path
#' attach_ducklake("my_lake", lake_path = "~/data/lake")
#'
#' # PostgreSQL catalog (multi-client)
#' attach_ducklake(
#'   "my_lake",
#'   backend = "postgres",
#'   catalog_connection_string = "dbname=ducklake_catalog host=localhost",
#'   lake_path = "/shared/lake/data/"
#' )
#'
#' # SQLite catalog (multi-process local)
#' attach_ducklake(
#'   "my_lake",
#'   backend = "sqlite",
#'   catalog_connection_string = "metadata.sqlite",
#'   lake_path = "data_files/"
#' )
#'
#' # MySQL catalog (multi-client)
#' attach_ducklake(
#'   "my_lake",
#'   backend = "mysql",
#'   catalog_connection_string = "db=ducklake_catalog host=localhost",
#'   lake_path = "data_files/"
#' )
#' }
attach_ducklake <- function(ducklake_name, lake_path = NULL,
                             backend = c("duckdb", "postgres", "sqlite", "mysql"),
                             catalog_connection_string = NULL,
                             read_only = FALSE) {
  backend <- match.arg(backend)
  
  # Validate inputs for non-DuckDB backends
  if (backend != "duckdb") {
    if (is.null(catalog_connection_string)) {
      cli::cli_abort(c(
        "A {.arg catalog_connection_string} is required for the {.val {backend}} backend.",
        "i" = "See {.url https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database} for connection string formats."
      ))
    }
    if (is.null(lake_path)) {
      cli::cli_abort(c(
        "A {.arg lake_path} is required for the {.val {backend}} backend.",
        "i" = "This specifies where Parquet data files will be stored (DuckLake's DATA_PATH)."
      ))
    }
  }
  
  # Warn about known MySQL issues
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
    # Only update backend metadata without storing the connection reference.
    # If the connection came from duckplyr's fallback (i.e., .ducklake_env$connection
    # is NULL), we must NOT store it - otherwise detach_ducklake() would shut down
    # duckplyr's shared singleton, breaking subsequent re-attaches.
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
  
  # Ensure required DuckDB extensions are installed and loaded
  ensure_extensions(backend)
  
  # Build and execute the ATTACH command
  attach_sql <- build_attach_sql(ducklake_name, lake_path, backend,
                                  catalog_connection_string, read_only)
  ducklake_db_exec(attach_sql)
  ducklake_db_exec(sprintf("USE %s;", ducklake_name))
  
  invisible(NULL)
}

#' Ensure required DuckDB extensions are installed and loaded
#'
#' @param backend The catalog backend type
#' @keywords internal
ensure_extensions <- function(backend) {
  # ducklake is always required
  tryCatch({
    ducklake_db_exec("LOAD ducklake;")
  }, error = function(e) {
    ducklake_db_exec("INSTALL ducklake;")
    ducklake_db_exec("LOAD ducklake;")
  })
  
  # Load backend-specific extension
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

#' Build the ATTACH SQL statement for a DuckLake
#'
#' @param ducklake_name Name for the ducklake alias
#' @param lake_path Path for data files
#' @param backend Catalog backend type
#' @param catalog_connection_string Backend-specific connection string
#' @param read_only Whether to attach in read-only mode
#'
#' @return A SQL ATTACH statement string
#' @keywords internal
build_attach_sql <- function(ducklake_name, lake_path, backend,
                              catalog_connection_string, read_only) {
  # Build the ducklake: connection string
  connection_string <- switch(backend,
    duckdb = {
      if (!is.null(lake_path)) {
        # Construct full path to ducklake file
        ducklake_path <- file.path(lake_path, paste0(ducklake_name, ".ducklake"))
        sprintf("ducklake:%s", ducklake_path)
      } else {
        sprintf("ducklake:%s.ducklake", ducklake_name)
      }
    },
    postgres = sprintf("ducklake:postgres:%s", catalog_connection_string),
    sqlite = sprintf("ducklake:sqlite:%s", catalog_connection_string),
    mysql = sprintf("ducklake:mysql:%s", catalog_connection_string)
  )
  
  # Build ATTACH options
  options <- character()
  
  if (!is.null(lake_path)) {
    options <- c(options, sprintf("DATA_PATH '%s'", lake_path))
  }
  
  if (read_only) {
    options <- c(options, "READ_ONLY")
  }
  
  # Assemble full ATTACH statement
  if (length(options) > 0) {
    options_str <- paste(options, collapse = ", ")
    sprintf("ATTACH '%s' AS %s (%s);", connection_string, ducklake_name, options_str)
  } else {
    sprintf("ATTACH '%s' AS %s;", connection_string, ducklake_name)
  }
}
