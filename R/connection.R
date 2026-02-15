# Package environment to store the ducklake connection
.ducklake_env <- new.env(parent = emptyenv())

#' Get the current DuckLake connection
#'
#' This function retrieves the active DuckLake connection. If no connection
#' has been explicitly set via \code{set_ducklake_connection()}, it falls back
#' to duckplyr's default DuckDB connection for seamless integration.
#'
#' @return A DuckDB connection object
#' @export
#'
#' @note This function uses \code{duckplyr:::get_default_duckdb_connection()}
#' as a fallback when no connection has been explicitly set. While this accesses
#' an unexported function, it is necessary for proper duckplyr integration as
#' duckplyr's connection provides critical setup (singleton pattern, temp directory
#' configuration, R function loading, and macro registration) that cannot be
#' easily replicated. See the duckplyr source for details:
#' \url{https://github.com/tidyverse/duckplyr/blob/main/R/relational-duckdb.R}
get_ducklake_connection <- function() {
  conn <- .ducklake_env$connection
  
  if (is.null(conn)) {
    # Fall back to duckplyr's default connection for proper integration
    conn <- duckplyr:::get_default_duckdb_connection()
  }
  
  return(conn)
}

#' Set the DuckLake connection
#'
#' @param conn A DuckDB connection object
#' @param backend Catalog backend type (e.g. `"duckdb"`, `"postgres"`, `"sqlite"`, `"mysql"`)
#' @param catalog_connection_string Backend-specific connection string for the catalog
#' @keywords internal
set_ducklake_connection <- function(conn, backend = "duckdb",
                                     catalog_connection_string = NULL) {
  .ducklake_env$connection <- conn
  .ducklake_env$backend <- backend
  .ducklake_env$catalog_connection_string <- catalog_connection_string
  invisible(conn)
}

#' Get the current catalog backend type
#'
#' Returns which catalog backend is in use. Defaults to `"duckdb"` when no
#' backend has been set.
#'
#' @returns One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`.
#' @export
get_ducklake_backend <- function() {
  backend <- .ducklake_env$backend
  if (is.null(backend)) "duckdb" else backend
}

#' Execute SQL on the DuckLake connection
#'
#' Thin wrapper around [DBI::dbExecute()] that routes through
#' [get_ducklake_connection()].
#'
#' @param sql SQL statement to execute
#'
#' @returns The number of rows affected, invisibly.
#' @keywords internal
ducklake_db_exec <- function(sql) {
  conn <- get_ducklake_connection()
  invisible(DBI::dbExecute(conn, sql))
}

#' Detach from a ducklake
#'
#' By default performs a soft detach: the DuckLake database is removed but the
#' DuckDB process stays alive, so you can attach a different lake in the same
#' session. Use `shutdown = TRUE` to fully close the connection and release
#' file locks.
#'
#' @param ducklake_name Optional name of the ducklake to detach.
#' @param shutdown If `TRUE`, shut down the DuckDB connection after detaching.
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' attach_ducklake("my_ducklake")
#' # ... do work ...
#' detach_ducklake("my_ducklake")
#'
#' # Full shutdown when completely done
#' detach_ducklake("my_ducklake", shutdown = TRUE)
#' }
detach_ducklake <- function(ducklake_name = NULL, shutdown = FALSE) {
  conn <- get_ducklake_connection()
  
  is_valid <- tryCatch(DBI::dbIsValid(conn), error = function(e) FALSE)
  
  if (is_valid) {
    if (!is.null(ducklake_name)) {
      tryCatch({
        # Detach the user-facing database and its metadata catalog
        DBI::dbExecute(conn, sprintf("DETACH %s;", ducklake_name))
        metadata_name <- sprintf("__ducklake_metadata_%s", ducklake_name)
        DBI::dbExecute(conn, sprintf("DETACH %s;", metadata_name))
      }, error = function(e) {
        # Ignore errors if database is not attached
      })
      
      # Switch back to in-memory so subsequent queries don't target
      # the detached lake
      tryCatch(DBI::dbExecute(conn, "USE memory;"), error = function(e) NULL)
    }
    
    # Only shut down if we own the connection. duckplyr's singleton
    # (used when .ducklake_env$connection is NULL) can't recover from
    # dbDisconnect and must not be killed.
    if (shutdown && !is.null(.ducklake_env$connection)) {
      tryCatch({
        DBI::dbDisconnect(conn, shutdown = TRUE)
      }, error = function(e) {
        warning("Could not disconnect: ", e$message)
      })
      
      # gc() finalizes DuckDB objects so file locks are released immediately
      # (on Windows, DuckDB holds exclusive locks until the R finalizer runs)
      gc()
      .ducklake_env$connection <- NULL
    }
  }
  
  .ducklake_env$backend <- NULL
  .ducklake_env$catalog_connection_string <- NULL
  
  invisible(NULL)
}
