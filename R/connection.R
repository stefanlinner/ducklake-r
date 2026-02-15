# Package environment to store the ducklake connection and backend metadata
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
#' @param backend The catalog backend type (e.g., "duckdb", "postgres", "sqlite", "mysql")
#' @param catalog_connection_string The backend-specific connection string used for the catalog
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
#' Returns which catalog database backend is in use for the current DuckLake
#' connection. Defaults to \code{"duckdb"} if no backend has been explicitly set.
#'
#' @return A character string: one of \code{"duckdb"}, \code{"postgres"},
#'   \code{"sqlite"}, or \code{"mysql"}.
#' @export
get_ducklake_backend <- function() {
  backend <- .ducklake_env$backend
  if (is.null(backend)) "duckdb" else backend
}

#' Execute a SQL statement on the DuckLake connection
#'
#' A thin wrapper around \code{DBI::dbExecute()} that always uses the
#' connection stored in the ducklake package environment. This ensures all
#' SQL statements (ATTACH, USE, CREATE TABLE, BEGIN/COMMIT, etc.) are routed
#' to the same DuckDB connection.
#'
#' @param sql A single SQL statement to execute
#'
#' @return The return value of \code{DBI::dbExecute()}, invisibly.
#' @keywords internal
ducklake_db_exec <- function(sql) {
  conn <- get_ducklake_connection()
  invisible(DBI::dbExecute(conn, sql))
}

#' Detach from a ducklake
#'
#' Detaches from the current DuckLake. By default, this performs a "soft detach":
#' the DuckLake databases are removed from the DuckDB connection, but the
#' underlying DuckDB process stays alive. This allows attaching a different
#' DuckLake (potentially with a different backend) in the same session without
#' losing the connection.
#'
#' Set \code{shutdown = TRUE} to fully shut down the DuckDB connection, which
#' frees all memory and releases any file locks. This is equivalent to the
#' previous default behavior.
#'
#' @note Both soft and hard detach clear the stored backend metadata
#'   (\code{backend}, \code{catalog_connection_string}). After a soft detach,
#'   \code{get_ducklake_backend()} will return \code{"duckdb"} (the default)
#'   until a new lake is attached.
#'
#' @param ducklake_name Optional name of the ducklake to detach. If not provided,
#'   only cleans up the package metadata.
#' @param shutdown Logical. If \code{TRUE}, fully shuts down the DuckDB connection
#'   after detaching. Defaults to \code{FALSE}.
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
#' # Attach a different ducklake in the same session
#' attach_ducklake("other_lake", backend = "sqlite", ...)
#'
#' # Full shutdown when completely done
#' detach_ducklake("other_lake", shutdown = TRUE)
#' }
detach_ducklake <- function(ducklake_name = NULL, shutdown = FALSE) {
  conn <- get_ducklake_connection()
  
  is_valid <- tryCatch(DBI::dbIsValid(conn), error = function(e) FALSE)
  
  if (is_valid) {
    # If a ducklake name is provided, detach it from DuckDB
    if (!is.null(ducklake_name)) {
      tryCatch({
        # First detach the user-facing database
        DBI::dbExecute(conn, sprintf("DETACH %s;", ducklake_name))
        
        # Also detach the metadata catalog that DuckLake creates
        metadata_name <- sprintf("__ducklake_metadata_%s", ducklake_name)
        DBI::dbExecute(conn, sprintf("DETACH %s;", metadata_name))
      }, error = function(e) {
        # Ignore errors if database is not attached
      })
      
      # Switch back to the default in-memory database so subsequent
      # operations don't target the detached lake
      tryCatch(DBI::dbExecute(conn, "USE memory;"), error = function(e) NULL)
    }
    
    # Optionally shut down the DuckDB connection entirely
    if (shutdown) {
      tryCatch({
        DBI::dbDisconnect(conn, shutdown = TRUE)
      }, error = function(e) {
        warning("Could not disconnect: ", e$message)
      })
      
      # Force garbage collection to finalize DuckDB objects and release file locks.
      # This is necessary on Windows where DuckDB holds exclusive file locks that
      # aren't released until the R connection object's finalizer runs.
      gc()
    }
  }
  
  # Clean up package metadata
  .ducklake_env$connection <- NULL
  .ducklake_env$backend <- NULL
  .ducklake_env$catalog_connection_string <- NULL
  
  invisible(NULL)
}
