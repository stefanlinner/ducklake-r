# Package environment to store ducklake metadata (backend type, connection string)
.ducklake_env <- new.env(parent = emptyenv())

#' Get the current DuckLake connection
#'
#' Returns the DuckDB connection used by DuckLake. This is always duckplyr's
#' default singleton connection, which provides critical setup (temp directory
#' configuration, R function loading, and macro registration).
#'
#' @return A DuckDB connection object
#' @export
#'
#' @note This function uses \code{duckplyr:::get_default_duckdb_connection()}.
#' While this accesses an unexported function, it is necessary for proper
#' duckplyr integration. See the duckplyr source for details:
#' \url{https://github.com/tidyverse/duckplyr/blob/main/R/relational-duckdb.R}
get_ducklake_connection <- function() {
  duckplyr:::get_default_duckdb_connection()
}

#' Get the current catalog backend type
#'
#' @returns One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`.
#'   Defaults to `"duckdb"` when no backend has been set.
#' @export
get_ducklake_backend <- function() {
  backend <- .ducklake_env$backend
  if (is.null(backend)) "duckdb" else backend
}

#' Execute SQL on the DuckLake connection
#'
#' @param sql SQL statement to execute
#' @returns The number of rows affected, invisibly.
#' @keywords internal
ducklake_db_exec <- function(sql) {
  conn <- get_ducklake_connection()
  invisible(DBI::dbExecute(conn, sql))
}

#' Detach from a ducklake
#'
#' Detaches the DuckLake database but keeps the DuckDB connection alive by
#' default. Use `shutdown = TRUE` to also close the connection and release
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
#' attach_ducklake("my_ducklake", lake_path = "path/to/lake")
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
    
    if (shutdown) {
      # Shut down duckplyr's singleton to release file locks and
      # immediately recreate it so the session keeps working
      shutdown_and_reset_singleton()
    }
  }
  
  .ducklake_env$backend <- NULL
  .ducklake_env$catalog_connection_string <- NULL
  
  invisible(NULL)
}

#' Shut down duckplyr's singleton connection and recreate it
#'
#' Shuts down the singleton DuckDB connection to release file locks, then
#' recreates it so the session keeps working. The new connection gets the
#' same macro/R-function setup that duckplyr normally does on first access.
#'
#' We replace `$con` directly instead of setting it to NULL because duckplyr
#' stacks `reg.finalizer(onexit = TRUE)` calls that accumulate across resets.
#' If `$con` is NULL when those finalizers fire at session exit, each one
#' calls `dbDisconnect(NULL)` and errors. Keeping a valid connection avoids
#' that.
#'
#' @note This accesses duckplyr internals (`default_duckdb_connection` and
#'   `create_default_duckdb_connection`). Validated against duckplyr 0.4.1.
#'   If duckplyr changes these internals the function returns `FALSE` and the
#'   caller falls back to the warning path.
#'
#' @returns `TRUE` on success, `FALSE` on failure.
#' @keywords internal
shutdown_and_reset_singleton <- function() {
  tryCatch({
    ddc  <- duckplyr:::default_duckdb_connection
    conn <- ddc$con

    if (!is.null(conn)) {
      DBI::dbDisconnect(conn, shutdown = TRUE)
      gc()

      # Immediately recreate so ddc$con stays valid (see note above)
      ddc$con <- duckplyr:::create_default_duckdb_connection()
    }
    TRUE
  }, error = function(e) {
    warning("Could not reset duckplyr singleton: ", e$message)
    FALSE
  })
}
