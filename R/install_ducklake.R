#' Install the ducklake extension to duckdb
#'
#' Installs the ducklake DuckDB extension and optionally the extensions for
#' alternative catalog backends (postgres, sqlite, mysql). Only needs to be
#' run once per DuckDB version.
#'
#' @param backend Optional character vector of backends to install. The ducklake
#'   extension is always installed. Pass `"postgres"`, `"sqlite"`, and/or
#'   `"mysql"` to install the corresponding backend extensions.
#'
#' @note On Windows the `postgres` and `mysql` extensions are not available
#'   (MinGW toolchain). See [attach_ducklake()] for details.
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' install_ducklake()
#' install_ducklake(backend = "postgres")
#' install_ducklake(backend = c("postgres", "sqlite", "mysql"))
#' }
install_ducklake <- function(backend = NULL) {
  # check that duckdb version is at least 1.3.0
  conn <- get_ducklake_connection()
  duckdb_version <- DBI::dbGetQuery(conn, "SELECT version()")[1, 1]
  duckdb_version_numeric <- as.integer(gsub("\\.", "", sub("^v", "", duckdb_version)))
  if (duckdb_version_numeric < 130) {
    cli::cli_abort("duckdb must be version 1.3.0 or higher")
  }

  # the long messages thrown on load for duckplyr are suppressed here
  # TODO: find a better/more global place to do this, since duckplyr used elsewhere
  suppressMessages(duckplyr::db_exec("INSTALL ducklake;"))
  cli::cli_inform("Installed {.pkg ducklake} extension.")

  valid_backends <- c("postgres", "sqlite", "mysql")
  if (!is.null(backend)) {
    backend <- match.arg(backend, valid_backends, several.ok = TRUE)
    for (ext in backend) {
      suppressMessages(duckplyr::db_exec(sprintf("INSTALL %s;", ext)))
      cli::cli_inform("Installed {.pkg {ext}} extension.")
    }
  }

  invisible(NULL)
}
