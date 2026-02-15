#' Install the ducklake extension to duckdb
#'
#' Installs the ducklake DuckDB extension and optionally the extensions required
#' for alternative catalog backends (postgres, sqlite, mysql). This only needs to
#' be run once on your system per DuckDB version.
#'
#' @param backend Character vector of backends to install extensions for. The
#'   \code{"ducklake"} extension is always installed. Additional values install
#'   the corresponding DuckDB extension needed for that catalog backend:
#'   \code{"postgres"}, \code{"sqlite"}, and/or \code{"mysql"}. Default installs
#'   only the ducklake extension.
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' # Install ducklake only (default)
#' install_ducklake()
#'
#' # Install ducklake + postgres extension
#' install_ducklake(backend = "postgres")
#'
#' # Install all supported backend extensions at once
#' install_ducklake(backend = c("postgres", "sqlite", "mysql"))
#' }
install_ducklake <- function(backend = NULL) {
  # check that duckdb version is at least 1.3.0
  drv <- duckdb::duckdb()
  con <- DBI::dbConnect(drv)
  duckdb_version <- DBI::dbGetQuery(con, "SELECT version()")[1,1]
  DBI::dbDisconnect(con)
  duckdb_version_numeric <- as.integer(gsub("\\.", "", sub("^v", "", duckdb_version)))
  if (duckdb_version_numeric < 130) {
    cli::cli_abort("duckdb must be version 1.3.0 or higher")
  }

  # the long messages thrown on load for duckplyr are suppressed here
  # TODO: find a better/more global place to do this, since duckplyr used elsewhere
  suppressMessages(ducklake_db_exec("INSTALL ducklake;"))
  cli::cli_inform("Installed {.pkg ducklake} extension.")

  valid_backends <- c("postgres", "sqlite", "mysql")
  if (!is.null(backend)) {
    backend <- match.arg(backend, valid_backends, several.ok = TRUE)
    for (ext in backend) {
      suppressMessages(ducklake_db_exec(sprintf("INSTALL %s;", ext)))
      cli::cli_inform("Installed {.pkg {ext}} extension.")
    }
  }

  invisible(NULL)
}
