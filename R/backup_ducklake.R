#' Create a complete DuckLake backup
#'
#' Creates a timestamped backup of the Parquet data files and, for file-based
#' catalog backends (DuckDB, SQLite), the catalog database file.
#'
#' @param ducklake_name Name of the attached DuckLake
#' @param lake_path Path to the DuckLake directory containing the data files
#'   (and catalog file for DuckDB/SQLite backends)
#' @param backup_path Directory where backups should be stored. A timestamped
#'   subdirectory will be created within this path.
#'
#' @return Invisibly returns the path to the created backup directory
#' @export
#'
#' @details
#' The function creates a backup by:
#' \enumerate{
#'   \item Creating a timestamped backup directory
#'   \item Copying the catalog database file (.ducklake or .sqlite) if the backend
#'     is file-based (DuckDB or SQLite)
#'   \item Copying all Parquet data files from the main/ directory
#' }
#'
#' For file-based backends (DuckDB, SQLite), the DuckLake is temporarily detached
#' during backup to ensure a consistent catalog copy. It is automatically
#' re-attached after the backup completes.
#'
#' For remote catalog backends (PostgreSQL, MySQL), only the Parquet data files
#' are backed up. The catalog database must be backed up separately using the
#' database's own backup tools (e.g., \code{pg_dump} for PostgreSQL,
#' \code{mysqldump} for MySQL).
#'
#' **Important notes:**
#' \itemize{
#'   \item Transactions committed after a backup won't be tracked when recovering.
#'     The data will exist in the Parquet files, but the backup will point to
#'     an earlier snapshot.
#'   \item Consider coordinating backups with maintenance operations (compaction
#'     and cleanup) for optimal storage efficiency.
#'   \item For production systems, schedule backups using \code{{cronR}} or
#'     \code{{taskscheduleR}}.
#' }
#'
#' @examples
#' \dontrun{
#' # Create a DuckLake
#' lake_dir <- tempfile("my_lake")
#' dir.create(lake_dir)
#' attach_ducklake("my_lake", lake_path = lake_dir)
#'
#' # Add some data
#' with_transaction(
#'   create_table(mtcars, "cars"),
#'   author = "User",
#'   commit_message = "Initial data"
#' )
#'
#' # Create a backup
#' backup_dir <- backup_ducklake(
#'   ducklake_name = "my_lake",
#'   lake_path = lake_dir,
#'   backup_path = file.path(lake_dir, "backups")
#' )
#'
#' # To restore from backup (override_data_path needed if location differs):
#' # detach_ducklake("my_lake")
#' # attach_ducklake("my_lake", lake_path = backup_dir, override_data_path = TRUE)
#' }
backup_ducklake <- function(ducklake_name, lake_path, backup_path) {
  # Validate inputs
  if (!is.character(ducklake_name) || length(ducklake_name) != 1) {
    cli::cli_abort("{.arg ducklake_name} must be a single character string.")
  }
  if (!dir.exists(lake_path)) {
    cli::cli_abort("{.arg lake_path} does not exist: {.path {lake_path}}")
  }

  backend <- get_ducklake_backend()

  # Create backup directory with timestamp
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_dir <- file.path(backup_path, paste0("backup_", timestamp))
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)

  # Backup catalog for file-based backends.
  # DuckDB's R bindings hold exclusive file locks on database files. To get a
  # consistent copy we must fully shut down the connection and run gc() so R
  # finalises the DuckDB objects and releases the OS file handles. The lake is
  # automatically re-attached after the copy.
  if (backend == "duckdb") {
    catalog_file <- file.path(lake_path, paste0(ducklake_name, ".ducklake"))
    if (file.exists(catalog_file)) {
      detach_ducklake(ducklake_name, shutdown = TRUE)
      gc()

      copy_ok <- file.copy(
        from = catalog_file,
        to = file.path(backup_dir, paste0(ducklake_name, ".ducklake"))
      )

      attach_ducklake(ducklake_name, lake_path = lake_path, backend = backend)

      if (copy_ok) {
        cli::cli_inform("Catalog backed up successfully.")
      } else {
        cli::cli_warn("Failed to copy catalog file: {.path {catalog_file}}")
      }
    } else {
      cli::cli_warn("Catalog file not found: {.path {catalog_file}}")
    }
  } else if (backend == "sqlite") {
    catalog_file <- .ducklake_env$catalog_connection_string
    if (!is.null(catalog_file) && file.exists(catalog_file)) {
      detach_ducklake(ducklake_name, shutdown = TRUE)
      gc()

      file.copy(
        from = catalog_file,
        to = file.path(backup_dir, basename(catalog_file))
      )

      attach_ducklake(ducklake_name, lake_path = lake_path, backend = backend,
                      catalog_connection_string = catalog_file)
      cli::cli_inform("SQLite catalog backed up successfully.")
    } else {
      cli::cli_warn("SQLite catalog file not found: {.path {catalog_file}}")
    }
  } else {
    # PostgreSQL or MySQL - catalog lives in a remote database
    tool <- if (backend == "postgres") "pg_dump" else "mysqldump"
    cli::cli_warn(c(
      "Catalog backup is not included for the {.val {backend}} backend.",
      "i" = "Use {.code {tool}} to backup the catalog database separately.",
      "i" = "Only Parquet data files will be backed up."
    ))
  }

  # Backup data directory
  main_dir <- file.path(lake_path, "main")
  if (dir.exists(main_dir)) {
    fs::dir_copy(
      path = main_dir,
      new_path = file.path(backup_dir, "main")
    )
    cli::cli_inform("Data files backed up successfully.")
  } else {
    cli::cli_warn("Data directory not found: {.path {main_dir}}")
  }

  cli::cli_inform("Backup completed: {.path {backup_dir}}")
  invisible(backup_dir)
}
