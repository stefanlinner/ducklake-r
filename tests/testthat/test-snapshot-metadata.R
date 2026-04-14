# Tests for set_snapshot_metadata() parameterized queries

test_that("snapshot metadata handles single quotes in author and commit message", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_test_1"),
    author = "Dr. O'Brien",
    commit_message = "It's a table with 'quoted' values"
  )

  snapshots <- list_table_snapshots("meta_test_1")
  expect_equal(nrow(snapshots), 1)
  expect_equal(snapshots$author, "Dr. O'Brien")
  expect_equal(snapshots$commit_message, "It's a table with 'quoted' values")

  cleanup_temp_ducklake(lake)
})

test_that("set_snapshot_metadata works with special characters", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_test_2"),
    author = "Test",
    commit_message = "init"
  )

  set_snapshot_metadata(
    ducklake_name = lake$ducklake_name,
    author = "François O'Malley",
    commit_message = "Added 'special' chars: è, ñ, ü"
  )

  snapshots <- list_table_snapshots("meta_test_2")
  expect_equal(snapshots$author, "François O'Malley")
  expect_equal(snapshots$commit_message, "Added 'special' chars: è, ñ, ü")

  cleanup_temp_ducklake(lake)
})

test_that("set_snapshot_metadata warns when no metadata provided", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_test_3"),
    author = "Test",
    commit_message = "init"
  )

  expect_warning(set_snapshot_metadata(lake$ducklake_name), "No metadata")

  cleanup_temp_ducklake(lake)
})
