# Changelog

## v0.3.3

 - Added S3 signed URL support (#27)

## v0.3.2

 - Updated `backoff` crate to fix #28
 - Updated `askama` and `rand` crates (#30)

## v0.3.1

 - Added support for Kubernetes credentials (#26)

## v0.3.0

 - Created this changelog.
 - Updated tokio to v1.0.

### Breaking changes

 - Changed the command line interface so that each storage backend is a separate
   subcommand.

### Features

 - Added a local-disk only storage backend.
 - Added support for more configuration via environment variables:
   - `RUDOLFS_HOST` (same as `--host`)
   - `RUDOLFS_KEY` (same as `--key`)
   - `RUDOLFS_CACHE_DIR` (same as `--cache-dir`)
   - `RUDOLFS_MAX_CACHE_SIZE` (same as `--max-cache-size`)
   - `RUDOLFS_LOG` (same as `--log-level`)
   - `RUDOLFS_S3_BUCKET` (same as `--bucket` when using S3 storage)
   - `RUDOLFS_S3_PREFIX` (same as `--prefix` when using S3 storage)
   - `RUDOLFS_LOCAL_PATH` (same as `--path` when using local disk storage)
