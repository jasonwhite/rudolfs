# Changelog

# v0.4.0

- Rely on docker / docker-compose to package `tini` (or compatible init system).
  All modern versions of the docker daemon support `--init`. Note that if you
  were using any previous version of rudolfs you now need to ensure
  that `--init` is added to your `docker run` arguments. If you are launching
  rudolfs via the `docker-compose` configs supplied with rudolfs then `--init`
  is enabled by default.

## v0.3.6

- Bump versions of various dependencies.

## v0.3.5

 - Support the `X-Forwarded-Host` header to work better with load balancers.
   (#31)
 - Support multipart S3 uploads for object sizes larger than 5GB. (#36)
   - Large uploads should also be more reliable for flaky S3 connections.

## v0.3.4

 - Fixed the S3 backend: 1c0d6c7e2638963e6755669c6013daae2fe47ee7
 - Added integration tests.
 - Add additional warnings when the `--cdn` flag is used with encryption or
   caching.

## v0.3.3 -- Yanked

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
