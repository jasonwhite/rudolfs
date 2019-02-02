# Rudo-LFS (aka "rudolfs")

A high-performance, caching Git LFS server with an AWS S3 back-end.

## Features

 - AWS S3 permanent storage back-end.

 - A configurable local disk cache to speed up downloads (and reduce your
   S3 bill).

 - Corruption-resilient local disk cache. Even if the disk is getting
   blasted by cosmic rays, it'll find corrupted LFS objects and purge them from
   the cache transparently. The client should never notice this happening.

 - Encryption of LFS objects in both the cache and in permanent storage.

## Running It

### Generate an encryption key

All LFS objects are encrypted with the xchacha20 symmetric stream cipher. You
must generate a 32-byte encryption key before starting the server.

Generating a random key is easy:

    openssl rand -hex 32

Keep this secret and save it in a password manager so you don't lose it. We will
pass this to the server below.

**Note**:
 - If the key ever changes, all existing LFS objects will become garbage.
   When the Git LFS client attempts to download them, the SHA256 verification
   step will fail.
 - LFS objects in both the cache and in permanent storage are encrypted.
   However, objects are decrypted before being sent to the LFS client, so take
   any necessary precautions to keep your intellectual property safe.

### Development

For testing during development, it is easiest to run it with Cargo. Create
a file called `test.sh` (this path is already ignored by `.gitignore`):

```bash
# Your AWS credentials.
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
export AWS_DEFAULT_REGION=us-west-1

# Change this to the output of `openssl rand -hex 32`.
KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

cargo run -- \
  --cache-dir cache \
  --host localhost:8080 \
  --s3-bucket foobar \
  --max-cache-size 10GiB \
  --key $KEY
```

**Note**: Always use a different S3 bucket, cache directory, and encryption key
than what you use in your production environment.

### Production

To run in a production environment, it is easiest to use `docker-compose`:

 1. Create a `.env` file next to `docker-compose.yml` with the configuration
    variables:

    ```
    AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXQ
    AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    AWS_DEFAULT_REGION=us-west-1
    LFS_ENCRYPTION_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    LFS_S3_BUCKET=my-bucket
    LFS_MAX_CACHE_SIZE=10GB
    ```

 2. Use the provided `docker-compose.yml` file to run a production environment:

    ```bash
    docker-compose up -d
    ```

 3. **[Optional]**: It is best to use nginx as a reverse proxy for this server.
    Use it to enable TLS. How to configure this is better covered by other
    tutorials on the internet.

**Note**:
 - A bigger cache is (almost) always better. Try to use ~85% of the available
   disk space.
 - The cache data is stored in a Docker volume named `rudolfs_data`. If you
   want to delete it, run `docker volume rm rudolfs_data`.

## AWS Credentials

AWS credentials must be provided to the server so that it can make requests to
the S3 bucket specified on the command line (with `--s3-bucket`).

Your AWS credentials will be searched for in the following order:

 1. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
 2. AWS credentials file. Usually located at `~/.aws/credentials`.
 3. IAM instance profile. Will only work if running on an EC2 instance with an
    instance profile/role.

The AWS region is read from the `AWS_DEFAULT_REGION` or `AWS_REGION` environment
variable. If it is malformed, it will fall back to `us-east-1`. If it is not
present it will fall back on the value associated with the current profile in
`~/.aws/config` or the file specified by the `AWS_CONFIG_FILE` environment
variable. If that is malformed or absent it will fall back to `us-east-1`.

## Client Configuration

Add `.lfsconfig` to the root of your Git repository:

```
[lfs]
url = "http://gitlfs.example.com:8080/"
```

Optionally, I also recommend changing these global settings to speed things up:

``` bash
# Increase the number of worker threads
git config --global lfs.concurrenttransfers 64

# Use a global LFS cache to make re-cloning faster
git config --global lfs.storage ~/.cache/lfs
```

## License

[MIT License](/LICENSE)

## Thanks

This was developed at [Environmental Systems Research
Institute](http://www.esri.com/) (Esri) who have graciously allowed me to retain
the copyright and publish it as open source software.
