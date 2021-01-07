EXAMPLES

    To start a local disk server with encryption enabled:

      # Change this to the output of `openssl rand -hex 32`.
      export RUDOLFS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

      rudolfs --host=localhost:8080 local --path=path/to/lfs/data

    To start an S3 server with encryption enabled:

      # Your AWS credentials.
      export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
      export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
      export AWS_DEFAULT_REGION=us-west-1

      # Change this to the output of `openssl rand -hex 32`.
      export RUDOLFS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

      rudolfs \
          --host=localhost:8080 \
          --cache-dir=my_local_cache \
          s3 --bucket=my_bucket

    NOTE: Always use a different S3 bucket, cache directory, and encryption key
    than what you use in your production environment.
