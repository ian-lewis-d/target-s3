## TESTS Readme

## Testing with localstack

Create the following `docker-compose.yml` file:

```yaml
version: '3.7'

services:
  localstack:
    image: localstack/localstack:latest
    container_name: localstack
    ports:
      - "4566-4599:4566-4599"
      - 12000:8080
    environment:
      - "PORT_WEB_UI=8080"
      - "SERVICES=s3"
      - "DEBUG=true"
      - "DOCKER_HOST=unix:///var/run/docker.sock"
      - "DEFAULT_REGION=eu-west-1"
      - "HOSTNAME_EXTERNAL=localstack"
      - "DATA_DIR=/tmp/localstack/data"
      # for aws cli
      - "AWS_ACCESS_KEY_ID=dummy"
      - "AWS_SECRET_ACCESS_KEY=dummy"
      - "AWS_REGION=eu-west-1"
    volumes:
      - ./tmp:/tmp/localstack/data
      - /var/run/docker.sock:/var/run/docker.sock
      - ./localstack/aws-setup/:/docker-entrypoint-initaws.d/

```

Create a `test.config.json` file in the `tests/` folder as follows:
```json
{
    "aws_access_key": "test",
    "aws_secret_access_key": "test",
    "aws_region": "eu-west-1",
    "format_type": "parquet",
    "endpoint_url": "http://localhost:4566",
    "bucket": "test-bucket",
    "prefix": "test-data-prefix"
}
```

Add a bucket for testing:
```shell
$ docker exec -it localstack /bin/sh "awslocal s3 mb s3://test-bucket"
```

Run target-s3 with the sample data.
```shell
$ poetry run target-s3 --config=target_s3/tests/test.config.json < target_s3/tests/sample_input.txt
```

Inspect the `localstack` folder for the files

`http://localhost:4566/test-bucket`