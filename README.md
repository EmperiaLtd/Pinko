# Pinko

Back End Management Repository for the pinko Project

# Requirements

- Docker
- SAM CLI https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html
- Redis CLI

# Getting started

```
pip install -r requirements.txt
```

# Configuration

The deployment information (stack name, s3 bucket, s3 prefix, region) can be configured in the samconfig.toml file.
The redis database connection can be configured in the db.py.

# Deploying the application

```
cd SAM-pinko
```

To build:

```
sam build
```

To deploy:

```
sam deploy
```

# Help

To get further information, please ask Owen Easter or Simonas Holcmann to understand how this is set up.
