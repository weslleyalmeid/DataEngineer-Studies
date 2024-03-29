Install Minio
```sh
# install minio in docker container
docker compose up -d
```

Install minio cli
```sh
curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/

mc --help
```

Install aws cli
```sh
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

sudo ./aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
```

Create bucket in the Minio
```sh
mc mb minio/mlflow-models
```

Create bucket in the aws cli
```sg
aws s3api create-bucket --bucket mlflow-models --endpoint-url http://localhost:9000 --profile minio
aws s3api create-bucket --bucket test-minio --endpoint-url http://localhost:9000 --profile minio_user
```


Send file
```sh
# aws s3 cp ./predict.py s3://mlflow-models/<optional-name> --endpoint-url http://localhost:9000
aws s3 cp ./predict.py s3://mlflow-models/ --endpoint-url http://localhost:9000 --profile minio_user
```


Vincule MLFlow
```py

import os
from dotenv import load_dotenv

# Carregar as variáveis de ambiente do arquivo .secrets
load_dotenv('.secrets')

# Definir a configuração do backend store do MLflow
os.environ['MLFLOW_S3_ENDPOINT_URL'] = os.getenv('minio_endpoint_url')
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('minio_access_key')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('minio_secret_key')
os.environ['MLFLOW_S3_IGNORE_TLS'] = 'true'
os.environ['MLFLOW_TRACKING_URI'] = os.getenv('minio_bucket_name')

import boto3

endpoint_url = 'http://localhost:9000'
s3_client = boto3.client('s3', endpoint_url=endpoint_url)
```

arquivo .secrets
```s
minio_endpoint_url = 'http://localhost:9000'
minio_access_key = 'minio_user'
minio_secret_key = 'minio_password'
minio_bucket_name = 's3://mlflow-models'
```

