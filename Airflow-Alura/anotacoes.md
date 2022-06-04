# Airflow Alura

## Inicialização do Ambiente

Variáveis de ambiente
```sh
export AIRFLOW_HOME=${PWD}/airflow
# ou 
export AIRFLOW_HOME=~/airflow

# Install Airflow using the constraints file
AIRFLOW_VERSION=2.3.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.7
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.3.1/constraints-3.7.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Inicialização do airflow, atenção na inicialização, é comum apresentar erro.
```sh
airflow db init

# em ambiente local, requer cadastro do usuario
FLASK_APP=airflow.www.app flask fab create-admin

# airflow standalone
airflow webserver
airflow scheduler

# abrir localhost:8080, login e senha foram cadastrado na etapa anterior do FLASK_APP
```

## Connections

Adicionando Conexões pra API do Twitter
```md
admin > connections > create

Preencher campos:
- connection id: twitter_default
- connection type: HTTP
- host: https://api.twitter.com/
- extra: {"Authorization":"Bearer bearer_token"}
```


## Hooks

Criando Ganchos/Hooks no airflow
```
airflow > create pasta plugin > create pasta hooks > criar arquivo .py hooks
```


Definição alura 
```
Hooks são interfaces para comunicar o DAG com recursos externos compartilhados, por exemplo, várias tarefas podem acessar um mesmo banco de dados MySQL. Assim, em vez de implementar uma conexão para cada tarefa, é possível receber a conexão pronta de um gancho.

Hooks usam conexões para ter acesso a endereço de serviços e formas de autenticação, mantendo assim o código usado para autenticação e informações relacionadas fora das regras de negócio do data pipeline.
```