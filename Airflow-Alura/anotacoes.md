# Airflow Alura

Glossário
```
- DAG: Directed Acyclic Graph
- Hooks: Ganchos para conexões
```

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


**Definição - Alura**
Hooks são interfaces para comunicar o DAG com recursos externos compartilhados, por exemplo, várias tarefas podem acessar um mesmo banco de dados MySQL. Assim, em vez de implementar uma conexão para cada tarefa, é possível receber a conexão pronta de um gancho.

Hooks usam conexões para ter acesso a endereço de serviços e formas de autenticação, mantendo assim o código usado para autenticação e informações relacionadas fora das regras de negócio do data pipeline.


## Operators

Todo operator vai ter um método chamado **execute**, esse método é chamado na DAG para executar a tarefa.
```py
# todo operator vai ter um método chamado execute
def execute(self, context):
    pass
```

**Definição - Alura**
Um operador possuirá três características:
1) Idempotência: Independentemente de quantas vezes uma tarefa for executada com os mesmos parâmetros, o resultado final deve ser sempre o mesmo;

2) Isolamento: A tarefa não compartilha recursos com outras tarefas de qualquer outro operador;

3) Atomicidade: A tarefa é um processo indivisível e bem determinado.

Operadores geralmente executam de forma independente, e o DAG vai garantir que operadores sejam executados na ordem correta. Quando um operador é instanciado, ele se torna parte de um nodo no DAG.

Todos os operadores derivam do operador base chamado BaseOperator, e herdam vários atributos e métodos. Existem 3 tipos de operadores:

- Operadores que fazem uma ação ou chamam uma ação em outro sistema;
- Operadores usados para mover dados de um sistema para outro;
- Operadores usados como sensores, que ficam executando até que um certo critério é atingido. Sensores derivam da BaseSensorOperator e utilizam o método poke para testar o critério até que este se torne verdadeiro ou True, e usam o poke_interval para determinar a frequência de teste.
