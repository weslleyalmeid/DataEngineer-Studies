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


**Ajustando o salvamento dos arquivos**
- Adicionar path_file no init
- Passar self_path para o execute


## Plugin
Classe utilizada para desenvolvimento de plugins personalizados, sendo possível desenvolver plugins nas seguintes categorias.
```md
    - name
    - source
    - hooks
    - executors
    - macros
    - admin_views
    - flask_blueprints
    - menu_links
    - appbuilder_views
    - appbuilder_menu_items
```


Para criar plugin personalizado, é necessário criar o arquivo airflow_plugin.py no diretório de plugins, o airflow 2.0 não aceita instaciar airflow.operators.nome_projeto diretamento, é necessário adicionar a pasta plugins no sys.path para localização do pacote ou criar um pacote dos operators.

**Adicionando pacote ao sys.path**
Arquivo airflow_plugin.py
```
from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator
import sys
import os

# inserir path do airflow no ambiente das libs
sys.path.insert(0,os.path.join(os.environ.get('AIRFLOW_HOME'), 'plugins'))


class AluraAirflowPlugin(AirflowPlugin):
    # normalmente e o nome da empresa/projeto
    name = "alura"
    operators = [TwitterOperator]
```


## Intalação do PySpark/Spark

```
pip install pyspark

# execucao
pyspark
```

Download do [Spark](https://spark.apache.org/downloads.html) e execução
```sh
tar -xf name_file.tgz

# abre o shell com scala
./name_file/bin/spark-shell

# executa um exemplo com 10 tasks para execução
./bin/spark-submit examples/src/main/python/pi.py 10
```

**Inicialização do Spark**
```py
#objeto mais importate do Spark
from pyspark.sql import SparkSession
# builder: vai construir a sessão
# appName: nome util para utilizacao do logs
# getOrCreate: verificar se existe sessao ativa ou criar uma nova

spark = SparkSession\
        .builder\
        .appName('twitter_transformation')\
        .getOrCreate()
```


**Resilient Distributed Dataset (RDD) no Spark**
```py
# lista exemplo
data = [1,2,3,4,5]
# criar rdd
rdd = sc.parallelize(data)

# numero de particoes, por padrao e numero de cores do processador
rdd.getNumPartitions()

# onde cada informacao esta alocado no rdd
rdd.glom().collect()

# operacao com todos os nodes e retornando resposta ao node main
rdd.reduce(lambda x, y: x + y)

# obter todos os dados em um node main
rdd.collect()

# criar dataframe em dados Row
from pyspark.sql import Row
df = rdd.map(lambda x: Row(n=x)).toDF()


# utilizando functions
from pyspark.sql import functions as f

# a functions desc e comandos sql também é permitido no spark
df.select('n').orderBy(f.desc('n')).show()

# criando tabela temporaria
df.createOrReplaceTempView('numeros')
# consultando tabelas
spark.sql('show tables').show()
# select na tabela com sql
spark.sql('select sum(n) from numeros').show()

# sum com functions spark
df.agg(f.sum('n')).show()
```