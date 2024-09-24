# Parte destinada aos imports.
import os
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import date
from datetime import timedelta, datetime

## PROJETO DE ETL COM ARQUITETURA MEDALHAO
## TODAS DECISOES DE CONCEPCAO DE SOLUCAO FORAM PENSADAS EXCLUSIVAMENTE EM CIMA DO DATASET DE 50 REGISTROS DE CERVEJARIAS.


# - Data de ingestao dos arquivos e caminhos dos arquivos
data_ingestao = date.today()
caminho_bronze = f'/opt/airflow/raw_data/bronze_breweries_{data_ingestao}.json'
caminho_silver = f'/opt/airflow/silver_layer/silver_particionado_{data_ingestao}.parquet'





# - CAMADA BRONZE (RAW)
def obter_dados_raw():
# - Obtem dados da API e os salva como JSON em seu estado puro, raw. Sem alteracoes e modificacoes, fiel a origem.
# - Iremos armazenar essa copia no banco de dados respeitando a camada Bronze(raw)
    try:
        url = "https://api.openbrewerydb.org/breweries"
        resposta = requests.get(url)
        dados = resposta.json()
# - Verifica se ja existe o arquivo na pasta, caso exista ele continua o codigo, caso nao os salva no destino em json
        if not os.path.exists(caminho_bronze):
            with open(caminho_bronze, 'w') as arquivo_json: #Funcao with open abre o arquivo para escrita e o fecha.
                json.dump(dados, arquivo_json, indent=4) # Escrever os dados no arquivo JSON com identacao de 4 espacos
            print(f"Arquivo JSON raw gravado em: {caminho_bronze}")
        else:
            print(f"O arquivo {caminho_bronze} já existe. Ignora a gravacao.")

    except Exception as e:
        print("Erro de ingestao na camada raw:", e)
        raise








def processar_camadas_silver():
# - Converte dados brutos para a camada silver.
# - Fazendo as mudancas necessarias para levar os dados de bronze para silver, para esse data set de 50 registros, como:
# - Remover colunas que nao sao relevantes para o escopo do projeto, melhorando a performance e eficiencia
# - Remover colunas que não tem dados ou são inconsistentes influencia na qualidade de dados(Coluna 'address_3')
# - Remocao da coluna 'state_province', pois contem os mesmos dados da coluna 'state', evitando redundancia.
# - Adição da coluna de data de ingestão dos dados (dt_ingestao - AAAA-MM-DD)

# - Leitura do arquivo previamente armezenado na camada bronze (raw), vamos comecar a trabalhar e impor o schema no dataframe.
# - Foi necessario utilizar o dtype no campo 'phone', pois vinha como numero, o que o desconfigurava e perdia caracteres
# - Para a camada silver os nomes das colunas originais foram mantidos, para facilitar o entendimento perante o arquivo de origem.
# - Nao retirei alguns campos que nao faziam parte do escopo, como por exemplo telefone e website, pois nunca sabemos
# - Que tipo de demanda pode surgir no nivel de negocios, caso fosse pedido uma agregacao de nome das lojas e seus enderecos
# - Poderiamos recuperar e entregar os dados. Caso essas colunas tivessem sido excluidas, precisaria reprocessar a camada prata
# - Com esses campos e com um novo schema, na duvida da utilizacao ou nao de uma coluna, na camadas bronze e silver melhor os manter.

    df_bronze = pd.read_json(caminho_bronze, dtype={'phone': str})

    # Define o schema das colunas
    schema = {
        "id": str,
        "name": str,
        "brewery_type": str,
        "address_1": str,
        "address_2": str,
        "city": str,
        "postal_code": str,
        "country": str,
        "longitude": str,
        "latitude": str,
        "phone": str,
        "website_url": str,
        "state": str,
        "street": str
    }

# - Cria o DataFrame com o schema definido com a coluna de data de ingestao 'dt_ingestao'
    df_silver = pd.DataFrame(df_bronze).astype(schema)
    df_silver['dt_ingestao'] = data_ingestao
    df_silver = df_silver.drop(columns=['address_3', 'state_province'])

# - Mesma logica de salvar utilizada para camada raw bronze.
# - Arquivo salvo no formato PARQUET como requisitado e particionado por COUNTRY STATE E CITY.

    if os.path.exists(caminho_silver):
        print(f"O arquivo '{caminho_silver}' ja existe. Ignora a gravacao.")
    else:
        df_silver.to_parquet(caminho_silver, partition_cols=['country', 'state', 'city'], index=False)
        print(f"Arquivo Parquet particionado por localizacao salvo em: {caminho_silver}")








# - Abaixo comeca os agrupamentos e enriquecimento da informacao para a camada gold, para preenchimento de relatorios, construcao de dashboards etc.
def agregar_por_tipo():
# - Agrega dados por tipo de cervejaria e salva na camada gold.
    df_silver = pd.read_parquet(caminho_silver)
    
    # Agregar quantidade de cervejarias por tipo
    df_agregado = (
        df_silver.groupby('brewery_type')
        .agg(qtd_cervejarias=('name', 'count'))
        .reset_index()
        .sort_values(by='qtd_cervejarias', ascending=False)
    )

    # Calcular porcentagem
    total_cervejarias = df_agregado['qtd_cervejarias'].sum()

    df_agregado['tipo_por_regiao_%'] = (df_agregado['qtd_cervejarias'] / total_cervejarias) * 100
    df_agregado.rename(columns={'brewery_type': 'tipo_cervejarias'}, inplace=True)
# - Salvando o arquivo em formato PARQUET como pedido no enunciado.
    caminho_gold = f'/opt/airflow/gold_layer/gold_agg_tipo_cervejarias_{data_ingestao}.parquet'
    df_agregado.to_parquet(caminho_gold, index=False)








def agregar_por_cidade():
# - Agrega dados por cidade e salva na camada gold.
    df_silver = pd.read_parquet(caminho_silver)
    
    df_agregado = (
        df_silver.groupby(['country','state','city'])
        .agg(qtd_cervejarias=('name', 'count'))
        .reset_index()
        .sort_values(by='qtd_cervejarias', ascending=False)
    )

    total_cervejarias = df_agregado['qtd_cervejarias'].sum()

    df_agregado['tipo_por_cidade_%'] = (df_agregado['qtd_cervejarias'] / total_cervejarias) * 100
    df_agregado.rename(columns={'country':'pais','state':'estado','city': 'cidades'}, inplace=True)

# - Retirando localizacoes que nao tem cervejarias
    df_agregado = df_agregado[df_agregado['qtd_cervejarias'] > 0]
# - Salvando o arquivo em formato PARQUET como pedido no enunciado.
    caminho_gold_2 = f'/opt/airflow/gold_layer/gold_agg_cidades_cervejarias_{data_ingestao}.parquet'
    df_agregado.to_parquet(caminho_gold_2, index=False)











def agregar_por_estado():
# - Agrega dados por cidade e salva na camada gold.
    df_silver = pd.read_parquet(caminho_silver)
    
    df_agregado = (
        df_silver.groupby(['country','state'])
        .agg(qtd_cervejarias=('name', 'count'))
        .reset_index()
        .sort_values(by='qtd_cervejarias', ascending=False)
    )

    total_cervejarias = df_agregado['qtd_cervejarias'].sum()

    df_agregado['tipo_por_estado_%'] = (df_agregado['qtd_cervejarias'] / total_cervejarias) * 100
    df_agregado.rename(columns={'state': 'estado', 'country':'pais'}, inplace=True)
    
# - Retirando localizacoes que nao tem cervejarias
    df_agregado = df_agregado[df_agregado['qtd_cervejarias'] > 0]

# - Salvando o arquivo em formato PARQUET como pedido no enunciado.
    caminho_gold_3 = f'/opt/airflow/gold_layer/gold_agg_estado_cervejarias_{data_ingestao}.parquet'
    df_agregado.to_parquet(caminho_gold_3, index=False)










def agregar_por_tipo_e_estado():
# - Agrega dados por tipo e estado e salva na camada gold.
    df_silver = pd.read_parquet(caminho_silver)
    
    df_agregado = (
        df_silver.groupby(['country','state','brewery_type'])
        .agg(qtd_cervejarias=('name', 'count'))
        .reset_index()
        .sort_values(by='state', ascending=True)
    )

    df_agregado.rename(columns={'state': 'estado', 'brewery_type': 'tipo_cervejarias', 'country':'pais' }, inplace=True)
# - Retirando localizacoes que nao tem cervejarias
    df_agregado = df_agregado[df_agregado['qtd_cervejarias'] > 0]

# - Salvando o arquivo em formato PARQUET como pedido no enunciado.
    caminho_gold_4 = f'/opt/airflow/gold_layer/gold_agg_tipos_por_estado_{data_ingestao}.parquet'
    df_agregado.to_parquet(caminho_gold_4, index=False)





# - Setando variaveis do Airflow para a orquestracao.
# - Inicializacao dos argumentos do DAG
default_args = {
    'owner': 'Wilson Roberto',
    'start_date': days_ago(5), # Caso Catchup = True vai rodar execucoes baseadas em 5 dias atras ate o dia atual
    'retries': 3, # Número de tentativas em caso de falha
    'retry_delay': timedelta(minutes=1), # Espera 1 minutos entre tentativas
}



# Definição do DAG
dag_breweries = DAG(
    'Breweries_Case_ETL', # Define o nome da dag
    default_args=default_args,
    description='Pipeline de Dados para ETL de raw data(bronze layer), silver layer e gold layer',
    schedule_interval = None,  # Definido para None para acionar diretamente no Airflow.
    catchup=False # Evita rodar execucoes passadas
)



# Definição das tarefas
tarefa_raw_data = PythonOperator(
    task_id='obter_dados_raw',
    python_callable=obter_dados_raw,
    dag=dag_breweries,
)



tarefa_silver_layer = PythonOperator(
    task_id='processar_silver_layer',
    python_callable=processar_camadas_silver,
    dag=dag_breweries,
)


tarefa_gold_layer_1 = PythonOperator(
    task_id='agregar_por_tipo',
    python_callable=agregar_por_tipo,
    dag=dag_breweries,
)


tarefa_gold_layer_2 = PythonOperator(
    task_id='agregar_por_cidade',
    python_callable=agregar_por_cidade,
    dag=dag_breweries,
)


tarefa_gold_layer_3 = PythonOperator(
    task_id='agregar_por_estado',
    python_callable=agregar_por_estado,
    dag=dag_breweries,
)


tarefa_gold_layer_4 = PythonOperator(
    task_id='agregar_por_tipo_e_estado',
    python_callable=agregar_por_tipo_e_estado,
    dag=dag_breweries,
)



# - Definição da ordem das tasks:
# - O seguinte modelo seria o ideal, mas para processamento local colocar 4 tasks em paralelo consome muito e trava o PC.
# tarefa_raw_data >> tarefa_silver_layer >> [tarefa_gold_layer_1, tarefa_gold_layer_2]

# - Como roda localmente foi escolhido da seguinte maneira.
# - Aqui se define a ordem de execucao das tasks.
tarefa_raw_data >> tarefa_silver_layer >> tarefa_gold_layer_1 >> tarefa_gold_layer_2 >> tarefa_gold_layer_3 >> tarefa_gold_layer_4