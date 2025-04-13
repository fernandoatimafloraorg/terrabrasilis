# Desafio Técnico - Engenheiro de Dados
# terrabrasilis

# Objetivo:

 O desafio consiste em desenvolver um processo de extração, transformação e carregamento (ETL) dos dados de desmatamento do último ano PRODES para o bioma Cerrado, obtidos a partir do GeoServer do TerraBrasilis (https://terrabrasilis.dpi.inpe.br/download-dedados/). O objetivo final é armazenar os dados processados em um banco de dados PostgreSQL com suporte a dados espaciais, garantindo a integridade e a eficiência das consultas geoespaciais.

 ## Requisitos do Desafio:

### 1.  Implementar uma classe para download dos dados do GeoServer do TerraBrasilis

### 2.  Criar um script principal que utilize essa classe

### 3.  Processar os dados baixados

### 4.  Armazenar os dados no banco de dados PostgreSQL


### 5.  Gerar um Relatório de conclusão

## Pré-requisitos

- Python 3.10 ou superior
- Criação do arquivo `.env` com aa variáveis de ambiente configuradas com a conexão do banco de dados de acordo com o arquivo de exemplo `.env.sample`
- Certifique-se de ter as bibliotecas necessárias instaladas: pip install requests pandas sqlalchemy geoalchemy2 shapely psycopg2-binary
- Criar extensão postgis: CREATE EXTENSION IF NOT EXISTS postgis;

## Criar um ambiente virtual para gerenciar as dependencias

cd seu_projeto
python -m venv venv
venv\Scripts\activate

## Testar o projeto

Executar no prompt de comando:

- cd seu_projeto
- python principal.py