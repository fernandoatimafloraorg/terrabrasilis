import os
import pandas as pd
from sqlalchemy import create_engine, text, Index, MetaData, Table, Column, Integer, Date, Float, JSON, func
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TEXT, JSONB
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import shape
from shapely.validation import make_valid
from sqlalchemy.exc import ProgrammingError
from urllib.parse import urljoin
from sqlalchemy import insert
import requests  # Importe a biblioteca requests aqui

# Certifique-se de que você tenha as bibliotecas necessárias instaladas:
# pip install pandas sqlalchemy geoalchemy2 shapely psycopg2-binary requests

# Importe a classe TerraBrasilisWFS
from terrabrasilis import TerraBrasilisWFS

def validate_geometry(geom_wkt):
    """Valida e corrige geometrias WKT usando Shapely."""
    try:
        geom = WKTElement(geom_wkt, srid=4326)
        shapely_geom = shape(geom.data)
        if not shapely_geom.is_valid:
            corrected_geom = make_valid(shapely_geom)
            return WKTElement(corrected_geom.wkt, srid=4326)
        return geom
    except Exception as e:
        print(f"Erro ao validar geometria: {e}")
        return None

def main():
    # 1. Inicializar a classe WFS
    wfs_client = TerraBrasilisWFS()
    bioma_interesse = "prodes-cerrado-nb"
    tipo_dado = "yearly_deforestation"
    #periodo_desejado = "2023-01-01/2023-12-31" 
    
    # Criar parametrização para o ano desejado :: TO-DO
    ## Verificar a base de dados para definir o ano desejado
        
    desired_year = 2023 # Exemplo de ano desejado para o filtro
    cql_filter = f"year>{desired_year}" if desired_year else None

    # 2. Definir filtros de data e paginação
    wfs_params = {
        'service': 'WFS',
        'version': '2.0.0',
        'request': 'GetFeature',
        'typeName': f"{bioma_interesse}:{tipo_dado}",
        'outputFormat': 'application/json',
        'CQL_FILTER': cql_filter,  # Filtro CQL para o ano
        'maxFeatures': 1000
    }

    # 3. Obter os dados de desmatamento
    response = wfs_client.download_data(bioma_interesse, tipo_dado, params=wfs_params)

    if response:
        print(f"Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")
        try:
            data = response.json()
            # ... processar os dados ...
        
            data = response.json()
            features = data.get('features', [])

            if features:
                print(f"Número de feições baixadas: {len(features)}")

                # 4. Estrutura para armazenamento em PostgreSQL
                db_config = {
                    'host': os.getenv('host'),
                    'port': os.getenv('port'),
                    'database': os.getenv('database'),
                    'user': os.getenv('user'),
                    'password': os.getenv('password')
                }
                engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
                table_name = "deforestation"
                OUTPUT_SCHEMA = "raw_data"
                full_table_name = f"{OUTPUT_SCHEMA}.{table_name}"

                metadata = MetaData(schema=OUTPUT_SCHEMA)
                deforestation_table = Table(
                    table_name,
                    metadata,
                    Column('id', Integer, primary_key=True),
                    Column('uid', Integer),
                    Column('satellite', TEXT),
                    Column('sensor', TEXT),
                    Column('publish_year', Date),
                    Column('year', Integer),
                    Column('state', TEXT),
                    Column('area_km', DOUBLE_PRECISION),
                    Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=4326)),
                    Column('properties', JSONB)
                    # Adicionar outras colunas conforme os atributos relevantes
                    # ...
                )

                try:
                    with engine.begin() as connection: # Use engine.begin() para gerenciar a transação
                        # Criar o schema se não existir
                        if not connection.dialect.has_schema(connection, OUTPUT_SCHEMA):
                            connection.execute(CreateSchema(OUTPUT_SCHEMA, if_not_exists=True))
                            #connection.commit() -- Não é necessário commit aqui, o contexto gerencia isso
                            print(f"Schema '{OUTPUT_SCHEMA}' criado com sucesso!")
                        else:
                            print(f"Schema '{OUTPUT_SCHEMA}' já existe.")

                        # Criar a tabela se não existir usando SQLAlchemy metadata
                        metadata.create_all(engine)
                        #connection.commit() -- Não é necessário commit aqui, o contexto gerencia isso
                        print(f"Tabela '{full_table_name}' criada ou já existente.")

                except ProgrammingError as e:
                    print(f"Erro ao interagir com o banco de dados - criando objetos: {e}")
                
                try:
                    # Inserir dados na tabela

                    data_to_insert = []
                    existing_ids = set() # Para controle de redundância (adapte conforme a chave única)

                    # Consultar IDs existentes (se houver uma chave única)
                    with engine.connect() as connection:
                        result = connection.execute(text(f"SELECT id FROM {full_table_name}")) # Adaptar a coluna de ID :: TO DO
                        for row in result:
                            existing_ids.add(row[0])

                    for feature in features:
                        properties = feature.get('properties', {})
                        geometry = feature.get('geometry')

                        # Extrair atributos relevantes
                        data_emissao_str = properties.get('publish_year')
                        data_emissao = pd.to_datetime(data_emissao_str, errors='coerce').date() if data_emissao_str else None
                        area_ha = properties.get('area_km')
                        satellite = properties.get('satellite') 
                        sensor = properties.get('sensor') 
                        year = properties.get('year') 
                        state = properties.get('state')


                        # Verificar redundância ('uid' )
                        feature_id = properties.get('uid') 
                        if feature_id is not None and feature_id in existing_ids:
                            print(f"Registro com ID '{feature_id}' já existe. Ignorando.")
                            continue

                        # Converter geometria para WKT
                            # Interoperabilidade: WKT é um formato padrão, facilitando a troca de informações geométricas 
                            # entre diferentes sistemas e softwares SIG.
                        geom_wkt = None
                        if 'geometry' in feature and feature['geometry']:
                            try:
                                shapely_geom = shape(feature['geometry'])
                                if shapely_geom.geom_type == 'Polygon' or shapely_geom.geom_type == 'MultiPolygon':
                                    geom_wkt = WKTElement(shapely_geom.wkt, srid=4326)
                                else:
                                    print(f"Tipo de geometria '{shapely_geom.geom_type}' não suportado, pulando feição.")
                            except Exception as e:
                                print(f"Erro ao processar geometria com Shapely: {e}")

                        #validated_geom = validate_geometry(geom_wkt)
                        #print(f"Tipo de 'properties': {type(properties)}")

                        if geom_wkt:
                            data_to_insert.append({
                                'uid': feature_id,
                                'satellite': satellite,
                                'sensor': sensor,
                                'year': year,
                                'publish_year': data_emissao,
                                'state': state,
                                'area_km': area_ha,
                                'geom': geom_wkt,
                                'properties': properties 
                            })
                            #print(data_to_insert[:5]) # Exibir os primeiros 5 registros para depuração
                        else:
                            print(f"Geometria inválida ou com erro, pulando feição.")

                    if data_to_insert:
                        with engine.begin() as connection:
                            stmt = insert(deforestation_table).values(data_to_insert)
                            connection.execute(stmt)
                            #connection.commit() -- Não é necessário commit aqui, o contexto gerencia isso
                            print(f"{len(data_to_insert)} registros inseridos na tabela '{full_table_name}'.")
                    else:
                        print("Nenhuma feição válida para inserir.")

                    # Criar índices para otimização
                    with engine.begin() as connection:
                        # Criar índices para colunas frequentemente consultadas
                        connection.execute(text(f"CREATE INDEX IF NOT EXISTS idx_publish_year ON {full_table_name} (publish_year);"))
                        connection.execute(text(f"CREATE INDEX IF NOT EXISTS idx_geom ON {full_table_name} USING GIST (geom);"))
                        connection.execute(text(f"CREATE INDEX IF NOT EXISTS idx_year ON {full_table_name} (year);")) 
                        # Adicionar outros índices conforme necessário para suas consultas mais utilizadas
                        #connection.commit() -- Não é necessário commit aqui, o contexto gerencia isso
                        print(f"Índices criados na tabela '{full_table_name}'.")                    

                except ProgrammingError as e:
                    print(f"Erro ao interagir com o banco de dados: {e}")
                finally:
                    engine.dispose()
            else:
                print("Nenhuma feição de desmatamento encontrada para o período.")
        except requests.exceptions.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
    else:
        print("Falha ao obter os dados do GeoServer.")

if __name__ == "__main__":
    main()