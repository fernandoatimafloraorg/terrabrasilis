import requests
import json
import time
from urllib.parse import urljoin

#1-Criar uma classe modular para interagir com o serviço WFS (Web Feature Service)
class TerraBrasilisWFS:
    def __init__(self, base_url="https://terrabrasilis.dpi.inpe.br/geoserver", retry_attempts=3, retry_delay=5):
        """
        Inicializa a classe para interagir com o serviço WFS do TerraBrasilis.
        Parameters:
            base_url (str): URL base do GeoServer.
            retry_attempts (int): Número máximo de tentativas em caso de falha na conexão.
            retry_delay (int): Tempo em segundos entre as tentativas.
        """
        self.base_url = base_url
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

    def _build_url(self, bioma, layer):
        """
        Constrói a URL completa para o serviço WFS.
        Parameters:
            bioma (str): Bioma desejado (ex: "prodes-cerrado-nb").
            layer (str): Camada de informações (ex: "yearly_deforestation").
        Returns:
            str: URL completa para o serviço WFS.
        """
        workspace = bioma
        full_url = urljoin(f"{self.base_url}/", f"{workspace}/{layer}/wfs")
        return full_url

    def download_data(self, bioma, layer, params=None, output_format="application/json"):
        """
        Realiza o download dos dados do GeoServer via WFS.
        Parameters:
            bioma (str): Bioma de interesse.
            layer (str): Camada de dados desejada.
            params (dict, optional): Dicionário de parâmetros para a requisição WFS.
                                     Exemplos: {'service': 'WFS', 'version': '1.0.0',
                                              'request': 'GetFeature', 'typeName': '...',
                                              'outputFormat': '...'}
                                     Se None, os parâmetros básicos serão definidos.
            output_format (str): Formato de saída desejado (ex: "application/json",
                                   "application/gml+xml", "csv").
        Returns:
            requests.Response or None: Objeto Response da requisição em caso de sucesso,
                                       None em caso de falha após várias tentativas.
        """
        url = self._build_url(bioma, layer)
        default_params = {
            'service': 'WFS',
            'version': '1.0.0',
            'request': 'GetFeature',
            'typeName': f"{bioma}:{layer}",  # Formato comum do typeName
            'outputFormat': output_format
        }

        if params:
            default_params.update(params)

        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(url, params=default_params)
                response.raise_for_status()  # Levanta uma exceção para códigos de erro HTTP
                return response
            except requests.exceptions.RequestException as e:
                print(f"Erro na conexão (tentativa {attempt + 1}/{self.retry_attempts}): {e}")
                if attempt < self.retry_attempts - 1:
                    print(f"Tentando novamente em {self.retry_delay} segundos...")
                    time.sleep(self.retry_delay)
                else:
                    print("Número máximo de tentativas excedido. Falha ao baixar os dados.")
                    return None

def main():
    wfs_client = TerraBrasilisWFS()
    bioma_exemplo = "prodes-cerrado-nb"
    layer_exemplo = "yearly_deforestation"
    periodo_exemplo = "2023-01-01/2023-12-31" # Exemplo de período (pode ser usado em filtros)

    # Download básico em formato JSON
    response_json = wfs_client.download_data(bioma_exemplo, layer_exemplo)
    if response_json:
        print("Download JSON bem-sucedido!")
        # print(response_json.json()) # Para visualizar o conteúdo JSON
        try:
            nome_arquivo = f"{bioma_exemplo}_{layer_exemplo}.json"
            # Salvar o conteúdo JSON em um arquivo
            data = response_json.json()
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print(f"Dados JSON salvos com sucesso em: {nome_arquivo}")
        except json.JSONDecodeError:
            print("Erro: A resposta não pôde ser decodificada como JSON.")
        except Exception as e:
            print(f"Erro ao salvar o arquivo JSON: {e}")

    # Download em formato GeoJSON para facilitar a integração geoespacial
    response_geojson = wfs_client.download_data(bioma_exemplo, layer_exemplo, output_format="application/json") # GeoJSON é um tipo de JSON
    if response_geojson:
        print("Download GeoJSON bem-sucedido!")
        # print(response_geojson.json())

if __name__ == "__main__":
    main()