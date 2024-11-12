import requests
import uuid
import boto3
from bs4 import BeautifulSoup

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimosismo/ajxob/2024"
    response = requests.get(url)

    # Verificar el estado de la respuesta
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }
    
    # Parsear la respuesta JSON
    data = response.json()
    
    # Obtener los 10 últimos sismos y ordenarlos por fecha
    recent_earthquakes = sorted(data, key=lambda x: x["createdAt"], reverse=True)[:10]

    # Configuración de DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingPropuesto')
    
    # Limpiar la tabla de registros antiguos
    with table.batch_writer() as batch:
        for item in table.scan()['Items']:
            batch.delete_item(Key={'id': item['id']})

    # Insertar los datos en DynamoDB
    for index, earthquake in enumerate(recent_earthquakes, start=1):
        item = {
            'id': str(uuid.uuid4()),
            'nro': index,
            'fecha': earthquake['createdAt'],
            'magnitud': earthquake.get('magnitude', 'N/A'),  # Ajusta según el formato de la API
            'profundidad': earthquake.get('depth', 'N/A'),  # Ajusta según el formato de la API
            'ubicación': earthquake.get('location', 'N/A')  # Ajusta según el formato de la API
        }
        table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': 'Datos de los últimos 10 sismos almacenados exitosamente'
    }
