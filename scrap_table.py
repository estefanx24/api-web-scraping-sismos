import requests
import uuid
import boto3

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2024"
    response = requests.get(url)
    
    # Verificar si la solicitud fue exitosa
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }
    
    # Parsear y ordenar los datos
    sismos = response.json()
    sismos_ordenados = sorted(sismos, key=lambda x: x["createdAt"], reverse=True)[:10]  # Obtener los 10 últimos
    
    # Conexión a DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingPropuesto')
    
    # Limpiar la tabla de registros antiguos
    with table.batch_writer() as batch:
        for item in table.scan()['Items']:
            batch.delete_item(Key={'id': item['id']})

    # Insertar nuevos registros y construir la respuesta
    arrreturn = []
    for i, sismo in enumerate(sismos_ordenados, start=1):
        sismo['id'] = str(uuid.uuid4())
        sismo['#'] = i
        arrreturn.append(sismo)
        table.put_item(Item=sismo)

    # Devolver los datos de los últimos 10 sismos
    return {
        'statusCode': 200,
        'body': arrreturn
    }
