import logging
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import TimeoutError, SQLAlchemyError
import pymongo 
from pymongo.errors import ConnectionFailure, PyMongoError
import certifi
import json
import time

# Función para obtener credenciales de base de datos
def get_credentials(db_name='feed_mysql'):
    """
    Retorna las credenciales necesarias para conectarse a una base de datos específica.

    :param db_name: Nombre de la base de datos para obtener credenciales.
    :return: Diccionario con las credenciales (host, puerto, usuario, contraseña, nombre de la base de datos).
    """
    conn_name_lower = db_name.lower()
    credentials = {}

    if conn_name_lower == 'feed_mysql':
        credentials = {
            'db_host': 'localhost',
            'db_port': '3306',
            'db_user': 'root',
            'db_password': 'password',
            'db_name':  'feed'
        }
    elif conn_name_lower == 'catalog_mysql':
        credentials = {
            'db_host': 'localhost',
            'db_port': '3306',
            'db_user': 'root',
            'db_password': 'pass',
            'db_name':  'catalog'
        }
    elif conn_name_lower == 'deliveries':
        credentials = {
            'db_host': 'localhost',
            'db_port': '27017',
            'db_user': 'user',
            'db_password': 'pass',
            'db_name':  'deliveries'
        }        

    return credentials


##############################################################################################################
# Conexiones y Acciones sobre Base de Datos MySQL
##############################################################################################################

def get_db_connection_pool(db_name='feed_mysql', pool_size=20, max_overflow=10):
    """
    Retorna una conexión pool configurada a la base de datos MySQL especificada.

    :param db_name: Nombre de la base de datos para conectarse.
    :param pool_size: Tamaño del pool de conexiones.
    :param max_overflow: Conexiones adicionales permitidas cuando el pool está lleno.
    :return: Engine de SQLAlchemy configurado o None en caso de error.
    """
    credenciales_bd = get_credentials(db_name)

    try:
        # Extraer credenciales
        host = credenciales_bd['db_host']
        port = credenciales_bd['db_port']
        database = credenciales_bd['db_name']
        user = credenciales_bd['db_user']
        password = credenciales_bd['db_password']
        
        # Formatear la URL de conexión
        db_url = f'mysql+pymysql://{user}:{password}@{host}/{database}'

        # Crear el engine con el pool de conexiones
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=30,  # Espera antes de lanzar un error si el pool está lleno
            pool_recycle=3600,  # Tiempo para reciclar conexiones y prevenir desconexiones
            pool_pre_ping=True  # Verificar si las conexiones están activas antes de usarlas
        )
        
        logging.info(f"Pool de conexiones creado exitosamente para la base de datos '{db_name}'.")
        return engine
    
    except SQLAlchemyError as e:
        logging.error(f"Error al conectar a la base de datos '{db_name}': {e}")
        return None


def execute_query(engine, query, query_values=None, retry_attempts=3, retry_delay=5):
    """
    Ejecuta una consulta SQL usando una conexión del pool proporcionado.

    :param engine: Objeto de conexión (engine) de SQLAlchemy.
    :param query: Consulta SQL a ejecutar.
    :param query_values: Valores para usar en la consulta SQL.
    :param retry_attempts: Número de intentos de reintento en caso de fallo.
    :param retry_delay: Tiempo en segundos entre intentos de reintento.
    :return: Resultado de la consulta o None en caso de error.
    """
    attempts = 0
    while attempts < retry_attempts:
        try:
            # Obtener una conexión desde el pool
            with engine.connect() as connection:
                # Ejecutar la consulta
                result = connection.execute(query, **(query_values or {}))

                # Si la consulta devuelve filas (como en un SELECT)
                if result.returns_rows:
                    rows = result.fetchall()
                    logging.info("Consulta SELECT ejecutada exitosamente.")
                    return rows
                else:
                    # Para consultas como INSERT, UPDATE o DELETE
                    logging.info("Consulta no-SELECT ejecutada exitosamente.")
                    return None

        except TimeoutError:
            logging.warning("Error: Tiempo de espera agotado al intentar obtener una conexión del pool. Reintentando...")
        except SQLAlchemyError as e:
            logging.error(f"Error al ejecutar la consulta: {e}")
            break

        attempts += 1
        time.sleep(retry_delay)

    logging.error("Error: No se pudo ejecutar la consulta después de varios intentos.")
    return None


def execute_query_batch(engine, query, query_values=None, retry_attempts=3, retry_delay=5):
    """
    Ejecuta una consulta SQL usando una conexión del pool proporcionado.

    :param engine: Objeto de conexión (engine) de SQLAlchemy.
    :param query: Consulta SQL a ejecutar.
    :param query_values: Valores para usar en la consulta SQL. Puede ser un diccionario (para un solo set de valores)
                         o una lista de diccionarios (para múltiples sets de valores, batch insert/update).
    :param retry_attempts: Número de intentos de reintento en caso de fallo.
    :param retry_delay: Tiempo en segundos entre intentos de reintento.
    :return: Resultado de la consulta o None en caso de error.
    """
    attempts = 0
    while attempts < retry_attempts:
        try:
            # Serializar JSON en los query_values si es necesario
            if isinstance(query_values, list):
                for entry in query_values:
                    if 'specific_data_track' in entry:
                        entry['specific_data_track'] = json.dumps(entry['specific_data_track'])
            elif isinstance(query_values, dict):
                if 'specific_data_track' in query_values:
                    query_values['specific_data_track'] = json.dumps(query_values['specific_data_track'])

            # Obtener una conexión desde el pool
            with engine.connect() as connection:
                # Ejecutar la consulta
                if isinstance(query_values, list):
                    connection.execute(query, query_values)
                else:
                    connection.execute(query, **(query_values or {}))

                logging.info("Consulta no-SELECT ejecutada exitosamente.")
                return None

        except TimeoutError:
            logging.warning("Error: Tiempo de espera agotado al intentar obtener una conexión del pool. Reintentando...")
        except SQLAlchemyError as e:
            logging.error(f"Error al ejecutar la consulta: {e}")
            break

        attempts += 1
        time.sleep(retry_delay)

    logging.error("Error: No se pudo ejecutar la consulta en batch después de varios intentos.")
    return None

##############################################################################################################
# Conexiones y Acciones sobre Colas SQS
##############################################################################################################

def get_sqs_client(queue_name):
    """
    Retorna un cliente de SQS configurado para la cola especificada.

    :param queue_name: Nombre de la cola SQS.
    :return: Cliente SQS de boto3 y la URL de la cola.
    """
    try:
        # Crear el cliente de SQS
        sqs_client = boto3.client('sqs')

        # Obtener la URL de la cola
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']

        logging.info(f"Cliente de SQS creado exitosamente para la cola '{queue_name}'.")
        
        return sqs_client, queue_url

    except ClientError as e:
        logging.error(f"Error al crear el cliente de SQS para la cola '{queue_name}': {e}")
        return None, None
    

def delete_message_from_sqs(sqs, p_queue_url, p_receipt_handle):
    """
    Elimina un mensaje de la cola SQS.

    :param sqs: Cliente de SQS.
    :param p_queue_url: URL de la cola SQS.
    :param p_receipt_handle: Identificador del mensaje a eliminar.
    :return: True si se eliminó exitosamente, False en caso contrario.
    """
    try:
        sqs.delete_message(
            QueueUrl=p_queue_url,
            ReceiptHandle=p_receipt_handle 
        )
        logging.info("Mensaje eliminado exitosamente de la cola.")
        return True
    except ClientError as e:
        logging.error(f"Error al eliminar el mensaje de la cola: {e}")
        return False
    

def get_messages_from_sqs(sqs, p_queue_url, messages_qty):
    """
    Recibe mensajes de la cola SQS.

    :param sqs: Cliente de SQS.
    :param p_queue_url: URL de la cola SQS.
    :param messages_qty: Cantidad máxima de mensajes a recibir.
    :return: Mensajes recibidos o False en caso de error.
    """
    try:
        response = sqs.receive_message(
            QueueUrl=p_queue_url,
            MaxNumberOfMessages=messages_qty,
            WaitTimeSeconds=5  # Tiempo de espera para recibir mensajes
        )
        
        if 'Messages' in response:
            logging.info(f"Se recibieron {len(response['Messages'])} mensajes de la cola.")
        else:
            logging.info("No se recibieron mensajes.")

        return response
    except ClientError as e:
        logging.error(f"Error al obtener mensajes de la cola: {e}")
        return False

##############################################################################################################
# Conexiones y Acciones sobre MongoDB
##############################################################################################################

def get_mongo_client(database_name, retry_attempts=3, retry_delay=5):
    """
    Retorna un cliente configurado para MongoDB con ajustes de pool de conexiones.

    :param database_name: Nombre de la base de datos de MongoDB a la que deseas conectarte.
    :param retry_attempts: Número de intentos de reintento en caso de fallo.
    :param retry_delay: Tiempo en segundos entre intentos de reintento.
    :return: Objeto de la base de datos o None en caso de error.
    """
    credenciales_bd = get_credentials(database_name)

    attempts = 0
    while attempts < retry_attempts:
        try:
            # Extraer credenciales
            host = credenciales_bd['db_host']
            port = credenciales_bd['db_port']
            database = credenciales_bd['db_name']
            user = credenciales_bd['db_user']
            password = credenciales_bd['db_password']
            
            # Formatear la URL de conexión
            uri = f'mongodb://{user}:{password}@{host}/'
        

            client = pymongo.MongoClient(uri)

            # Crear el cliente de MongoDB con configuración de pool y certificado TLS
            # client = pymongo.MongoClient(
            #     uri,
            #     retryWrites=False,  # Deshabilita los reintentos de escritura
            #     tlsCAFile=certifi.where(),  # Verificación de certificado TLS
            #     maxPoolSize=200,   # Tamaño máximo del pool de conexiones
            #     minPoolSize=10,    # Tamaño mínimo del pool de conexiones
            #     maxIdleTimeMS=30000  # Tiempo máximo de inactividad
            # )
            #
            # Obtener la base de datos
            db = client[database]
            
            logging.info(f"Cliente de MongoDB creado exitosamente para la base de datos '{database}'")
            
            return db

        except ConnectionFailure as e:
            logging.warning(f"Error al conectar a MongoDB (intento {attempts + 1}/{retry_attempts}): {e}")
            attempts += 1
            time.sleep(retry_delay)
        except PyMongoError as e:
            logging.error(f"Error general al conectar a MongoDB: {e}")
            break

    logging.error("Error: No se pudo conectar a MongoDB después de varios intentos.")
    return None
