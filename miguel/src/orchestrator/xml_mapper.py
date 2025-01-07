import re
from datetime import datetime, timedelta


# Funciones Auxiliares

def duration_to_seconds(duration):
    """
    Convierte una cadena de duración en formato ISO 8601 a segundos.
    
    :param duration: Cadena de duración en formato ISO 8601 (e.g., 'PT1H30M45S').
    :return: Duración total en segundos.
    :raises ValueError: Si el formato de la duración es inválido.
    """
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    
    if not match:
        raise ValueError(f"Formato de duración no válido: {duration}")

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds


def seconds_to_hhmmss(seconds):
    """
    Convierte una cantidad de segundos a formato hh:mm:ss.
    
    :param seconds: Duración en segundos.
    :return: Cadena en formato hh:mm:ss.
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def get_value_from_path(json_dict, path):
    """
    Extrae un valor de un diccionario JSON usando una ruta de claves separadas por '/'.
    Puede manejar namespaces en las claves.

    :param json_dict: Diccionario JSON del cual extraer el valor.
    :param path: Ruta separada por '/' para acceder al valor.
    :return: El valor encontrado en la ruta especificada.
    :raises KeyError: Si alguna clave no se encuentra en el diccionario.
    """
    keys = path.split('/')
    for key in keys:
        if ':' in key:
            # Remover el namespace de la clave
            key_without_ns = key.split(':')[1]  # Obtener la clave sin el namespace
        else:
            key_without_ns = key

        # Intentar acceder a la clave con o sin namespace
        if key in json_dict:
            json_dict = json_dict[key]
        elif key_without_ns in json_dict:
            json_dict = json_dict[key_without_ns]
        else:
            raise KeyError(f"Key '{key}' not found in path '{path}'")
    
    return json_dict


def timedelta_to_string(td):
    """
    Convierte un objeto timedelta en una cadena de texto en formato 'HH:MM:SS'.
    
    :param td: Objeto timedelta a convertir.
    :return: Cadena en formato 'HH:MM:SS'.
    """
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def datetime_to_string(dt):
    """
    Convierte un objeto datetime en una cadena de texto en formato ISO 8601.
    
    :param dt: Objeto datetime a convertir.
    :return: Cadena en formato ISO 8601.
    """
    return dt.isoformat()


def serialize_with_custom_types(data):
    """
    Serializa un diccionario o lista, manejando tipos de datos especiales como timedelta y datetime.
    
    :param data: Diccionario o lista a serializar.
    :return: El diccionario o lista serializado, con los tipos timedelta y datetime convertidos a cadenas.
    """
    if isinstance(data, list):
        return [serialize_with_custom_types(item) for item in data]
    elif isinstance(data, dict):
        return {k: serialize_with_custom_types(v) for k, v in data.items()}
    elif isinstance(data, timedelta):
        return timedelta_to_string(data)
    elif isinstance(data, datetime):
        return datetime_to_string(data)
    else:
        return data
    


def convert_timedelta_to_seconds(td):
    """
    Convierte un objeto datetime.timedelta a segundos.
    
    :param td: Objeto timedelta.
    :return: El valor en segundos como un entero.
    """
    if isinstance(td, timedelta):
        return int(td.total_seconds())
    return td
