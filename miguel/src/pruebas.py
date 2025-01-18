l = []


def list_to_sql_in_str(data):
    return "('" + "', '".join(data) + "')"
