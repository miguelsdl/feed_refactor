class SQLValuesTupleBuilder:
    def __init__(self):
        # Devuelve lo que va dentro de los parentesis
        self.sql_tuple = "({})"
        self.elements = list()

    def add(self, e):
        self.elements.append(e)