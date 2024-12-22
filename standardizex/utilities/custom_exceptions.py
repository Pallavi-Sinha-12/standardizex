class SourceColumnsAdditionError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class NewColumnAdditionError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ColumnDescriptionUpdateError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class CopyToStandardizedDataProductError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class TemporaryStandardizedDataProductDropError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class StandardizationError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)