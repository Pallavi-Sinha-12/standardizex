class SourceColumnsAdditionError(Exception):
    """
    Raised when there is an issue adding source columns to the standardized data product.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class NewColumnAdditionError(Exception):
    """
    Raised when an error occurs while adding new columns during the standardization process.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ColumnDescriptionUpdateError(Exception):
    """
    Raised when an error occurs while updating the column descriptions during the standardization process.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class CopyToStandardizedDataProductError(Exception):
    """
    Raised when an error occurs while copying data from the temporary standardized data product to the actual standardized data product.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class TemporaryStandardizedDataProductDropError(Exception):
    """
    Raised when error occurs while dropping the temporary standardized data product.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ConfigTemplateGenerationError(Exception):
    """
    Raised when an error occurs while generating the configuration template.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ConfigTypeOrVersionError(Exception):
    """
    Raised when the configuration type or version is not supported.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DataProductDisplayError(Exception):
    """
    Raised when an error occurs while displaying the data product with the
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
