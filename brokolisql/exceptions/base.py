class BrokoliSQLException(Exception):
    """Base exception for BrokoliSQL errors."""

    def __init__(self, message, hint=None):
        super().__init__(message)
        self.message = message
        self.hint = hint

    def __str__(self):
        return f"{self.message}" + (f"\n\n Hint: {self.hint}" if self.hint else "")

class DatabaseConnectionError(BrokoliSQLException):
    """Exception raised when database connection fails."""
    pass

class DatabaseQueryError(BrokoliSQLException):
    """Exception raised when a database query fails."""
    pass

class APIConnectionError(BrokoliSQLException):
    """Exception raised when API connection fails."""
    pass

class APIResponseError(BrokoliSQLException):
    """Exception raised when API response processing fails."""
    pass

class FileFormatNotSupported(BrokoliSQLException):
    def __init__(self, ext):
        message = f"The file extension '{ext}' is not supported."
        hint = "Try using CSV, Excel (.xls/.xlsx), JSON, XML, or HTML — or specify the format manually with `--format`."
        super().__init__(message, hint)

class FileNotFound(BrokoliSQLException):
    def __init__(self, filepath):
        message = f"The file '{filepath}' was not found."
        hint = "Make sure the path is correct. You can use an absolute path or check your current working directory."
        super().__init__(message, hint)

class FileParsingError(BrokoliSQLException):
    def __init__(self, filepath, original_exception):
        message = f"Failed to parse file '{filepath}'."
        hint = f"Make sure the file structure is valid for its format. Error details: {original_exception}"
        super().__init__(message, hint)

class FileLoadError(BrokoliSQLException):
    def __init__(self, filepath, original_exception):
        message = f"An error occurred while trying to load '{filepath}'."
        hint = f"This might be a permission issue or an unexpected file encoding. Details: {original_exception}"
        super().__init__(message, hint)
        