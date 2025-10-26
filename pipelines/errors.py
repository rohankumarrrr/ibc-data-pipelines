class PipelineError(Exception):
    def __init__(self, code, message):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message

class DataConflictError(PipelineError):
    def __init__(self, detail="Duplicate or conflicting data detected"):
        super().__init__("E001", detail)

class AuthorizationError(PipelineError):
    def __init__(self, detail="User not authorized to perform this operation"):
        super().__init__("E002", detail)

class InvalidFormatError(PipelineError):
    def __init__(self, detail="Invalid data format or missing field"):
        super().__init__("E003", detail)

class DatabaseConnectionError(PipelineError):
    def __init__(self, detail="Failed to connect to database"):
        super().__init__("E004", detail)

class SheetReadError(PipelineError):
    def __init__(self, detail="Failed to read data from Google Sheet"):
        super().__init__("E005", detail)