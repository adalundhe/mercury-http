from .request import HTTP2Request


class HTTP2Response:

    def __init__(self, name: str, request: HTTP2Request, error: Exception = None) -> None:
        self.name = name
        self.url = request.url.full
        self.method = request.method
        self.path = request.url.path
        self.hostname = request.url.hostname
        self.content = b''
        self.error = error
        self.time = 0
        self.user = request.user
        self.tags = request.tags
        self.status = None
        self.headers = []
        self.extentions = {}
        