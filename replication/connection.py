class Connection(object):

    def __init__(self, host, user, port, password, charset, timeout, name=None):
        super().__init__()
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.charset = charset
        self.timeout = timeout
        self.name = name
