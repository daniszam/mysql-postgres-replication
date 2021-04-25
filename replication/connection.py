class Connection(object):

    def __init__(self, host, user, port, password, charset, timeout, server_id=None, name=None, database=None):
        super().__init__()
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.charset = charset
        self.timeout = timeout
        self.name = name
        self.server_id = server_id
        self.database = database
