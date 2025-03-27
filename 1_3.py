class Server:
    __ip = 0

    def __init__(self):
        Server.__ip += 1
        self.ip = Server.__ip + 1
        self.buffer = []

    def get_data(self):
        data = self.buffer
        self.buffer = []
        return data
    
    def get_ip(self):
        return self.ip
    
    def send_data(self, data):
        Router._buffer.append(data) # тут очень сомнительно, может привязывать сервер к роутеру при регистрации?????

    
class Router:
    _servers = {}
    _buffer = []
    
    def link(self, server):
        if server.ip not in self._servers:
            self._servers[server.ip] = server 

    def unlink(self, server):
        if server.ip in self._servers:
            self._servers.pop(server.ip)
    
    def send_data(self):
        for data in self._buffer:
            if data.ip in self._servers:
                server = self._servers[data.ip]
                server.buffer.append(data)
        self._buffer = []


class Data:
    def __init__(self, data, ip):
        self.data = data
        self.ip = ip
    



# router = Router()
# s1 = Server()
# s2 = Server()
# router.link(s1)
# router.link(s2)
# data1 = Data('test1', 2)
# s1.send_data(data1)
# router.send_data()
# print(s2.buffer)
# s2.get_data()
# print(s2.buffer)

