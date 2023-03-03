import socket

BUFFER_SIZE = 1024

class Buffer(object):
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def get_line(self):
        while b"\r\n" not in self.buffer:
            data = self.sock.recv(BUFFER_SIZE)
            if not data: # socket is closed
                return None
            self.buffer += data
        line, sep, self.buffer = self.buffer.partition(b"\r\n")
        return line.decode()
    

def parse_message(messages):
    if messages[0][0] != "*":
        return []
    
    result = []
    msg_len = int(messages[0][1:])
    i = 1
    while i <= msg_len:
        if messages[i][0] == "$" and int(messages[i][1:]) > 0:
            result.append(messages[i + 1].strip())
            i += 1
        elif messages[i][0] == ":":
            result.append(int(messages[i][1:]))

        i += 1

    return result

def process_command(commands):
    for command in commands:
        if command.lower() == "ping":
            return "PONG"
        
    return None


def serialize(result):
    if isinstance(result, str):
        return b"+" + bytes(result, "utf-8") + b"\r\n"
    return None


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()
    
    conn, addr = server_socket.accept()
    
    with conn:
        while True:
            data = conn.recv(BUFFER_SIZE)
            if not data:
                break

            message = data.decode("utf-8").split("\r\n")
            command = parse_message(message)
            result = process_command(command)
            serialized_data = serialize(result)

            conn.sendall(serialized_data)

    server_socket.close()


if __name__ == "__main__":
    main()
