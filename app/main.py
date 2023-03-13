import socket
import selectors
import types
import threading
import socketserver


HOST = "localhost"
PORT  = 6379
BUFFER_SIZE = 1024
    

sel = selectors.DefaultSelector()

DATA = {}

def parse_message(messages):
    if messages[0][0] != "*":
        return []
    
    result = []
    msg_len = len(messages)
    i = 1
    while i < msg_len:
        if not messages[i]:
            i += 1
            continue

        if messages[i][0] == "$" and int(messages[i][1:]) > 0:
            result.append(messages[i + 1].strip())
            i += 1
        elif messages[i][0] == ":":
            result.append(int(messages[i][1:]))

        i += 1
    return result

def process_command(commands):
    if not commands:
        return None
    
    the_command = commands[0].lower()
    if the_command.lower() == "ping":
        return "PONG"
    elif the_command.lower() == "echo":
        if len(commands) == 1:
            return ""
        else:
            return commands[1]
    elif the_command.lower() == "set":
        if len(commands) < 3:
            return ""
        else:
            DATA[commands[1]] = commands[2]
            return "OK"
    elif the_command.lower() == "get":
        if len(commands) == 1:
            return ""
        return DATA.get(commands[1], "")

    return None


def serialize(result):
    if isinstance(result, str):
        return b"$" + str(len(result)).encode() + b"\r\n" + bytes(result, "utf-8") + b"\r\n"
    return None


def accept_connection(sock):
    conn, addr = sock.accept()

    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)


def serve_connection(key, mask):
    sock = key.fileobj
    data = key.data

    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(BUFFER_SIZE)
        if recv_data:
            message = recv_data.decode("utf-8").split("\r\n")
            command = parse_message(message)
            result = process_command(command)
            data.outb = serialize(result)
        else:
            sel.unregister(sock)
            sock.close()

    if mask & selectors.EVENT_WRITE:
        if data.outb:
            sent = sock.send(data.outb)
            data.outb = data.outb[sent:]

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    server_socket.setblocking(False)

    sel.register(server_socket, selectors.EVENT_READ, data=None)

    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_connection(key.fileobj)
                else:
                    serve_connection(key, mask)
    except KeyboardInterrupt:
        print("Exiting Application")
    finally:
        sel.close()

    server_socket.close()


if __name__ == "__main__":
    main()

