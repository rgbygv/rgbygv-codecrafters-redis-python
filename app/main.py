import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()  # wait for client
    print(f"recieve client {conn} {addr}")
    while True:
        command = conn.recv()
        print(command)
        response = b"+PONG\r\n"
        print(f"sending response {response}")
        conn.sendall(response)


if __name__ == "__main__":
    main()
