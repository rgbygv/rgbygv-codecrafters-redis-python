import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.accept()  # wait for client

    server_socket.send("+PONG\r\n")


if __name__ == "__main__":
    main()
