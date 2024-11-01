import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    command = server_socket.accept()  # wait for client
    print(f"recieve command {command}")
    response = b"+PONG\r\n"
    print(response)
    server_socket.sendall(response)


if __name__ == "__main__":
    main()
