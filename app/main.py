import socket  # noqa: F401
import asyncio


async def handle_client(s: socket):
    conn, addr = s.accept()  # wait for client
    print(f"Accepted connection from {addr}")
    while True:
        msg = await conn.recv(1024)
        print(msg)
        response = b"+PONG\r\n"
        print(f"sending response {response}")
        conn.sendall(response)


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handle_client(server_socket))


if __name__ == "__main__":
    main()
