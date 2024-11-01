import socket  # noqa: F401
import asyncio


async def handle_client(conn: socket, addr: str):
    print(f"Accepted connection from {addr}")
    loop = asyncio.get_event_loop()
    while True:
        msg = await loop.sock_recv(conn, 1024)
        if len(msg) == 0:
            break
        print(f"Received: {msg}")
        response = b"+PONG\r\n"
        print(f"Sending response {response}")
        await loop.sock_sendall(conn, response)
    # print(f"Closes connection from {addr}")


async def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    loop = asyncio.get_event_loop()
    while True:
        conn, addr = await loop.sock_accept(server_socket)
        conn.setblocking(False)
        loop.create_task(handle_client(conn, addr))


if __name__ == "__main__":
    asyncio.run(main())
