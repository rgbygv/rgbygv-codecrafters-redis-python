import socket  # noqa: F401
import asyncio


async def handle_client(conn: socket, addr: str):
    print(f"Accepted connection from {addr}")
    while True:
        msg = await conn.recv(1024)
        if not msg:
            break
        print(f"Received: {msg}")
        response = b"+PONG\r\n"
        print(f"Sending response {response}")
        await conn.sendall(response)
    conn.close()
    print(f"Closes connection from {addr}")


async def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    loop = asyncio.get_event_loop()
    while True:
        conn, addr = await loop.sock_accept(server_socket)
        loop.create_task(handle_client(conn, addr))


if __name__ == "__main__":
    asyncio.run(main())
