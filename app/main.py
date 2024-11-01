import socket  # noqa: F401
import asyncio
from asyncio import StreamWriter, StreamReader
from app.redis import decode, encode, OK, NULL


async def handle_client(reader: StreamReader, writer: StreamWriter):
    # print(f"Accepted connection from {addr}")
    m = dict()
    while True:
        msg = await reader.read(1024)
        if len(msg) == 0:
            break
        print(f"Received: {msg}")
        command, *args = decode(msg)
        if command == b"PING":
            response = b"+PONG\r\n"
        elif command == b"ECHO":
            response = encode(args)
        elif command == b"SET":
            k, v = args
            m[k] = encode([v])
            response = OK
        elif command == b"GET":
            k = args[0]
            if k in m:
                response = m[k]
            else:
                response = NULL
        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
