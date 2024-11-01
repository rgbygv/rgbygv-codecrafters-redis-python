import asyncio
from asyncio import StreamWriter, StreamReader
from app.redis import decode, encode, OK, NULL
import time


async def handle_client(reader: StreamReader, writer: StreamWriter):
    # print(f"Accepted connection from {addr}")
    m = dict()
    expiry = dict()
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
            if len(args) == 2:
                k, v = args
                m[k] = encode([v])
            elif len(args) == 4:
                k, v, _, t = args
                t = int("".join(map(chr, t)))
                m[k] = encode([v])
                expiry[k] = time.perf_counter_ns() + (10**6) * t
            else:
                raise NotImplementedError
            response = OK
        elif command == b"GET":
            k = args[0]
            if k in m and (k not in expiry or time.perf_counter_ns() <= expiry[k]):
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
