import asyncio
from asyncio import StreamWriter, StreamReader
from app.redis import decode, encode, OK, NULL
import time
import sys

# globle config read from commands
# ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

DIR = None
DBFILENAME = None


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
            # TODO: need lock
            if len(args) == 2:
                k, v = args
                m[k] = encode([v])
            elif len(args) == 4:
                k, v, _, t = args
                t = int(t.decode())
                m[k] = encode([v])
                expiry[k] = time.perf_counter_ns() + (10**6) * t
            else:
                raise NotImplementedError
            response = OK
        elif command == b"GET":
            # TODO: need lock
            k = args[0]
            if k in m and (k not in expiry or time.perf_counter_ns() <= expiry[k]):
                response = m[k]
            else:
                response = NULL
        elif command == b"CONFIG":
            assert len(args) == 2
            sub_command, file_type = args
            assert sub_command == b"GET"
            if file_type == b"dir":
                response = encode([file_type, DIR.encode()])
            elif file_type == b"dbfilename":
                response = encode([file_type, DBFILENAME.encode()])
        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def main():
    # TODO: use click to handle cli
    if len(sys.argv) >= 5:
        global DIR, DBFILENAME
        assert sys.argv[1] == "--dir"
        DIR = sys.argv[2]
        assert sys.argv[3] == "--dbfilename"
        DBFILENAME = sys.argv[4]

    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
