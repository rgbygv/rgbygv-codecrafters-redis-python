import asyncio
import re
import sys
import time
from asyncio import StreamReader, StreamWriter

from app.redis import NULL, OK, decode, encode, read_rdb

# globle config read from commands
# ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

DIR = None
DBFILENAME = None

# global for client
m: dict[bytearray, bytearray] = dict()
expiry: dict[bytearray, int] = dict()


async def handle_client(reader: StreamReader, writer: StreamWriter):
    # print(f"Accepted connection from {addr}")
    while True:
        msg = await reader.read(1024)
        if len(msg) == 0:
            break
        print(f"Received: {decode(msg)}")
        # TODO: make all message to str
        command, *args = decode(msg)
        command = command.upper()  # ignore case
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
                expiry[k] = time.time() + t / 1000
            else:
                raise NotImplementedError
            response = OK
        elif command == b"GET":
            # TODO: need lock
            k = args[0]
            if k in m and (k not in expiry or time.time() <= expiry[k]):
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
        elif command == b"KEYS":
            assert len(args) == 1
            pattern = args[0].decode().replace("*", ".*")
            print(f"search keys like {pattern} in {m}")
            response = []
            for key in m.keys():
                if re.match(pattern.encode(), key):
                    response.append(key)
            if response:
                response = encode(response, True)
            else:
                response = NULL
        else:
            print(command)
            raise NotImplementedError

        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def main():
    # TODO: use click to handle cli
    if len(sys.argv) >= 5:
        global DIR, DBFILENAME, m, expiry
        assert sys.argv[1] == "--dir"
        DIR = sys.argv[2]
        assert sys.argv[3] == "--dbfilename"
        DBFILENAME = sys.argv[4]
        m, expiry = read_rdb(DIR, DBFILENAME)

    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
