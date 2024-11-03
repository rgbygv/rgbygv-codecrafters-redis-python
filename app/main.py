import argparse
import asyncio
import re
import time
from asyncio import StreamReader, StreamWriter

from app.redis import NULL, OK, decode, encode, read_rdb

# TODO: make global vars in their place
PORT = None
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
        elif command == b"INFO":
            assert args == [b"replication"]
            response = encode([b"role" + b":" + b"master"])
        else:
            print(command)
            raise NotImplementedError

        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def main():
    parser = argparse.ArgumentParser(description="Process some arguments.")

    parser.add_argument(
        "--dir",
        type=str,
        default=".",
        help="Directory to use (default: current directory)",
    )
    parser.add_argument(
        "--dbfilename",
        type=str,
        default="dump.rdb",
        help="Database filename (default: dump.rdb)",
    )
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to use (default: 6380)"
    )

    args = parser.parse_args()
    global DIR, DBFILENAME, PORT, m, expiry
    PORT = args.port
    DIR = args.dir
    DBFILENAME = args.dbfilename
    m, expiry = read_rdb(DIR, DBFILENAME)

    server = await asyncio.start_server(handle_client, "localhost", PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
