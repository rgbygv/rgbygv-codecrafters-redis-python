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
REPLICAOF = None

# global for client
m: dict[bytearray, bytearray] = dict()
expiry: dict[bytearray, int] = dict()


async def handle_client(reader: StreamReader, writer: StreamWriter):
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
            response = [b"role"]
            role = b"master" if not REPLICAOF else b"slave"
            response.append(role)
            response.append(b"master_replid")
            master_replid = b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            response.append(master_replid)
            response.append(b"master_repl_offset")
            master_repl_offset = b"0"
            response.append(master_repl_offset)

            response = encode([b":".join(response)])
        else:
            print(command)
            raise NotImplementedError

        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def send_message_to_master(master_host, master_port, message):
    _, writer = await asyncio.open_connection(master_host, master_port)

    msg = encode([message], array_mode=True)
    writer.write(msg)
    await writer.drain()
    print(f"Sent PING to master: {msg}")

    writer.close()
    await writer.wait_closed()


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
    parser.add_argument(
        "--replicaof", type=str, default=None, help="<MASTER_HOST> <MASTER_PORT>"
    )

    args = parser.parse_args()
    global DIR, DBFILENAME, PORT, REPLICAOF, m, expiry
    PORT = args.port
    DIR = args.dir
    DBFILENAME = args.dbfilename
    REPLICAOF = args.replicaof

    if REPLICAOF:
        master_host, master_port = REPLICAOF.split(" ")
        master_port = int(master_port)
        master = await asyncio.start_server(handle_client, master_host, master_port)
        asyncio.create_task(master.serve_forever())
        await send_message_to_master(master_host, master_port, b"PING")

        # async with master:
        #     await master.serve_forever()

    m, expiry = read_rdb(DIR, DBFILENAME)

    server = await asyncio.start_server(handle_client, "localhost", PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
