import argparse
import asyncio
import binascii
import re
import time
from asyncio import StreamReader, StreamWriter

from app.redis import NULL, OK, decode, encode, read_rdb

PORT = None
DIR = None
DBFILENAME = None
REPLICAOF = None

m: dict[bytearray, bytearray] = dict()
expiry: dict[bytearray, int] = dict()

replica_ports = {}
connect_replica = {}

SILENT = False


async def handle_client(reader: StreamReader, writer: StreamWriter):
    _, connection_port, *_ = writer.get_extra_info("peername")
    print(f"Connect from {connection_port}")

    while True:
        msg = await reader.read(1024)
        if len(msg) == 0:
            break
        print(f"Received: {decode(msg)}")
        command, *args = decode(msg)
        command = command.upper()  # ignore case
        if command == b"PING":
            response = b"+PONG\r\n"
        elif command == b"ECHO":
            response = encode(args)
        elif command == b"SET":
            if connection_port == REPLICAOF:  # don't response
                global SILENT
                SILENT = True
            for _replica_port, _writer in connect_replica.values():
                print(f"Propagating command {msg} to replica {_replica_port}")
                await send_command_to_replica(_replica_port, _writer, msg)
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
        elif command == b"REPLCONF":
            # [b'listening-port', b'6380']
            # We ignore the origin replica port
            # replace it by the connect_port by replica
            if args[0] == b"listening-port":
                replica_port = args[1].decode()
            replica_ports[connection_port] = replica_port

            response = OK
        elif command == b"PSYNC":
            response = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
            print(f"Sending response {response}")
            writer.write(response)
            await writer.drain()
            hex_empty_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
            bin_empty_file = binascii.unhexlify(hex_empty_file)
            response = encode([bin_empty_file], trail_space=False)
            connect_replica[connection_port] = replica_ports[connection_port], writer

        else:
            print(command)
            raise NotImplementedError

        if not SILENT:
            print(f"Sending response {response}")
            writer.write(response)
            await writer.drain()
            # except ConnectionResetError:
            #     connect_replica.pop(connection_port)
    # TODO: we give replica writer to connect_replcia dict


# can i do this by replica, not create a new connnection?
async def send_message_to_master(master_host, master_port, messages: list[bytearray]):
    reader, writer = await asyncio.open_connection(master_host, master_port)

    responses = [b"+PONG\r\n", OK, OK]
    # b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n",
    for i, message in enumerate(messages):
        writer.write(message)
        await writer.drain()
        print(f"Sent {message} to master")

        response = await reader.read(1024)
        print(f"received {response} from master")
        if i < len(responses):
            assert response == responses[i]

    # here can receive response from master
    while 1:
        msg = await reader.read(1024)
        if not msg:
            break
        print(f"Reveive message {msg}")

    # writer.close()
    # await writer.wait_closed()


async def send_command_to_replica(replica_port, writer, command: bytearray):
    writer.write(command)
    await writer.drain()
    print(f"Sent {command} to replica {replica_port}")
    # writer.close()
    # await writer.wait_closed()


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
        "--port", type=str, default="6379", help="Port number to use (default: 6380)"
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
        await send_message_to_master(
            master_host,
            master_port,
            [
                encode([b"PING"], array_mode=True),
                encode([b"REPLCONF", b"listening-port", PORT.encode()]),
                encode(b"REPLCONF capa psync2".split()),
                encode(b"PSYNC ? -1".split()),
            ],
        )

    # TODO: handle ownship of this
    m, expiry = read_rdb(DIR, DBFILENAME)

    server = await asyncio.start_server(handle_client, "localhost", PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
