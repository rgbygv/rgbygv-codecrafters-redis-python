import argparse
import asyncio
from asyncio import StreamReader, StreamWriter

from app.redis import decode, encode, read_rdb
from app.command import handle_command, send_message_to_master

PORT = None
DIR = None
DBFILENAME = None
REPLICAOF = None

m: dict[bytearray, bytearray] = dict()
expiry: dict[bytearray, int] = dict()

replica_ports = {}
connect_replica = {}


async def handle_client(reader: StreamReader, writer: StreamWriter):
    _, connection_port, *_ = writer.get_extra_info("peername")
    print(f"Connect from {connection_port}")

    while True:
        msg = await reader.read(1024)
        if len(msg) == 0:
            break
        print(f"Received: {decode(msg)}")
        response = await handle_command(msg, connection_port, writer)
        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()

    # TODO: we give replica writer to connect_replcia dict


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
