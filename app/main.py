import argparse
import asyncio
import binascii
import re
import time
from asyncio import StreamReader, StreamWriter

from app.redis import NULL, OK, Redis, decode, decode_master, encode, read_rdb

r = Redis()


async def send_message_to_master(master_host, master_port, messages: list[bytearray]):
    reader, writer = await asyncio.open_connection(master_host, master_port)
    responses = [
        b"+PONG\r\n",
        OK,
        OK,
        b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n",
    ]
    for i, message in enumerate(messages):
        writer.write(message)
        await writer.drain()
        print(f"Sent {message} to master")
        if i < len(responses):
            response = await reader.read(len(responses[i]))
            print(f"received {response} from master")

    # rdb_file
    rdb_file_len = await reader.readline()
    rdb_file = await reader.read(int(rdb_file_len[1:]))
    print(f"rdb file length: {rdb_file_len}, file: {rdb_file}")

    offset = 0
    while 1:
        msg = await reader.read(1024)
        if not msg:
            break
        print(f"replica receive master's command {msg}")
        multi_command = decode_master(msg)
        for cmd in multi_command:
            print(cmd)
            cmd[0] = cmd[0].upper()
            if cmd[0] == b"SET":
                await handle_command(encode(cmd), None, writer)
            elif cmd[0] == b"REPLCONF":
                print("Send ack response to master")
                response = encode(f"REPLCONF ACK {offset}".encode().split())
                writer.write(response)
                await writer.drain()
            else:
                # ping
                pass
            offset += len(encode(cmd, array_mode=True))


async def send_command_to_replica(replica_port, writer, command: bytearray):
    writer.write(command)
    await writer.drain()
    print(f"Sent {command} to replica {replica_port}")
    # writer.close()
    # await writer.wait_closed()


async def handle_client(reader: StreamReader, writer: StreamWriter):
    _, connection_port, *_ = writer.get_extra_info("peername")
    print(f"Connect from {connection_port}")

    while True:
        msg = await reader.read(1024)
        if len(msg) == 0:
            break
        print(f"Received: {msg}")
        response = await handle_command(msg, connection_port, writer)
        if response:
            print(f"Sending response {response}")
            writer.write(response)
            await writer.drain()


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

    r.PORT = args.port
    r.DIR = args.dir
    r.DBFILENAME = args.dbfilename
    r.REPLICAOF = args.replicaof

    r.m, r.expiry = read_rdb(r.DIR, r.DBFILENAME)

    # await asyncio.create_task(asyncio.start_server(handle_client, "localhost", PORT))
    async def _start_server():
        server = await asyncio.start_server(handle_client, "localhost", r.PORT)
        print(f"Server started on port {r.PORT}")
        async with server:
            await server.serve_forever()

    server_task = asyncio.create_task(_start_server())

    if r.REPLICAOF:
        master_host, master_port = r.REPLICAOF.split(" ")
        await send_message_to_master(
            master_host,
            master_port,
            [
                encode([b"PING"], array_mode=True),
                encode([b"REPLCONF", b"listening-port", r.PORT.encode()]),
                encode(b"REPLCONF capa psync2".split()),
                encode(b"PSYNC ? -1".split()),
            ],
        )

    await server_task


async def handle_command(msg: bytes, connection_port: str | None, writer):
    print(f"handle message {decode(msg)}")
    command, *args = decode(msg)
    command = command.upper()  # ignore case
    response = None
    if command == b"PING":
        response = b"+PONG\r\n"
    elif command == b"ECHO":
        response = encode(args)
    elif command == b"SET":
        if len(args) == 2:
            k, v = args
            r.m[k] = encode([v])
        elif len(args) == 4:
            k, v, _, t = args
            t = int(t.decode())
            r.m[k] = encode([v])
            r.expiry[k] = time.time() + t / 1000
        else:
            raise NotImplementedError
        response = OK
        # since we propagate new commmand, so all replica become not acked
        r.ack_replica = 0
        r.expect_offset += len(msg)
        for _replica_port, _writer in r.connect_replica.values():
            print(f"Sending propagating command {msg} to replica {_replica_port}")
            await send_command_to_replica(_replica_port, _writer, msg)

        ack_msg = encode(b"REPLCONF GETACK *".split())
        for _replica_port, _writer in r.connect_replica.values():
            print(f"Sending acknowledgement command to replica {_replica_port}")
            await send_command_to_replica(_replica_port, _writer, ack_msg)
        r.expect_offset += len(ack_msg)
    elif command == b"GET":
        k = args[0]
        if k in r.m and (k not in r.expiry or time.time() <= r.expiry[k]):
            response = r.m[k]
        else:
            response = NULL
    elif command == b"CONFIG":
        assert len(args) == 2
        sub_command, file_type = args
        assert sub_command == b"GET"
        if file_type == b"dir":
            response = encode([file_type, r.DIR.encode()])
        elif file_type == b"dbfilename":
            response = encode([file_type, r.DBFILENAME.encode()])
    elif command == b"KEYS":
        assert len(args) == 1
        pattern = args[0].decode().replace("*", ".*")
        print(f"search keys like {pattern} in {r.m}")
        response = []
        for key in r.m.keys():
            if re.match(pattern.encode(), key):
                response.append(key)
        if response:
            response = encode(response, True)
        else:
            response = NULL
    elif command == b"INFO":
        assert args == [b"replication"]
        response = [b"role"]
        role = b"master" if not r.REPLICAOF else b"slave"
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
            r.replica_ports[connection_port] = replica_port
            response = OK
        elif args[0] == b"capa":
            response = OK
        elif args[0] == b"ACK":
            offset = int(args[1].decode())
            print(f"receive replica response of ack: {args}")
            ack_msg = encode(b"REPLCONF GETACK *".split())
            if offset == r.expect_offset - len(ack_msg):
                r.ack_replica += 1
            else:
                print(f"expect offset: {r.expect_offset}, actual: {offset}")
    elif command == b"PSYNC":
        response = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
        print(f"Sending response {response}")
        writer.write(response)
        await writer.drain()
        hex_empty_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        bin_empty_file = binascii.unhexlify(hex_empty_file)
        response = encode([bin_empty_file], trail_space=False)
        r.connect_replica[connection_port] = r.replica_ports[connection_port], writer
    elif command == b"WAIT":
        expect_replica, expiry_time = map(int, (arg.decode() for arg in args))
        print(
            f"wait for {expect_replica} replicas have acknowledged or {expiry_time} ms"
        )
        cur_time = time.time()
        while (
            r.ack_replica != -1
            and r.ack_replica < expect_replica
            and (time.time() - cur_time) * 1000 < expiry_time
        ):
            await asyncio.sleep(0)
        # don't propagating write command
        if r.ack_replica == -1:
            response = encode([len(r.replica_ports)])
        else:
            response = encode([r.ack_replica])
    else:
        print(command)
        raise NotImplementedError
    return response


if __name__ == "__main__":
    asyncio.run(main())
