import asyncio
import binascii
import re
import time


async def send_message_to_master(master_host, master_port, messages: list[bytearray]):
    from app.redis import OK, decode_write, encode

    reader, writer = await asyncio.open_connection(master_host, master_port)

    responses = [b"+PONG\r\n", OK, OK]
    for i, message in enumerate(messages):
        writer.write(message)
        await writer.drain()
        print(f"Sent {message} to master")

        response = await reader.read(1024)
        print(f"received {response} from master")
        if i < len(responses):
            assert response == responses[i]

    # ?
    rdb_file = await reader.read(1024)
    print(f"replica receive rdbfile {rdb_file}")

    # writer.close()
    # await writer.wait_closed()

    # here can receive response from master
    while 1:
        msg = await reader.read(1024)  # write message: SET
        if not msg:
            break
        print(f"replica receive master's command {msg}")
        try:
            multi_set_command = decode_write(msg)
        except Exception:
            print(msg)
            multi_set_command = []

        for write_msg in multi_set_command:
            await handle_command(encode(write_msg), None, writer)


async def send_command_to_replica(replica_port, writer, command: bytearray):
    writer.write(command)
    await writer.drain()
    print(f"Sent {command} to replica {replica_port}")
    # writer.close()
    # await writer.wait_closed()


async def handle_command(msg: bytes, connection_port: str | None, writer):
    from app.main import (
        DBFILENAME,
        DIR,
        REPLICAOF,
        connect_replica,
        expiry,
        m,
        replica_ports,
    )
    from app.redis import NULL, OK, decode, encode

    print(f"handle message {decode(msg)}")
    command, *args = decode(msg)
    command = command.upper()  # ignore case
    if command == b"PING":
        response = b"+PONG\r\n"
    elif command == b"ECHO":
        response = encode(args)
    elif command == b"SET":
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
    return response
