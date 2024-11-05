from dataclasses import dataclass, field
from typing import Tuple

OK = b"+OK\r\n"
NULL = b"$-1\r\n"


@dataclass
class Redis:
    PORT: str = None
    DIR: str = None
    DBFILENAME: str = None
    REPLICAOF: str = None

    m: dict[bytearray, bytearray] = field(default_factory=dict)
    expiry: dict[bytearray, int] = field(default_factory=dict)

    replica_ports: dict = field(default_factory=dict)
    connect_replica: dict = field(default_factory=dict)


# b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'
def decode_write(message):
    msg = message.split(b"\r\n")[:-1]
    print(f"maybe contain multiple command: {msg}")
    n = len(msg)
    i = 0
    msgs = []
    while i < n:
        # assert msg[i][0:1] == b"*"
        res = []
        size = int(msg[i][1:])
        i += 1
        for _ in range(size):
            res.append(msg[i + 1])
            i += 2
        msgs.append(res)
    print(f"decode multi set command :{msgs}")
    return msgs


def decode(message: bytearray) -> list[bytearray]:
    msg = message.split(b"\r\n")[:-1]
    n = len(msg)
    flg = msg[0]
    if flg[0:1] == b"*":
        # list
        res = []
        for i in range(1, n, 2):
            # msg[i] is length, msg[i+1] is value
            res.append(msg[i + 1])
        return res
    elif flg[0:1] == b"$":
        # simple string
        return [flg[1]]
    else:
        raise NotImplementedError


def encode(
    s: list[bytearray], array_mode: bool = False, trail_space: bool = True
) -> bytearray:
    res = []
    if len(s) == 1 and not array_mode:
        # simple string
        res.append(f"${len(s[0])}".encode())
        res.append(s[0])
    else:
        res.append(f"*{len(s)}".encode())
        for key in s:
            n = len(key)
            res.append(f"${n}".encode())
            res.append(key)
    if trail_space:
        res.append(b"")
    return b"\r\n".join(res)


def read_rdb(dir: str, dbfile_name: str) -> Tuple[dict, dict]:
    file = f"{dir}/{dbfile_name}"

    m = dict()
    expiry = dict()

    import os

    if not os.path.exists(file):
        return m, expiry

    with open(file, "rb") as f:
        # ignore head and metadata session
        # just find the FC and FD

        """e.g:
        b'\xfe\x00\xfb\x01\x00\x00\x05mykey\x05myval'
        b'\xfe\x00\xfb\x03\x01\x00\x05mykey\x05myval
        \xfc\xe7\x03\xfc\xec\x92\x01\x00\x00\x00\x05hello\x05world
        \x00\x03foo\x03bar\xffE\xea\xea\x9b\xb7c\xdbL'
        """
        data = f.read(1)[0]
        while data != 0xFE:
            data = f.read(1)[0]

        # here use unpack bytes, so each value is a u8
        index, table_type, size, expires_size = f.read(4)
        # print(index, table_type, size, expires_size)
        assert table_type == 0xFB

        # data come random
        for _ in range(size):
            value_type = f.read(1)[0]
            # handle normal dict
            if value_type == 0:  # string, normal dict
                n = f.read(1)[0]
                key = f.read(n)
                n = f.read(1)[0]
                value = f.read(n)
                print(f"insert {key} {value}")
                m[key] = encode([value])
                continue
            # handle expiry table
            timestamp_type = value_type
            if timestamp_type == 0xFC:  # ms
                t = int.from_bytes(f.read(8), "little") / 1000
            elif timestamp_type == 0xFD:  # s
                t = int.from_bytes(f.read(4), "little")
            value_type = f.read(1)[0]
            assert value_type == 0, print(value_type)
            n = f.read(1)[0]
            key = f.read(n)
            n = f.read(1)[0]
            value = f.read(n)
            print(f"insert {key}: {value}, expiry: {t}")
            m[key] = encode([value])
            expiry[key] = t

    return m, expiry


if __name__ == "__main__":
    msg = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    s = decode(msg)
    rmsg = encode(s)
    assert msg == rmsg
    read_rdb(".", "dump.rdb")
