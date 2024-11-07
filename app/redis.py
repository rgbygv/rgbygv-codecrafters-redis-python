from dataclasses import dataclass, field
from typing import Tuple
from math import inf
from collections import defaultdict

OK = b"+OK\r\n"
NULL = b"$-1\r\n"


@dataclass
class Stream:
    entry_id: bytes
    kvs: dict = field(default_factory=dict)

    def encode(self):
        res = []
        res.append(self.entry_id)
        inner = []
        for k, v in self.kvs.items():
            inner.append(k)
            inner.append(v)
        res.append(inner)
        return res

    def valid(self, start, end, inclusive=True):
        if inclusive:
            return self.ge(start) and self.le(end)
        return self.gt(start) and self.lt(end)

    @staticmethod
    def parse(entry_id: bytes):
        if entry_id == b"-":
            return 0, 0
        if entry_id == b"+":
            return inf, inf
        if b"-" in entry_id:
            mt, sn = map(int, entry_id.decode().split("-"))
            return mt, sn
        return int(entry_id.decode()), 0

    def ge(self, entry_id):
        return self.parse(self.entry_id) >= self.parse(entry_id)

    def le(self, entry_id):
        return self.parse(self.entry_id) <= self.parse(entry_id)

    def gt(self, entry_id):
        return self.parse(self.entry_id) > self.parse(entry_id)

    def lt(self, entry_id):
        return self.parse(self.entry_id) < self.parse(entry_id)


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

    ack_replica: int = -1
    expect_offset: int = 0

    last_entry_id: bytes | None = None
    last_seq: dict[int, int] = field(default_factory=dict)

    streams_dict: defaultdict[bytes, list[Stream]] = field(
        default_factory=lambda: defaultdict(list)
    )


def decode_master(message):
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
    print(f"decode multi command :{msgs}")
    return msgs


# TODO: return list[str]
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


# TODO: input should be str, not bytes
def encode(
    s: list[bytearray | int], array_mode: bool = False, trail_space: bool = True
) -> bytearray:
    res = []
    print(f"trying encode {s}")
    if len(s) == 1 and not array_mode:
        # simple string
        if isinstance(s[0], bytes):
            res.append(f"${len(s[0])}".encode())
            res.append(s[0])
        # int
        elif isinstance(s[0], int):
            res.append(f":{s[0]}".encode())
        elif isinstance(s[0], list):
            res.append(b"*1")
            res.append(encode(s[0], trail_space=False))
        else:
            raise NotImplementedError
    else:
        res.append(f"*{len(s)}".encode())
        for key in s:
            if isinstance(key, list):
                res.append(encode(key, trail_space=False))
            else:
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
    # streams = [
    #     [b"1526985054069-0", [b"temperature", b"36", b"humidity", b"95"]],
    #     [b"1526985054079-0", [b"temperature", b"37", b"humidity", b"94"]],
    # ]
    # print(streams)

    # print(encode(streams).decode())
    # print(encode(streams))
    msg = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    s = decode(msg)
    rmsg = encode(s)
    assert msg == rmsg
    read_rdb(".", "dump.rdb")
