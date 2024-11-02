from typing import Tuple

OK = b"+OK\r\n"
NULL = b"$-1\r\n"


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


def encode(s: list[bytearray], array_mode: bool = False) -> bytearray:
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
    res.append(b"")
    return b"\r\n".join(res)


def read_rdb(dir: str, dbfile_name: str) -> Tuple[dict, dict]:
    file = f"{dir}/{dbfile_name}"
    m = dict()
    expiry = dict()

    with open(file, "rb") as f:
        # ignore head and metadata session
        # just find the FC and FD

        """e.g:  b'\xfe\x00\xfb\x01\x00\x00\x05mykey\x05myval' """

        data = f.read(1)[0]
        while data != 0xFE:
            data = f.read(1)[0]

        # here use unpack bytes, so each value is a u8
        index, table_type, size, expires_size = f.read(4)
        # print(index, table_type, size, expires_size)
        assert table_type == 0xFB

        # handle normal dict
        for _ in range(size):
            value_type = f.read(1)[0]
            assert value_type == 0  # only string
            n = f.read(1)[0]
            key = f.read(n)
            n = f.read(1)[0]
            value = f.read(n)
            print(f"insert {key} {value}")
            m[key] = encode([value])

        # handle expiry table
        for _ in range(expires_size):
            timestamp_type = f.read(1)[0]
            if timestamp_type == 0xFC:
                t = int.from_bytes(f.read(8), "little")
            elif timestamp_type == 0xFD:
                t = int.from_bytes(f.read(4), "little")
            value_type = f.read(1)[0]
            assert value_type == 0
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
