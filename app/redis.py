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


def encode(s: list[bytearray]) -> bytearray:
    res = []
    if len(s) == 1:
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


if __name__ == "__main__":
    msg = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    s = decode(msg)
    rmsg = encode(s)
    assert msg == rmsg
