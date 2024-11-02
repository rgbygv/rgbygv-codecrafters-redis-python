import asyncio
from app.redis import encode


async def tcp_client():
    reader, writer = await asyncio.open_connection("localhost", 6379)

    # SET
    message = encode([b"CONFIG", b"GET", b"dir"])
    print(f"Sending: {message}")
    writer.write(message)
    await writer.drain()
    data = await reader.read(1024)
    print(f"Received: {data}")

    # GET
    message = encode([b"CONFIG", b"GET", b"dbfilename"])
    print(f"Sending: {message}")
    writer.write(message)
    await writer.drain()
    data = await reader.read(1024)
    print(f"Received: {data}")

    # Close
    writer.close()
    await writer.wait_closed()


# 运行客户端
if __name__ == "__main__":
    asyncio.run(tcp_client())
    # for i in range(10):
    #     asyncio.run(tcp_client())
