import asyncio


async def tcp_client():
    reader, writer = await asyncio.open_connection("localhost", 6379)

    # 发送消息
    message = b"PING\r\n"
    print(f"Sending: {message}")
    writer.write(message)
    await writer.drain()  # 确保消息已经发送

    # 接收响应
    data = await reader.read(1024)
    print(f"Received: {data}")

    # 关闭连接
    writer.close()
    await writer.wait_closed()
    print("Connection closed.")


# 运行客户端
if __name__ == "__main__":
    for i in range(10):
        asyncio.run(tcp_client())
