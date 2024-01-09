import asyncio
import random
import string

import httpx


async def send_post_request():
    url = "http://127.0.0.1:8000/api/edit"

    while True:
        document_name = "".join(random.choice(string.ascii_lowercase) for _ in range(2))
        data = {"document": document_name}

        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data)

        print(f"Sent POST request with document name: {document_name}")

        # Sleep for 100 milliseconds
        await asyncio.sleep(0.01)


if __name__ == "__main__":
    asyncio.run(send_post_request())
