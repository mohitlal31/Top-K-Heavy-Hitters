import asyncio
import json
import time

import httpx


async def send_post_requests(data_points, url, wait_time=0.1):
    async with httpx.AsyncClient() as client:
        for data in data_points:
            response = await client.post(url, json=data)
            print(
                f"Sent POST request with document name: {data.get('document')}, Status Code: {response.status_code}"
            )

            # Wait for the specified time before sending the next request
            await asyncio.sleep(wait_time)


async def main():
    json_file_path = "data_file.json"
    api_url = "http://127.0.0.1:8000/api/edit"
    wait_time = 0.001  # in seconds (10 milliseconds)

    with open(json_file_path, "r") as file:
        data_points = [json.loads(line.strip()) for line in file]

    await send_post_requests(data_points, api_url, wait_time)


if __name__ == "__main__":
    asyncio.run(main())
