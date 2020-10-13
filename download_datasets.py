from pathlib import Path
from typing import List
import httpx
import json
import asyncio

async def download_file(filename):
    async with httpx.AsyncClient() as client:
        response = await client.get(filename)
        return response.content


async def write_file(filename, content):
    Path(filename.split('/')[-1]).write_bytes(content)

async def get_files(filename):
    content = await(download_file(filename))
    await write_file(filename, content)

async def download(files: List[str]=None, include_resources: bool=False):
    """downloads the keys to download new datafiles. If None, download all datasets. 
    Optionally download resources
    """
    with open ('datasets.json') as fp:
        json_data = json.load(fp)
    
    queue = []

    for url in [x for x in json_data['datasets'].values()]:
        queue.append(get_files(url))
    await asyncio.wait(queue)

if __name__ == "__main__":
    asyncio.run(download())
