import pandas as pd 
import requests 
pd.set_option('display.max_columns', None)

df = pd.DataFrame(requests.get('https://api.artic.edu/api/v1/artworks?limit=100&q=dogs').json()['data'])


import aiohttp
import asyncio
import nest_asyncio

nest_asyncio.apply()

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)
urls = [x for n,x in df['api_link'].items()]   

# Now this should work without raising the RuntimeError
results = asyncio.run(main(urls))

