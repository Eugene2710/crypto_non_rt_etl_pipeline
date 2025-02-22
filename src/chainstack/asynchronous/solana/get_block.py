import json
import os
from typing import Any
import aiohttp
import asyncio
import dotenv
from retry import retry

from src.chainstack.exceptions.chainstack_client_error import ChainStackClientError

dotenv.load_dotenv()

async def get_block()