from models.database import create_db, drop_db
import asyncio
from parser import parsing_trading_on_file, get_data, save_data_to_db


async def main():
    await drop_db()
    await create_db()


if __name__ == '__main__':
    asyncio.run(main())
