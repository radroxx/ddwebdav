import asyncio
import sys
import hashlib


import click
import aiodav


class options():
    host = "https://webdav.***"
    login = "******"
    password = "******"
    basedir = "/****/"


async def async_list():
    async with aiodav.Client(options.host, login=options.login, password=options.password) as client:
        list = await client.list(options.basedir)
        for bucket in list:
            print(bucket[:-1])


def filename_to_chunk(filename):
    obj = {}
    split_data = filename.split("_")
    obj["begin"] = int(split_data[0], 16)
    obj["end"] = int(split_data[1], 16)
    obj["sha256"] = split_data[2].split(".")[0]
    obj["is_zero"] = False
    if obj["sha256"] == "zero":
        obj["is_zero"] = True
    obj["original_filename"] = filename
    return obj


async def worker_upload(queue, bucket_name, client):

    uploaded = []
    while chunk := await queue.get():
        hash = hashlib.sha256()
        hash.update(chunk["data"])

        hash_str = hash.hexdigest()

        if chunk["sha256"] == hash_str:
            continue

        old_file_name = "%0.12x_%0.12x_%s.bin"%(
            chunk["begin"], chunk["end"], chunk["sha256"]
        )
        file_name = "%0.12x_%0.12x_%s.tmp"%(
            chunk["begin"], chunk["end"], hash_str
        )
        await client.upload_to(
            options.basedir + bucket_name + "/" + file_name,
            chunk["data"]
        )
        uploaded.append((file_name, chunk.get("original_filename", None)))
    return uploaded


async def worker_read_data(chunks_list, chunck_size, queue):

    current_pos = 0
    chunk_from_list = {"begin": -1}
    current_chunk = None

    while True:

        if current_chunk is not None:

            # Чтение данных
            data = sys.stdin.buffer.read(current_chunk["end"]-current_pos)
            current_pos += len(data)

            if len(data) == 0:
                break
            
            if "data" not in current_chunk:
                current_chunk["data"] = b''

            current_chunk["data"] += data

            if current_pos >= current_chunk["end"]:
                await queue.put(item=current_chunk)
                current_chunk = None
 
            continue

        # if current_chunk is None
        if current_pos > chunk_from_list["begin"]:
            if len(chunks_list) > 0:
                chunk_from_list = filename_to_chunk(chunks_list.pop())
            else:
                chunk_from_list["begin"] = sys.maxsize

        if current_pos == chunk_from_list["begin"]:
            current_chunk = chunk_from_list

        if current_pos < chunk_from_list["begin"]:
            current_chunk = {
                "begin": current_pos,
                "end": chunk_from_list["begin"],
                "sha256": "0",
                "is_zero": False
            }
            if current_chunk["end"] - current_chunk["begin"] > chunck_size:
                current_chunk["end"] = current_pos + chunck_size - current_pos%chunck_size
    
    for _ in range(queue.maxsize):
        await queue.put(item=None)
    
    return []


async def async_write(name, parallels, chunck_size):
    async with aiodav.Client(options.host, login=options.login, password=options.password) as client:

        chunks_list = await client.list(options.basedir + name)
        chunks_list.sort(reverse=True)

        queue = asyncio.Queue(maxsize=parallels)

        tasks = []
        tasks.append(worker_read_data(chunks_list, chunck_size, queue))
        for _ in range(queue.maxsize):
            tasks.append(worker_upload(queue, name, client))
        result = await asyncio.gather(*tasks)

        # commit tmp
        for lst in result:
            for upload in lst:
                if upload[1] is not None:
                    await client.delete( options.basedir + name + "/" + upload[1] )
                await client.move(
                    options.basedir + name + "/" + upload[0],
                    options.basedir + name + "/" + upload[0][:-3] + "bin"
                )


async def async_read(name):
    async with aiodav.Client(options.host, login=options.login, password=options.password) as client:
        chunks_list = await client.list(options.basedir + name)
        chunks_list.sort()
        for chunk in chunks_list:
            if chunk[:-3] == "tmp":
                continue
            await client.download_to(options.basedir + name + "/" + chunk, sys.stdout.buffer)


@click.group()
def cmd():
    pass


@cmd.command()
def list():
    """Show buckets"""
    asyncio.run(async_list())


@cmd.command()
@click.option("--name", "-n", help="Bucket name")
def read(name):
    """Read bucket"""
    asyncio.run(async_read(name))


@cmd.command()
@click.option("--name", "-n", required=True, help="Bucket name")
@click.option("--parallels", "-p", default=1, help="Count parallels upload worker")
@click.option("--chunck_size", default=16, help="Chunk size megabites")
def write(name, parallels, chunck_size):
    """Write bucket"""
    asyncio.run(async_write(name, parallels, 1048576 * chunck_size))


if __name__ == "__main__":
    cmd()
