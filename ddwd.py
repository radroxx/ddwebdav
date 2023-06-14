import asyncio
import sys
import hashlib
import gzip


import click
import aiodav


BASEDIR = "/ddwd/"

options = {
    "host": "http://127.0.0.1:8080",
}

##### libs functions
def webdav_create_client():

    return aiodav.Client(
        options.get("host"),
        login=options.get("login", ""),
        password=options.get("password", "")
    )


async def webdav_list(dir, client):
    return await client.list(BASEDIR + dir)


async def storage_get_buckets(client):

    buckets = await webdav_list("", client)
    result = []
    for bucket in buckets:
        result.append(bucket[:-1])
    return result


async def storage_bucket_get_versions(bucket, client):
    result = []
    not_completed_version = None
    versions = await webdav_list(bucket, client)
    for version in versions:
        if ".tmp" in version:
            not_completed_version = int(version[:-5])
        else:
            result.append(int(version[:-1]))

    return result, not_completed_version


async def storage_bucket_get_map(bucket, versions, target_version, client):

    versions.sort()

    mapa = {}

    for version in versions:
        if version > target_version:
            continue
        files = await webdav_list(bucket + "/" + str(version), client)
        for file in files:
            if file[-4:] == ".tmp":
                continue

            file_data = file.split("_")
            chunk_begin = int(file_data[0], 16)

            mapa[chunk_begin] = {
                "begin": chunk_begin,
                "end": int(file_data[1], 16),
                "sha256": file_data[2].split(".")[0],
                "version": version
            }

    try:
        if await client.is_directory(BASEDIR + bucket + "/" + str(target_version) + ".tmp"):
            files = await webdav_list(bucket + "/" + str(target_version) + ".tmp", client)
            for file in files:
                if file[-4:] == ".tmp":
                    continue

                file_data = file.split("_")
                chunk_begin = int(file_data[0], 16)

                mapa[chunk_begin] = {
                    "begin": chunk_begin,
                    "end": int(file_data[1], 16),
                    "sha256": file_data[2].split(".")[0],
                    "version": target_version
                }
    except aiodav.exceptions.RemoteResourceNotFound: pass

    return mapa


async def storage_bucket_begin_version(bucket, version, client):

    path = BASEDIR + bucket + "/" + str(version) + ".tmp/"
    await client.create_directory(path)


async def storage_bucket_commit_version(bucket, version, client):

    src_path = bucket + "/" + str(version) + ".tmp/"
    dst_path = bucket + "/" + str(version) + "/"

    count_files = 0
    files_list = await webdav_list(src_path, client)
    for file in files_list:
        if file[-4:] == ".tmp":
                continue
        count_files += 1

    if count_files > 0:
        await client.move(BASEDIR + src_path, BASEDIR + dst_path)
    else:
        await client.delete(BASEDIR + src_path)

##### libs functions


def find_empty_chunk(bucket_map):
    chunks_count = len(bucket_map)
    if chunks_count == 0:
        return None

    begin_points = set(bucket_map.keys())
    last_chunk = max(begin_points)

    begin_empty_pos = None
    for i in begin_points:

        chunks_count -=1 
        if chunks_count == 0:
            break

        if bucket_map[i]["end"] not in bucket_map:
            begin_empty_pos = bucket_map[i]["end"]
            break
    
    if begin_empty_pos is not None:
        end_empty_pos = max(begin_points)
        if begin_empty_pos == bucket_map[end_empty_pos]["end"]:
            return None
        for i in begin_points:
            if begin_empty_pos < i and i < end_empty_pos:
                end_empty_pos = i
        return (begin_empty_pos, end_empty_pos)

    return None


async def worker_upload(queue, client):

    while chunk := await queue.get():

        bucket = chunk[0]
        current_version = chunk[1]
        current_chunk = chunk[2]

        hash = hashlib.sha256()
        hash.update(current_chunk["data"])

        if hash.hexdigest() == current_chunk["sha256"]:
            continue

        file_path_cv  = BASEDIR + bucket + "/" + str(current_version) + ".tmp/"
        file_path_cv += "%0.12x_%0.12x_"%(current_chunk["begin"], current_chunk["end"])

        if current_chunk["version"] == current_version and len(current_chunk["sha256"]) == 65:
            # delete old file
            delete_file_name = file_path_cv + current_chunk["sha256"] + ".bin"
            await client.delete(delete_file_name)

        file_name = file_path_cv + hash.hexdigest() + ".bin"

        compressed_data = gzip.compress(current_chunk["data"])

        retry_count = 5
        while retry_count > 0:
            retry_count -= 1
            try:
                await client.upload_to(file_name + ".tmp", compressed_data)
                await client.move(file_name + ".tmp", file_name)
                retry_count = 0
            except aiodav.exceptions.MethodNotSupported: pass
            except aiodav.exceptions.ResponseErrorCode: pass

        print("uploaded", file_name)


async def worker_read_data(bucket, bucket_map, current_version, chunck_size, queue):

    current_pos = 0

    current_chunk = bucket_map[current_pos]
    del bucket_map[current_pos]

    while chunk_data := sys.stdin.buffer.read(current_chunk["end"]-current_pos):

        current_pos += len(chunk_data)

        if "data" not in current_chunk:
            current_chunk["data"] = b''

        if current_chunk["sha256"] != "zero":
            current_chunk["data"] += chunk_data

        if current_pos >= current_chunk["end"]:

            await queue.put(item=(bucket, current_version, current_chunk))

            if current_pos in bucket_map:
                current_chunk = bucket_map[current_pos]
                del bucket_map[current_pos]
            else:
                current_chunk = {
                    "begin": current_pos,
                    "end": current_pos + chunck_size - current_pos%chunck_size,
                    "sha256": "",
                    "version": current_version
                }

    await queue.put(item=(bucket, current_version, current_chunk))

    for _ in range(queue.maxsize):
        await queue.put(item=None)
    return 0


async def async_write(name, parallels, chunck_size):

    async with webdav_create_client() as client:

        versions, current_version = await storage_bucket_get_versions(name, client)

        if current_version is None:
            current_version = max(versions) + 1

        await storage_bucket_begin_version(name, current_version, client)

        bucket_map = await storage_bucket_get_map(name, versions, current_version, client)

        while empty_chunk := find_empty_chunk(bucket_map):
            chunk = {
                "begin": empty_chunk[0],
                "end": empty_chunk[1],
                "sha256": "",
                "version": current_version
            }
            if chunk["end"] - chunk["begin"] > chunck_size:
                chunk["end"] = chunk["begin"] + chunck_size - chunk["begin"]%chunck_size
            bucket_map[chunk["begin"]] = chunk
        
        if len(bucket_map) == 0:
            bucket_map[0] = {
                "begin": 0,
                "end": chunck_size,
                "sha256": "",
                "version": current_version
            }

        queue = asyncio.Queue(maxsize=parallels)

        tasks = []
        tasks.append(worker_read_data(name, bucket_map, current_version, chunck_size, queue))
        for _ in range(queue.maxsize):
            tasks.append(worker_upload(queue, client))
        await asyncio.gather(*tasks)

        await storage_bucket_commit_version(name, current_version, client)
        

async def async_read(name, version = None):
    async with webdav_create_client() as client:

        versions, not_completed_version = await storage_bucket_get_versions(name, client)

        if version is None:
            version = max(versions)

        bucket_map = await storage_bucket_get_map(name, versions, version, client)

        curent_chunk = bucket_map[0]
        
        while len(bucket_map) > 0:

            del bucket_map[curent_chunk["begin"]]

            file_name  = BASEDIR + name + "/" + str(curent_chunk["version"]) + "/"
            file_name += "%0.12x_%0.12x_%s.bin"%(curent_chunk["begin"], curent_chunk["end"], curent_chunk["sha256"])

            file_data = b''
            file_download_iterator = await client.download_iter(file_name)
            async for chunk in file_download_iterator:
                file_data += chunk
                
            decompress_data = gzip.decompress(file_data)
            file_data = b''
                
            sys.stdout.buffer.write(decompress_data)

            if curent_chunk["end"] in bucket_map:
                curent_chunk = bucket_map[curent_chunk["end"]]
            else:
                break


@click.group()
def cmd():
    pass


@cmd.command()
def list():
    """Show buckets"""
    async def async_list():
        async with webdav_create_client() as client:
            return await storage_get_buckets(client)
    buckets = asyncio.run(async_list()) 
    for bucket in buckets:
        print(bucket)
    

@cmd.command()
@click.option("--name", "-n", required=True, help="Bucket name")
@click.option("--version", "-v", default=None, help="Version")
def read(name, version):
    """Read bucket"""
    asyncio.run(async_read(name, version))


@cmd.command()
@click.option("--name", "-n", required=True, help="Bucket name")
@click.option("--parallels", "-p", default=1, help="Count parallels upload worker")
@click.option("--chunck-size", "-s", "chunck_size", default=16, help="Chunk size megabites")
def write(name, parallels, chunck_size):
    """Write bucket"""
    asyncio.run(async_write(name, parallels, 1048576 * chunck_size))

if __name__ == "__main__":
    cmd()
