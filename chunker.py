# Goal: Memcache can only store 1MB slabs, so if given a file that exceeds
#       that size, it'll cause some level of failure.
#       Add a wrapper library that extracts the logic needed to break up
#       large files and store them in Memcache on SETs, and find, concatenate,
#       and return them on GETs
#
# More detail: https://gist.github.com/jsherer/d9bf1c10feb7d6590fd3
import hashlib
from pymemcache.client.base import Client


client = Client(('localhost', 11211))


# s is bytes type
def set_file(name, s):
    """store a file in memcache by its name"""
    md5 = checksum_bytes(s)
    client.set(name, md5)

    # exceeds 1 MB
    if len(s) > 1000000:
        chunk_count = len(s) // 1000000

        # account for overflow e.g. size 5000001 needs 6 chunks
        if chunk_count * 1000000 < len(s):
            chunk_count += 1

        # split bytes into segments of 1MB == 1000000 bytes
        chunks = [s[i:i + 1000000] for i in range(0, chunk_count * 1000000, 1000000)]
        keys = []

        for idx, chunk in enumerate(chunks):
            key = f'{md5}:{idx}'
            keys.append(key)
            client.set(key, chunk)

        client.set(f'{name}:keys', keys)
    else:
        key = f'{md5}:1'
        client.set(f'{name}:keys', [key])
        client.set(key, s)


# returns bytes type
def get_file(name):
    """retrieve a file from memcache by its name"""
    md5 = client.get(name)

    keys = client.get(f'{md5}:keys')

    chunks = []

    for key in keys:
        chunks.append(client.get(key))

    # TODO: fix this for bytes
    data = b''.join(chunks)

    return data


def test_file():
    name = 'lorum_large.txt'

    infile_path = f'./fixtures/{name}'
    with open(infile_path, 'rb') as infile:
        data = infile.read()

    set_file(name, data)
    result = get_file(name)

    outfile_path = f'./fixtures/{name}.out'
    with open(outfile_path, 'wb') as outfile:
        outfile.write(result)

    print(checksum_file(infile_path))
    print(checksum_file(outfile_path))
    assert checksum_file(infile_path) == checksum_file(outfile_path)


def checksum_file(path):
    return hashlib.md5(open(path, 'rb').read()).hexdigest()


def checksum_bytes(b):
    return hashlib.md5(b).hexdigest()

if __name__ == '__main__':
    test_file()
