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


def set_file(name, s):
  """store a file in memcache by its name"""
  client.set(name, s)


def get_file(name):
  """retrieve a file from memcache by its name"""
  return client.get(name)


def test_file():
    name = 'lorum_small.txt'

    infile_path = f'./fixtures/{name}'
    with open(infile_path, 'r') as infile:
        data = infile.read()

    set_file(name, data)
    result = get_file(name)

    outfile_path = f'./fixtures/{name}.out'
    with open(outfile_path, 'w') as outfile:
        outfile.write(str(result))

    # assert checksum_file(infile_path) == checksum_file(outfile_path)
    print(checksum_file(infile_path))
    print(checksum_file(outfile_path))


def checksum_file(path):
    return hashlib.md5(open(path, 'rb').read()).hexdigest()


if __name__ == '__main__':
    test_file()
