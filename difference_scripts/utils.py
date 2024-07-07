import hashlib
import json
import os
import xml.etree.ElementTree as ET

import requests


def _str_to_bitstring(s):
    return ''.join(format(ord(c), '08b') for c in s)


def _add_bitstrings(*bitstrings):
    max_len = max(len(b) for b in bitstrings)
    bitstrings = [b.zfill(max_len) for b in bitstrings]
    result = int(bitstrings[0], 2)
    for b in bitstrings[1:]:
        result += int(b, 2)
    return bin(result)[2:]


def _hash_bitstring(bitstring):
    byte_array = int(bitstring, 2).to_bytes((len(bitstring) + 7) // 8, byteorder='big')
    sha256_hash = hashlib.md5(byte_array).hexdigest()
    return sha256_hash


def get_unique_key_for_strings_list(strings):
    bitstrings = [_str_to_bitstring(s.strip()) for s in strings]
    summed_bitstr = _add_bitstrings(*bitstrings)
    result_hash = _hash_bitstring(summed_bitstr)
    return result_hash


def save_to_json(file_name: str, obj) -> None:
    with open(file_name, 'w') as out:
        json.dump(obj, out, indent=4)


def init():
    os.makedirs('repodata', exist_ok=True)
    os.makedirs('results', exist_ok=True)


def get_primary_checksum(url):
    url = url + "repodata/repomd.xml"
    xml_content = requests.get(url).content
    root = ET.fromstring(xml_content)
    namespaces = {
        'repo': 'http://linux.duke.edu/metadata/repo',
        'rpm': 'http://linux.duke.edu/metadata/rpm'
    }
    primary_data = root.find("repo:data[@type='primary']", namespaces)

    if primary_data is not None:
        checksum = primary_data.find('repo:checksum', namespaces).text
        return checksum
    else:
        return None
