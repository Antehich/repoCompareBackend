import json
import os.path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Tuple, List

import createrepo_c

from .redis_controller import is_cached, load_cached, cache
from .utils import get_unique_key_for_strings_list, init, save_to_json, get_primary_checksum


def get_repodata_by_repo_url(repo_url: str) -> Dict:
    checksum = get_primary_checksum(repo_url)
    if os.path.exists(f'repodata/{checksum}.json'):
        file = open(f'repodata/{checksum}.json')
        data = json.load(file)
        return data
    repodata = createrepo_c.Metadata()
    repodata.locate_and_load_xml(repo_url)
    data = {key:
                {'name': repodata.get(key).name, 'epoch': repodata.get(key).epoch,
                 'version': repodata.get(key).version,
                 'release': repodata.get(key).release, 'arch': repodata.get(key).arch,
                 'nevra': repodata.get(key).nevra(), }
            for key in repodata.keys()}
    save_to_json(f'repodata/{checksum}.json', data)
    return data


def merge_repo(links) -> Dict:
    with ProcessPoolExecutor() as executor:
        future_to_url = {executor.submit(get_repodata_by_repo_url, link): link for link in links}
        accumulator = {}
        for future in as_completed(future_to_url):
            data = future.result()
            accumulator.update(data)
    return accumulator


def parse_merged_repos(merged_packages: Dict) -> Tuple[Dict, Dict, Dict]:
    name_dict = {}
    nevra_dict = {}
    package_data_dict = {}

    for package in merged_packages.values():
        name = package['name']
        epoch = package['epoch']
        version = package['version']
        release = package['release']
        arch = package['arch']
        nevra = package['nevra']
        name_dict[name] = ''
        nevra_dict[nevra] = {'name': name, 'epoch': epoch, 'version': version, 'release': release, 'arch': arch}
        name_arch = name + '.' + arch
        if name_arch in package_data_dict:
            if package_data_dict[name_arch]['epoch'] > epoch:
                continue
            if package_data_dict[name_arch]['version'] > version:
                continue
            if package_data_dict[name_arch]['release'] > release:
                continue
        package_data_dict[name_arch] = {'epoch': epoch, 'version': version, 'release': release}
    return name_dict, package_data_dict, nevra_dict


def get_unique_packages_by_name(first_dict: Dict, second_dict: Dict) -> Tuple[List, List]:
    unique_first = []
    keys = first_dict.copy().keys()
    for key in keys:
        if key in second_dict:
            first_dict.pop(key)
            second_dict.pop(key)
        else:
            unique_first.append(key)
    unique_second = list(second_dict.keys())
    unique_first.sort()
    unique_second.sort()
    return unique_first, unique_second


def get_unique_packages_by_nevra(first_dict: Dict, second_dict: Dict) -> Tuple[Dict, Dict]:
    for key in first_dict.copy().keys():
        if key in second_dict:
            first_dict.pop(key)
            second_dict.pop(key)
    return dict(sorted(first_dict.items())), dict(sorted(second_dict.items()))


def get_newest_namesake_packages(first_dict: Dict, second_dict: Dict):
    out = []
    for key in first_dict.keys():
        if key in second_dict:
            first = first_dict[key]['epoch'] + ':' + first_dict[key]['version'] + '.' + first_dict[key]['release']
            second = second_dict[key]['epoch'] + ':' + second_dict[key]['version'] + '.' + second_dict[key]['release']
            if first != second:
                out.append({key: [first, second]})
    return out


def compute_difference(alpha_links, beta_links):
    init()
    groups_hash = get_unique_key_for_strings_list(alpha_links + beta_links)
    primaries_checksums = []

    with ProcessPoolExecutor() as executor:
        future_to_url = [executor.submit(get_primary_checksum, link) for link in alpha_links + beta_links]
        for future in as_completed(future_to_url):
            data = future.result()
            primaries_checksums.append(data)

    primaries_groups_checksum = get_unique_key_for_strings_list(primaries_checksums)
    if is_cached(groups_hash):
        current = load_cached(groups_hash)
        if current == primaries_groups_checksum:
            return groups_hash, False
    with ProcessPoolExecutor() as executor:
        future_alpha = executor.submit(merge_repo, alpha_links)
        future_beta = executor.submit(merge_repo, beta_links)
        alpha = future_alpha.result()
        beta = future_beta.result()

    alpha_name_dict, alpha_package_data_dict, alpha_nerva_dict = parse_merged_repos(alpha)
    beta_name_dict, beta_package_data_dict, beta_nerva_dict = parse_merged_repos(beta)
    alpha_unique_by_name, beta_unique_by_name = get_unique_packages_by_name(alpha_name_dict, beta_name_dict)
    alpha_unique_by_nerva, beta_unique_by_nerva = get_unique_packages_by_nevra(alpha_nerva_dict, beta_nerva_dict)
    newest_namesake_packages = get_newest_namesake_packages(alpha_package_data_dict, beta_package_data_dict)
    os.makedirs(f'results/{groups_hash}', exist_ok=True)
    save_to_json(f'results/{groups_hash}/unique_by_name.json', [alpha_unique_by_name, beta_unique_by_name])
    save_to_json(f'results/{groups_hash}/unique_by_nerva.json', [alpha_unique_by_nerva, beta_unique_by_nerva])
    save_to_json(f'results/{groups_hash}/newest_namesake_packages.json', newest_namesake_packages)
    cache(groups_hash, primaries_groups_checksum)
    return groups_hash, True
