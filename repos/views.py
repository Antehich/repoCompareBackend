import json
from concurrent.futures import ProcessPoolExecutor, as_completed

from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from django.http import HttpResponse

import utils_scripts
from difference_scripts.difference import compute_difference
from difference_scripts.s3_controller import upload_result_into_storage
from .models import Repo, Group

COMPARE_TYPE = {'unique_by_name': 'Unique by name',
                'unique_by_nerva': 'Unique by nerva',
                'newest_namesake_packages': 'Newest namesake'}


def compute_difference_view(request):
    body = json.loads(request.body)
    alpha_id = body['alpha']
    beta_id = body['beta']
    try:
        alpha_repos_links = utils_scripts.get_links(alpha_id)
        beta_repos_links = utils_scripts.get_links(beta_id)
        groups_hash, need_to_update = compute_difference(alpha_repos_links, beta_repos_links)
        results = {}
        if need_to_update:
            with ProcessPoolExecutor() as executor:
                future_to_url = [
                    executor.submit(upload_result_into_storage, groups_hash, type, f'results/{groups_hash}/{type}.json')
                    for type in COMPARE_TYPE.keys()]
                for future in as_completed(future_to_url):
                    result, key = future.result()
                    results[COMPARE_TYPE[key]] = 'https://storage.yandexcloud.net/' + result
                return HttpResponse(json.dumps(results), status=200)
        for k, v in COMPARE_TYPE.items():
            results[v] = 'https://storage.yandexcloud.net/' + f'repo-compare-results/{groups_hash}_{k}.json'
        return HttpResponse(json.dumps(results), status=200)
    except ObjectDoesNotExist as e:
        print(e)
        return HttpResponse(e, status=409)
    except Exception as e:
        return HttpResponse(e, status=500)


def create_group_view(request):
    body = json.loads(request.body)
    name = body['name']
    repos = body['repos']
    for link in repos:
        link.rstrip('/') + '/'
    try:
        group = Group(name=name)
        group.save()
        for link in repos:
            repo = Repo(link=link, group=group)
            repo.save()
        return HttpResponse(json.dumps({'id': group.id}), status=200)
    except IntegrityError as e:
        return HttpResponse(e, status=409)
    except Exception as e:
        return HttpResponse(e, status=500)


def validate_repo_view(request):
    body = json.loads(request.body)
    result = utils_scripts.validate_repo_link(body['link'])
    code = 200 if result else 400
    return HttpResponse(status=code)
