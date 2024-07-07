import requests
import requests.exceptions

from repos.models import Repo, Group


def get_links(group_id):
    return list(Repo.objects.all().filter(group=group_id).values_list('link', flat=True))


def get_group_id(name):
    return Group.objects.get(name=name).id


def validate_repo_link(url):
    try:
        url = url.rstrip('/') + "/repodata/"
        result = requests.get(url)
        if result.status_code == 200:
            return True
        else:
            return False
    except requests.exceptions.MissingSchema:
        return False
