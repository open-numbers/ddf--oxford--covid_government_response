import requests
import json
import os.path as osp

user = 'OxCGRT'
repo = 'covid-policy-tracker'
branch = 'master'
path = 'data/OxCGRT_latest.csv'
remote_version = None

curdir = osp.dirname(osp.abspath(__file__))
source_dir = osp.join(curdir,'..','source')
source_path = osp.join(source_dir, 'OxCGRT_latest.csv')
sha_path = osp.join(source_dir, 'sha.txt')

def get_latest():
    local_sha = get_datapackage_sha()
    git_sha = get_github_head_sha(user, repo, branch)

    if git_sha == local_sha:
        print('Current dataset source is already latest. Not fetching new source.')
        return
    
    print('Updating source...')

    url = f'https://raw.githubusercontent.com/{user}/{repo}/{git_sha}/{path}'

    r = requests.get(url)
    with open(source_path, '+w', newline='') as f:
        f.write(r.text)

    with open(sha_path, '+w') as f:
        f.write(git_sha)

def get_github_head_sha(user, repo, branch):
    url = f'https://api.github.com/repos/{user}/{repo}/branches/{branch}'
    r = requests.get(url)
    return r.json()["commit"]["sha"]

def get_datapackage_sha():
    # Opening JSON file 
    dp_path = osp.join(curdir, '..','..','datapackage.json')
    try: 
        f = open(dp_path)
        dp = json.load(f) 
        return dp['source']['sha']
    except (FileNotFoundError, KeyError):
        return None

if (__name__ == '__main__'):
    get_latest()