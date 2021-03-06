#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from bson import ObjectId
import pymongo
import git
import json
import sys



USAGE = """
该脚本用于代码库的合并和不同数据库实例中page_setup表中数据的迁移
代码合并以及数据库page_setup数据迁移：
    命令:
        python fast_merge.py -a fast_merge --workdirs <workdir1> <workdir2> --from_branches dev,dev2 dev,dev2 --to_branch master --remote_name origin --from_hosts <from_host1> <from_host2> --to_host <to_host> --page_ids <page_ids1> <page_ids2> --db_name <db_name> 
    详细说明：
    1.用于代码合并的参数说明:
        --workdirs: 工作目录，支持多个传值 ,空格分隔, 
        --from_branches: 需要合并的源分支，支持多个传值，空格分隔；传值个数与工作目录一致，单个值示例：branch1或branch1,branch2，表示对应的工作目录下操作的分支
        --to_branch: 将要合并到的目标分支
        --remote_name: 远程库的名称，默认为origin
    2.用于数据迁移的参数说明:
        --from_hosts: 源数据库主机,支持多个传值,空格分开;eg: ip1:port1或ip:port ip2:port2
        --to_host: 目标数据库主机
        --db_name: 数据库名称
        --page_ids: 页面id, 支持多个传值，空格分隔； 传值个数与from_hosts一致，单个传值示例：page_id1或者page_id1,page_id2，表示对应源主机查找多个页面id

代码合并，提交:
    可以同时操作多个代码库，将多个分支合并到某一分支，并提交
    命令:
        python fast_merge.py -a fast_code_merge --workdirs <workdir1> <workdir2> --from_branches dev,dev2 dev,dev2 --to_branch master --remote_name origin
    示例:
        python fast_merge.py -a fast_code_merge --workdirs . --from_branches dev --to_branch master --remote_name origin
数据迁移(page_setup数据）：
    可以从多个数据库实例中迁移数据到某数据库实例中
    命令：
        python fast_merge.py -a fast_data_merge --from_hosts <from_host1> <from_host2> --to_host <to_host> --page_ids <page_ids1> <page_ids2> --db_name <db_name>
    示例:
        python fast_merge.py -a fast_data_merge --from_hosts 192.168.0.102:27098 192.168.0.235:27017 --to_host 192.168.0.203:27018 --page_ids 5b724cf89ff6eb2b4ff72352,5b724cf89ff6eb2b4ff72353 5b724cf89ff6eb2b4ff72355 --db_name gz_opinion

"""


class FastMerge(object):
    # 数据库client缓存
    _client_cache = {}

    def __init__(self, code_workdirs):
        # 代码合并工作目录的默认值
        self.code_repos = []
        for code_workdir in code_workdirs:
            code_repo = git.Repo(code_workdir)
            self.code_repos.append(code_repo)
    
    def fast_merge(self, from_branches, to_branch, from_hosts, to_host, db_name, page_ids, remote_name="origin"):
        """
        1.多个代码库，多个分支合并到目标分支，并且提交
        2.从多个分支对应数据库实例中,根据页面id导出页面关系数据，导入到目标分支对应的数据库实例中
        """
        self.bulk_merge_push(self.code_repos, from_branches, to_branch, remote_name)
        self.pageSetup_data_merge(from_hosts, to_host, db_name, page_ids)
    
    def fast_code_merge(self, from_branches, to_branch, remote_name="origin"):
        """
        多个代码库，多个分支合并到目标分支，并且提交
        """
        self.bulk_merge_push(self.code_repos, from_branches, to_branch, remote_name)

    def bulk_merge_push(self, code_repos, from_branches, to_branch, remote_name="origin"):
        """
        多个代码库，多个分支合并到目标分支，并且提交
        """
        self.bulk_code_merge(code_repos,from_branches, to_branch, remote_name)
        self.git_bulk_push(self.code_repos, to_branch, remote_name) 

    def bulk_code_merge(self, code_repos, from_branches, to_branch, remote_name="origin"):
        """将代码合并
        code_repos: git库列表
        from_branches: 需要合并的分支名列表，list, ["branch1,branch3", "branch2", ...]
        to_branch: 合并到的目标分支名, str
        """
        for code_repo, _from_branches in zip(code_repos, from_branches):
            _from_branches = _from_branches.split(",")
            for _from_branch in _from_branches:
                self.code_merge(code_repo, _from_branch, to_branch)
    
    def code_merge(self, code_repo, from_branch, to_branch, remote_name="origin"):
        if not code_repo.is_dirty() and not code_repo.untracked_files:
            self.merge(code_repo, from_branch, to_branch)
        else:
            raise Exception("{} has modified files or untracked files".format(code_repo))

    def merge(self, repo, from_branch, to_branch, remote_name="origin"):
        """
        合并from_branch到to_branch
        1.先切换到目标分支-to_branch
        2.获取远程仓库代码
        3.将from_branch的远程分支合并到to_branch分支 
        """
        self.git_checkout(repo, to_branch)
        self.git_fetch(repo)
        self.git_merge(repo, from_branch, to_branch)

    def git_merge(self, repo, from_branch, to_branch, remote_name="origin"):
        current_branch = repo.active_branch
        origin = repo.remotes[remote_name] 
        # double check条件
        if current_branch.name == to_branch:
            try:
                branch_from_remote = origin.refs[from_branch]
            except IndexError:
                branch_from_remote = repo.heads[from_branch]
            repo.git.merge(branch_from_remote.name)
            # 找到两个分支的合并基
            # merge_base = repo.merge_base(current_branch, branch_from_remote)
            # 合并操作
            # repo.index.merge_tree(branch_from_remote, base=merge_base)
            # 提交并提供指向两个父提交
            # repo.index.commit("merge {} into {} by script fast_merge.py".format(from_branch, to_branch), parent_commits=(current_branch.commit, branch_from_remote.commit))
            # 不可以少的一步：执行新提交的checkout
            # current_branch.checkout(force=True)
        else:
            raise Exception("git_merge: current branch <{}> is not to_branch<{}>".format(current_branch.name, to_branch))

    def git_bulk_push(self, repos, branch_name, remote_name="orgin"):
        for repo in repos:
            self.git_push(repo, branch_name, remote_name) 

    def git_push(self, repo, branch_name, remote_name="origin"):
        repo.git.push(remote_name, branch_name)

    def git_fetch(self, repo, remote_name="origin"):
        origin = repo.remotes[remote_name]
        origin.fetch()
        
    def git_checkout(self, repo, branch_name, remote_name="origin"):
        """切换分支
        1.如果当前分支已经是要切换的分支，pass, 否则step2
        2.切换时，注意一点，是否已经有分支的head指针，如果没有，需要创建分支的head指针并且设置远程跟踪分支
        """
        current_branch = repo.active_branch
        if current_branch.name != branch_name: 
            origin = repo.remotes[remote_name]
            if self._branch_in_heads(repo, branch_name): 
                repo.heads[branch_name].checkout()
            elif self._branch_in_remote(origin, branch_name):
                repo.create_head(branch_name, origin.refs[branch_name]).set_tracking_branch(origin.refs[branch_name]) 
                repo.heads[branch_name].checkout()
        
    def _branch_in_remote(self, origin, branch_name):
        r_remote_names = [remote_ref.name for remote_ref in origin.refs]
        flag = False
        if branch_name in r_remote_names:
            flag = True
        return flag
            
    def _branch_in_heads(self, repo, branch_name):
        r_head_names = [head.name for head in repo.heads]    
        flag = False
        if branch_name in r_head_names:
            flag = True
        return flag

    @classmethod
    def pageSetup_data_merge(cls, from_hosts, to_host, db_name, page_ids):
        """page_setup数据合并到一个实例中
        from_hosts: list, [(ip, port), (ip, port)]
        to_host: tuple, (ip, port)
        db_name: str
        page_ids: ["pageid1,pageid11", "pageid2", "pageid3, pageid33"]
        """
        print "migrating data, page_ids <{}> from {} to {}".format(page_ids, from_hosts, to_host)
        page_setup_datas = []
        for from_host, _page_ids in zip(from_hosts, page_ids):
            _page_ids = _page_ids.split(",")
            page_setup_data = cls.get_merginData_pageSetup(from_host, db_name, _page_ids)
            for _data in page_setup_data:
                page_setup_datas.append(_data)
        cls.insert_pageSetup_data(to_host, db_name, page_setup_datas)
        print "data has migrated from {} to {}".format(from_hosts, to_host)

    @classmethod
    def get_merginData_pageSetup(cls, from_host, db_name, page_ids):
        if from_host in cls._client_cache:
            client = cls._client_cache[from_host]
        else:
            from_host_ip, from_host_port = from_host.split(":")
            client = pymongo.MongoClient(from_host_ip, int(from_host_port))
            cls._client_cache[from_host] = client
        db = client[db_name]
        page_ids = [ObjectId(p_id) for p_id in page_ids]
        cond = {}
        cond["_id"] = {"$in": page_ids}
        result = db.page_setup.find(cond)
        return result
    
    @classmethod
    def insert_pageSetup_data(cls, to_host, db_name, page_setup_datas):
        if to_host in cls._client_cache:
            client = cls._client_cache[to_host]
        else:
            to_host_ip, to_host_port = to_host.split(":")
            client = pymongo.MongoClient(to_host_ip, int(to_host_port))
        db = client[db_name]
        try:
            for doc in page_setup_datas: 
                db.page_setup.save(doc)
        except Exception as e:
            print "insert_pageSetup_data: save {} failed".format(doc)
            raise e

        
def classify_workdirs(workdirs):
    """
    工作目录分类
    做一个保险
    """
    code_workdirs = []
    data_workdirs = []
    for workdir in workdirs:
        if workdir.endswith("data"):
            data_workdirs.append(workdir)
        else:
            code_workdirs.append(workdir)
    return code_workdirs, data_workdirs


## api global functioins
# merge codes and datas
def fast_merge(workdirs, from_branches, to_branch, from_hosts,
        to_host, db_name, page_ids, remote_name="origin"):
    code_workdirs, _ = classify_workdirs(workdirs)
    f_merge = FastMerge(code_workdirs)
    f_merge.fast_merge(from_branches, to_branch, from_hosts, to_host,
        db_name, page_ids, remote_name)


# merge codes
def fast_code_merge(workdirs, from_branches, to_branch, remote_name="origin"):
    code_workdirs, _ = classify_workdirs(workdirs)
    f_merge = FastMerge(code_workdirs)
    f_merge.fast_code_merge(from_branches, to_branch, remote_name)


# merge datas
def fast_data_merge(from_hosts, to_host, db_name, page_ids):
    FastMerge.pageSetup_data_merge(from_hosts, to_host, db_name, page_ids)


action_methods = {
    "fast_merge": (fast_merge, ("workdirs", "from_branches", "to_branch", "from_hosts", "to_host", "db_name", "page_ids", "remote_name")),
    "fast_code_merge": (fast_code_merge, ("workdirs", "from_branches", "to_branch", "remote_name")), 
    "fast_data_merge": (fast_data_merge, ("from_hosts", "to_host", "db_name", "page_ids")), 
}


def get_method_kwargs(action, args):
    try:
        method, fields = action_methods.get(action, (None, None))
    except:
        raise
    if method is None:
        print USAGE
        sys.exit()
    kwargs = {}
    for key in fields:
        kwargs[key] = args.__dict__[key]
    return method, kwargs
    

def main():
    import argparse
    parser = argparse.ArgumentParser("fast_merge.py")
    parser.add_argument("-a", "--action", required=True)
    parser.add_argument("--workdirs", nargs="*")
    parser.add_argument("--from_branches", nargs="*")
    parser.add_argument("--to_branch")
    parser.add_argument("--from_hosts", nargs="*")
    parser.add_argument("--to_host")
    parser.add_argument("--db_name")
    parser.add_argument("--page_ids", nargs="*")
    parser.add_argument("--remote_name", default="origin")
    args = parser.parse_args()
    action = args.action
    action_method, kwargs = get_method_kwargs(action, args)
    print kwargs
    action_method(**kwargs)
    

if __name__ == "__main__":
    main()

    
    

    

    
