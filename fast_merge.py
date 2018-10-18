#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from contextlib import contextmanager
from bson import ObjectId
import json
import os
import datetime
import subprocess
import pymongo

import git



@contextmanager
def enter_workdir(workdir):
    old_cwd = os.getcwd()
    try:
        os.chdir(workdir)
        yield
    finally:
        os.chdir(old_cwd)


class FastMerge(object):
    # 数据库client缓存
    _client_cache = {}

    def __init__(self, code_workdirs=None, data_workdirs=[]):
        # 代码合并工作目录的默认值
        if code_workdirs is None:
            code_workdirs = []
        self.code_workdirs = code_workdirs
        self.data_workdirs = data_workdirs
    
    def code_merge(self, from_branches, to_branch):
        """将代码合并
        from_branches: 需要合并的分支名列表，list, [branch1, branch2, ...]
        to_branch: 合并到的目标分支名, str
        """
        for code_workdir in self.code_workdirs:
            for from_branch in from_branches:
                self._code_merge(code_workdir, from_branch, to_branch)
    
    def _code_merge(self, code_workdir, from_branch, to_branch):
        code_repo = git.Repo(code_workdir)
        if not code_repo.is_dirty() and not code_repo.untracked_files:
            self.git_merge(code_repo, from_branch, to_branch)
        else:
            raise Exception("{} has modified files or untracked files".format(code_workdir))

    def git_merge(self, repo, from_branch, to_branch):
        self.git_fetch(repo)
        pass

    def git_fetch(self, repo):
        repo.fetch()
        

            
    def data_merge(self, from_hosts, to_host, db_name, page_ids):
        """page_setup数据合并到一个实例中
        from_hosts: list, [(ip, port), (ip, port)]
        to_host: tuple, (ip, port)
        db_name: str
        page_ids: ["pageid1,pageid11", "pageid2", "pageid3, pageid33"]
        """
        page_setup_datas = []
        page_id_index = 0
        for from_host in from_hosts:
            _page_ids = page_ids[page_id_index].split(",")
            page_setup_data = self.get_merginData_pageSetup(from_host, db_name, _page_ids)
            for _data in page_setup_data:
                page_setup_datas.append(_data)
            page_id_index += 1
        self.insert_pageSetup_data(to_host, db_name, page_setup_datas)

    def get_merginData_pageSetup(self, from_host, db_name, page_ids):
        if from_host in self._client_cache:
            client = self._client_cache[from_host]
        else:
            client = pymongo.MongoClient(from_host[0], from_host[1])
            self._client_cache[from_host] = client
        db = client[db_name]
        page_ids = [ObjectId(p_id) for p_id in page_ids]
        cond = {}
        cond["_id"] = {"$in": page_ids}
        result = db.page_setup.find(cond)
        return result

    def insert_pageSetup_data(self, to_host, db_name, page_setup_datas):
        client = pymongo.MongoClient(to_host[0], to_host[1])
        db = client[db_name]
        try:
            db.page_setup.save(page_setup_datas)
        except Exception as e:
            raise e
        

        
        
    
