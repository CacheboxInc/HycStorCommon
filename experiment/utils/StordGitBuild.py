#!/usr/bin/python3

import sys
import datetime
import shlex
import subprocess
import os
import re

program_name = sys.argv[0]
branch = sys.argv[1:]

def Log(cmd, rc, out, err):
    print(cmd, rc, out, err)

def RunCommand(directory, cmd):
    out_file = "/tmp/cmd.out"
    err_file = "/tmp/cmd.err"

    args = shlex.split(cmd)
    out_fh = open(out_file, "w")
    err_fh = open(err_file, "w")
    process = subprocess.Popen(args, cwd=directory, stdout=out_fh, stderr=err_fh)
    process.wait()
    with open(out_file) as fh:
        o = fh.readlines()
    with open(err_file) as fh:
        e = fh.readlines()
    rc = process.returncode
    print("cwd = ", directory)
    Log(cmd, rc, o, e)
    if rc:
        raise Exception("Executing command ('%s') failed with error = %d" % (cmd, rc))
    return (rc, o, e)

'''
def RunCommand(cmd):
    args = shlex.split(cmd)
    process = subprocess.Popen(agrs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    o = process.stdout
    e = process.stderr
    rc = process.returncode
    Log(cmd, rc, o, e)
    return (rc, o, e)
'''

class Git:
    def __init__(self, directory, repo, branch):
        self._branch = branch
        self._dir = directory
        self._repo = repo
        self._repo_url = "git@github.com:CacheboxInc/%s.git" % self._repo

    def ParentDir(self):
        return self._dir + "/"

    def RepoDir(self):
        return self.ParentDir() + self._repo + "/"

    def RepoUrl(self):
        return self._repo_url

    def UpdateRemote(self):
        cmd = "git remote update"
        return RunCommand(self.RepoDir(), cmd)

    def GetHeadCommitHash(self, branch):
        cmd = "git rev-parse %s" % branch
        (rc, o, e) = RunCommand(self.RepoDir(), cmd)
        assert(len(o) == 1)
        return o[0]

    def HasNewCommit(self):
        remote_branch = "origin/%s" % self._branch

        self.UpdateRemote()
        local = self.GetHeadCommitHash(self._branch)
        remote = self.GetHeadCommitHash(remote_branch)
        (rc, base, e) = RunCommand(self.RepoDir(), "git merge-base %s %s" % (self._branch, remote_branch))

        if local == remote:
            # already up-to-date
            return False
        elif local == base:
            # need a pull
            return True
        elif remote == base:
            # we need a push from here
            return False
        else:
            # remote and local have diverged
            assert(False)
            return False

    def Pull(self):
        cmd = "git pull origin %s" % self._branch
        RunCommand(self.RepoDir(), cmd)
        self.PullSubmodules()

    def PullSubmodules(self):
        cmd = "git submodule init"
        RunCommand(self.RepoDir(), cmd)

        cmd = "git submodule update"
        RunCommand(self.RepoDir(), cmd)

    def Clone(self):
        if not self.IsCloned():
            cmd = "git clone %s" % self._repo_url
            RunCommand(self.ParentDir(), cmd)
        else:
            self.Pull()
        self.PullSubmodules()

    def GetCurrentBranch(self):
        try:
            cmd = "git branch"
            (rc, output, e) = RunCommand(self.RepoDir(), cmd)
            expr = re.compile("\*\s+(\w+)")
            for line in output:
                print(line)
                match = expr.match(line)
                if not match:
                    continue
                return match.group(1)
            assert(0)
        except Exception as e:
            print("Not a git repository")
            raise e

    def IsCloned(self):
        rc = os.path.isdir(self.RepoDir())
        if not rc:
            print("Directory not present %")
            return False
        try:
            b = self.GetCurrentBranch()
            assert(len(b) > 0)
            return True
        except:
            print("failed")
            return False

    def CheckoutBranch(self):
        cmd = "git checkout %s" % (self._branch)
        RunCommand(self.RepoDir(), cmd)

class CMakeBuildSystem:
    def __init__(self, build_type, cmake_flags, directory, commit_hash="", cpus=1):
        self._dir = directory
        self._commit = commit_hash
        self._build_type = build_type
        self._cpus = cpus
        self._cmake_flags = cmake_flags
        self.InitializeBuildDirectory()
        self.WriteCommitHash()

    def WriteCommitHash(self):
        if len(self._commit) <= 0:
            return
        with open("%s/commit" % self._dir, "w") as fh:
            fh.write(self._commit)
            fh.flush()

    def InitializeBuildDirectory(self):
        try:
            os.mkdir(self._dir)
        except FileExistsError as e:
            pass
        except Exception as e:
            print("Failed to create directory %s" % (self._dir))
            raise e

    def CMake(self):
        cmd = "cmake %s -DCMAKE_BUILD_TYPE=%s .." % (self._cmake_flags, self._build_type)
        RunCommand(self._dir, cmd)

    def Compile(self):
        cmd = "make -j %d" % (self._cpus)
        RunCommand(self._dir, cmd)

    def Install(self):
        cmd = "sudo make install"
        RunCommand(self._dir, cmd)

class MakeBuildSystem:
    def __init__(self, directory, cpus = 1):
        self._dir = directory
        self._cpus = cpus

    def Make(self):
        cmd = "make -j %d" % (self._cpus)
        print("Running command ", cmd)
        RunCommand(self._dir, cmd)

    def Install(self):
        cmd = "sudo make install"
        RunCommand(self._dir, cmd)

class HaLibBuild:
    def __init__(self, directory):
        self._dir = directory

    def Compile(self):
        self.BuildThridParty()
        build_dir = self._dir + "/build/"
        build = CMakeBuildSystem("Release", cmake_flags="", directory=build_dir)
        build.CMake()
        build.Compile()
        build.Install()

    def BuildThridParty(self):
        d = self._dir + "/" + "third-party/"
        make = MakeBuildSystem(d, 1)
        make.Make()

class StordBuild:
    def __init__(self, directory, branch, build_type):
        self._branch = branch
        self._repo = "hyc-storage-layer"
        self._build_type = build_type
        self._git = Git(directory, self._repo, branch)
        self._parent_dir = directory

    def RepoDir(self):
        return self._git.RepoDir()

    def Clone(self):
        self._git.Clone()
        self._git.CheckoutBranch()

    def Build(self, build_dir_name):
        path = self.RepoDir() + build_dir_name + "/"
        commit = self._git.GetHeadCommitHash(self._branch)
        self.BuildHaLib()
        build = CMakeBuildSystem(self._build_type, cmake_flags="-DUSE_NEP=OFF", directory=path, commit_hash=commit, cpus=2)
        build.CMake()
        build.Compile()

    def BuildHaLib(self):
        path = self.RepoDir() + "/thirdparty/ha-lib/"
        build = HaLibBuild(path)
        build.Compile()

class HycCommonBuild:
    def __init__(self, directory, branch, build_type):
        self._parent_dir = directory
        self._branch = branch
        self._build_type = build_type
        self._repo = "HycStorCommon"
        self._git = Git(directory, self._repo, branch)

    def RepoDir(self):
        return self._git.RepoDir();

    def Clone(self):
        self._git.Clone()
        self._git.CheckoutBranch()

    def Build(self, build_dir_name):
        path = self.RepoDir() + "/" + build_dir_name + "/"
        commit = self._git.GetHeadCommitHash(self._branch)
        build = CMakeBuildSystem(self._build_type, cmake_flags="", directory=path, commit_hash=commit, cpus=2)
        build.CMake()
        build.Compile()
        build.Install()

class TgtBuild:
    def __init__(self, directory, branch, build_type):
        self._branch = branch
        self._repo = "tgt"
        self._build_type = build_type
        self._git = Git(directory, self._repo, branch)
        self._parent_dir = directory
        self._hyc_common_build = HycCommonBuild(directory, branch, build_type)

    def Clone(self):
        self._hyc_common_build.Clone()
        self._git.Clone()

    def RepoDir(self):
        return self._git.RepoDir()

    def Build(self, build_dir_name):
        path = self.RepoDir() + "/" + build_dir_name + "/"
        self._hyc_common_build.Build(build_dir_name)
        self.BuildHaLib()
        build = MakeBuildSystem(self.RepoDir(), 1)
        build.Make()

    def BuildHaLib(self):
        path = self.RepoDir() + "/thirdparty/ha-lib/"
        build = HaLibBuild(path)
        build.Compile()

if __name__ == "__main__":
    stord = StordBuild("/home/prasad/", "master", "Release")
    build_dir_name = "build" + datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
    stord.Clone()
    stord.Build(build_dir_name)

    tgtd = TgtBuild("/home/prasad/", "master", "Release")
    tgtd.Clone()
    tgtd.Build(build_dir_name)
