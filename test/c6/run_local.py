#!/usr/bin/env python3
"""Local end-to-end run of c6bb.py against a scratch OpenList (spec 7.2).

Never touches port 5244, 115 or any remote host. Usage (Windows or Linux):

    python run_local.py --bin PATH_TO_OPENLIST --work DIR --src DATASET_TREE [--port 5261 --vfs-port 5262]

Pass 1: OpenList /dav. manifest -> upload -> wait-drained -> verify --all -> mutate ->
        verify --all -> burst + kill -9 -> restart -> verify -> kill, delete the cfs state
        dir -> restart -> verify --all -> drive-check.
Pass 2: the VFS WebDAV of `cfs serve webdav`: upload + verify + mutate + verify.
"""
import argparse
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
C6 = os.path.join(HERE, "c6bb.py")


def step(msg):
    print(f"\n=== {msg}", flush=True)


def c6(*args):
    t0 = time.time()
    r = subprocess.run([sys.executable, C6, *map(str, args)])
    print(f"--- c6bb {args[0]}: exit {r.returncode} in {time.time() - t0:.1f}s", flush=True)
    if r.returncode != 0:
        raise SystemExit(f"FAILED: c6bb {' '.join(map(str, args[:1]))}")


def api(port, method, path, body, token=""):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", "Authorization": token})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


class Env:
    def __init__(self, binary, root, port):
        self.bin, self.root, self.port = binary, root, port
        self.data = os.path.join(root, "op-data")
        self.stats = os.path.join(root, "stats.json")
        self.under = os.path.join(root, "under")
        self.pw = secrets.token_hex(8)
        self.dav_auth = os.path.join(root, "dav.auth")
        self.admin_auth = os.path.join(root, "admin.auth")
        self.proc = None
        self.logf = None
        if port == 5244:
            raise SystemExit("refusing port 5244")

    def prepare(self):
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.under)
        os.makedirs(self.data)
        subprocess.run([self.bin, "admin", "set", self.pw, "--data", self.data], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cfg = os.path.join(self.data, "config.json")
        s = open(cfg, encoding="utf-8").read()
        s = re.sub(r'"http_port":\s*\d+', f'"http_port": {self.port}', s)
        s = re.sub(r'"address":\s*"[^"]*"', '"address": "127.0.0.1"', s)
        open(cfg, "w", encoding="utf-8").write(s)
        for f, u in ((self.dav_auth, "cfsdav"), (self.admin_auth, "admin")):
            with open(f, "w") as fh:
                fh.write(f"{u}:{self.pw}\n")

    def start(self, *args):
        args = args or ("server",)
        self.logf = open(os.path.join(self.root, "openlist.log"), "ab")
        env = dict(os.environ, CFS_STATS_FILE=self.stats)
        self.proc = subprocess.Popen([self.bin, *args, "--data", self.data], stdout=self.logf, stderr=self.logf, env=env)
        deadline = time.time() + 120
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise SystemExit("openlist exited early, see openlist.log in the work dir")
            try:
                self.token = api(self.port, "POST", "/api/auth/login", {"username": "admin", "password": self.pw})["data"]["token"]
                break
            except Exception:
                time.sleep(0.3)
        else:
            raise SystemExit("openlist did not come up")
        # The storage init finishes after login works; wait for /cfs to answer.
        time.sleep(1)

    def kill(self):
        """kill -9 equivalent, by PID."""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        if self.logf:
            self.logf.close()

    def stop(self):
        self.kill()

    def create_storages(self):
        def add(mount, driver, addition):
            out = api(self.port, "POST", "/api/admin/storage/create", {
                "mount_path": mount, "driver": driver, "order": 0, "cache_expiration": 0,
                "addition": json.dumps(addition), "webdav_policy": "native_proxy"}, self.token)
            assert out["code"] == 200, out
            sid = out["data"]["id"]
            for _ in range(300):
                g = api(self.port, "GET", f"/api/admin/storage/get?id={sid}", None, self.token)["data"]
                if g.get("status") == "work":
                    return
                time.sleep(0.2)
            raise SystemExit(f"storage {mount} not ready")
        add("/under", "Local", {"root_folder_path": self.under})
        add("/cfs", "Cfs", {"remote_path": "/under/vol", "root_folder_path": "/", "seal_quiet": "2s"})
        out = api(self.port, "POST", "/api/admin/user/create", {
            "username": "cfsdav", "password": self.pw, "base_path": "/", "role": 0, "permission": 0x73FF}, self.token)
        assert out["code"] == 200, out["message"]


def make_dataset(src_tree, dest):
    step("dataset")
    shutil.rmtree(dest, ignore_errors=True)
    shutil.copytree(src_tree, dest, ignore=shutil.ignore_patterns(".git", "NUL"), symlinks=False)
    for i in (1, 2):
        with open(os.path.join(dest, f"c6-random-{i}.bin"), "wb") as f:
            f.write(os.urandom(20 * 1024 * 1024))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--src", required=True, help="tree to use as dataset (.git is skipped)")
    ap.add_argument("--port", type=int, default=5261)
    ap.add_argument("--vfs-port", type=int, default=5262)
    ap.add_argument("--pass", dest="which", choices=("1", "2", "both"), default="both")
    a = ap.parse_args()
    work = os.path.abspath(a.work)
    os.makedirs(work, exist_ok=True)
    ds = os.path.join(work, "dataset")
    man = os.path.join(work, "m1.json")
    if not os.path.isdir(ds):
        make_dataset(a.src, ds)
    step("manifest")
    c6("manifest", "--src", ds, "--out", man)

    if a.which in ("1", "both"):
        e = Env(a.bin, os.path.join(work, "p1"), a.port)
        e.prepare()
        e.start()
        e.create_storages()
        dav = f"http://127.0.0.1:{a.port}/dav/cfs"
        try:
            step("pass 1: upload through /dav")
            c6("upload", "--src", ds, "--dav", dav, "--auth", e.dav_auth, "--manifest", man, "--workers", 8)
            step("wait-drained")
            c6("wait-drained", "--stats", e.stats, "--timeout", 600)
            step("verify --all")
            c6("verify", "--dav", dav, "--auth", e.dav_auth, "--manifest", man, "--all")
            step("mutate")
            m2 = os.path.join(work, "m2.json")
            c6("mutate", "--dav", dav, "--auth", e.dav_auth, "--manifest", man, "--out", m2, "--seed", 7)
            step("verify --all after mutate")
            c6("verify", "--dav", dav, "--auth", e.dav_auth, "--manifest", m2, "--all")
            step("burst, then kill -9")
            m3 = os.path.join(work, "m3.json")
            c6("burst", "--dav", dav, "--auth", e.dav_auth, "--manifest", m2, "--out", m3, "--n", 300, "--prefix", "c6-burst/", "--seed", 9)
            e.kill()
            print("killed openlist (kill -9 equivalent)", flush=True)
            e.start()
            step("verify --all after restart")
            c6("verify", "--dav", dav, "--auth", e.dav_auth, "--manifest", m3, "--all")
            c6("wait-drained", "--stats", e.stats, "--timeout", 600)
            e.kill()
            shutil.rmtree(os.path.join(e.data, "cfs"))
            print("deleted the cfs state directory", flush=True)
            e.start()
            step("verify --all after losing the index")
            c6("verify", "--dav", dav, "--auth", e.dav_auth, "--manifest", m3, "--all")
            step("drive-check")
            c6("drive-check", "--api", f"http://127.0.0.1:{a.port}", "--auth-admin", e.admin_auth,
               "--remote", "/under/vol", "--manifest", m3)
        finally:
            e.stop()

    if a.which in ("2", "both"):
        e = Env(a.bin, os.path.join(work, "p2"), a.port)
        e.prepare()
        e.start()
        e.create_storages()
        e.kill()
        e.start("cfs", "serve", "webdav", "/cfs", "--addr", f"127.0.0.1:{a.vfs_port}", "--vfs-cache-mode", "writes")
        vfs = f"http://127.0.0.1:{a.vfs_port}"
        try:
            step("pass 2: upload through the VFS WebDAV")
            c6("upload", "--src", ds, "--dav", vfs, "--auth", e.dav_auth, "--manifest", man, "--workers", 8)
            c6("wait-drained", "--stats", e.stats, "--timeout", 600)
            c6("verify", "--dav", vfs, "--auth", e.dav_auth, "--manifest", man, "--all")
            m2 = os.path.join(work, "v2.json")
            c6("mutate", "--dav", vfs, "--auth", e.dav_auth, "--manifest", man, "--out", m2, "--seed", 7)
            c6("verify", "--dav", vfs, "--auth", e.dav_auth, "--manifest", m2, "--all")
        finally:
            e.stop()
    print("\nALL STEPS PASSED")


if __name__ == "__main__":
    main()
