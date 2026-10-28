#!/usr/bin/env python3
"""c6bb: black-box verification of a cfs volume used through OpenList.

Python 3.12 standard library only. Everything goes over HTTP (WebDAV, the
OpenList admin API) plus the CFS_STATS_FILE the OpenList process writes; the
cfs state directory is never read. Any inconsistency makes the process exit
non-zero. Secrets (passwords, tokens) are never printed.

Subcommands: manifest, upload, verify, mutate, burst, wait-drained, drive-check.
Auth files hold one line: user:password.
"""
import argparse
import hashlib
import http.client
import json
import os
import posixpath
import random
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

MiB = 1 << 20
LARGE = 8 * MiB  # files of at least this size are stored 1:1, not packed
RETRY_DELAYS = (2, 5, 10)
CHUNK = 1 << 20


# ---------------------------------------------------------------- helpers

def log(msg):
    print(msg, flush=True)


def read_auth(path):
    with open(path, "r", encoding="utf-8") as f:
        line = f.readline().rstrip("\r\n")
    if ":" not in line:
        raise SystemExit("auth file must hold one line user:password")
    user, pw = line.split(":", 1)
    return user, pw


def human(n):
    n = float(n)
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or u == "TiB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.2f} {u}"
        n /= 1024


def pct(sorted_vals, p):
    """Nearest-rank percentile of an ascending list."""
    if not sorted_vals:
        return 0.0
    i = max(0, min(len(sorted_vals) - 1, int(-(-p * len(sorted_vals) // 100)) - 1))
    return sorted_vals[i]


def read_stats(src):
    """The cfs stats JSON from a file path or an http(s) URL; None when unreadable."""
    for _ in range(5):
        try:
            if src.startswith(("http://", "https://")):
                with urllib.request.urlopen(src, timeout=10) as r:
                    return json.loads(r.read())
            with open(src, "rb") as f:
                return json.loads(f.read())
        except (OSError, ValueError):
            time.sleep(0.3)  # the file is rewritten every 300 ms; a read can hit a half-written one
    return None


def stats_delta(before, after):
    calls = {}
    for k in set(before.get("Calls") or {}) | set(after.get("Calls") or {}):
        d = (after.get("Calls") or {}).get(k, 0) - (before.get("Calls") or {}).get(k, 0)
        if d:
            calls[k] = d
    return {"calls": dict(sorted(calls.items())), "total": after.get("Total", 0) - before.get("Total", 0),
            "bytes_read": after.get("BytesRead", 0) - before.get("BytesRead", 0)}


def class_of(size):
    return "big" if size >= LARGE else "small"


def class_report(items, wall):
    """items: [(size, ttfb, total)] of one size class."""
    n = len(items)
    nbytes = sum(s for s, _, _ in items)
    tt = sorted(t for _, t, _ in items)
    to = sorted(t for _, _, t in items)
    wall = max(wall, 1e-9)
    r = {"files": n, "bytes": nbytes, "wall_seconds": wall,
         "mb_per_s": nbytes / wall / 1e6, "files_per_s": n / wall}
    for name, v in (("ttfb", tt), ("total", to)):
        for p in (50, 95, 99):
            r[f"{name}_p{p}"] = pct(v, p)
        r[f"{name}_max"] = v[-1] if v else 0.0
    return r


def class_line(name, r):
    return (f"  {name:5} {r['files']} files, {human(r['bytes'])}, {r['wall_seconds']:.1f}s, "
            f"{r['mb_per_s']:.1f} MB/s, {r['files_per_s']:.1f} files/s; "
            f"ttfb p50/p95/p99/max {r['ttfb_p50'] * 1000:.0f}/{r['ttfb_p95'] * 1000:.0f}/{r['ttfb_p99'] * 1000:.0f}/{r['ttfb_max'] * 1000:.0f} ms; "
            f"total p50/p95/p99/max {r['total_p50'] * 1000:.0f}/{r['total_p95'] * 1000:.0f}/{r['total_p99'] * 1000:.0f}/{r['total_max'] * 1000:.0f} ms")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(CHUNK)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def load_manifest(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(path, m):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(m, f, sort_keys=True, indent=0)
    os.replace(tmp, path)


def write_report(path, rep):
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=1, sort_keys=True)


def show_list(title, items, limit=20):
    if not items:
        return
    log(f"  {title}: {len(items)}")
    for it in items[:limit]:
        log(f"    {it}")
    if len(items) > limit:
        log(f"    ... {len(items) - limit} more (see --report)")


def new_content(rng, n):
    return rng.randbytes(n) if n else b""


def sha_of(b):
    return hashlib.sha256(b).hexdigest()


def excluded(rel, excludes):
    for e in excludes:
        e = e.strip("/")
        if rel == e or rel.startswith(e + "/"):
            return True
    return False


# ---------------------------------------------------------------- WebDAV client

class DavError(Exception):
    pass


class Dav:
    """Small WebDAV client; one keep-alive connection per thread."""

    def __init__(self, base, auth_file, timeout=900):
        u = urllib.parse.urlparse(base)
        self.scheme, self.host = u.scheme, u.netloc
        self.prefix = u.path.rstrip("/")
        user, pw = read_auth(auth_file)
        import base64
        self._auth = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
        self.timeout = timeout
        self._tl = threading.local()

    def _conn(self):
        c = getattr(self._tl, "c", None)
        if c is None:
            cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
            c = cls(self.host, timeout=self.timeout)
            self._tl.c = c
        return c

    def _drop(self):
        c = getattr(self._tl, "c", None)
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
        self._tl.c = None

    def url_path(self, rel):
        rel = rel.strip("/")
        p = self.prefix + "/" + "/".join(urllib.parse.quote(s, safe="") for s in rel.split("/")) if rel else self.prefix + "/"
        return p

    def request(self, method, rel, body=None, size=None, headers=None, stream=None):
        """One attempt. Returns (status, body bytes) or feeds stream(chunk)."""
        h = {"Authorization": self._auth}
        if headers:
            h.update(headers)
        if body is not None:
            h["Content-Length"] = str(size if size is not None else len(body))
        elif method in ("PUT", "MKCOL", "MOVE", "DELETE", "PROPFIND"):
            h["Content-Length"] = "0"
        try:
            c = self._conn()
            c.request(method, self.url_path(rel), body=body, headers=h)
            r = c.getresponse()
            if stream is not None and r.status == 200:
                while True:
                    b = r.read1(CHUNK)
                    if not b:
                        break
                    stream(b)
                return r.status, b""
            data = r.read()
            if r.getheader("Connection", "").lower() == "close":
                self._drop()
            return r.status, data
        except (OSError, http.client.HTTPException) as e:
            self._drop()
            raise DavError(f"{method} {rel}: {type(e).__name__}: {e}")

    def retrying(self, what, fn, delays=RETRY_DELAYS):
        last = None
        for i in range(len(delays) + 1):
            try:
                return fn()
            except DavError as e:
                last = e
            if i < len(delays):
                time.sleep(delays[i])
        raise DavError(f"{what}: {last} (after {len(delays) + 1} attempts)")

    # --- operations (each raises DavError unless success)

    def put(self, rel, data=None, fileobj=None, size=None):
        def go():
            if fileobj is not None:
                fileobj.seek(0)
                st, body = self.request("PUT", rel, body=fileobj, size=size)
            else:
                st, body = self.request("PUT", rel, body=data, size=len(data))
            if st not in (200, 201, 204):
                self._drop()
                raise DavError(f"PUT {rel}: HTTP {st}")
            return st
        return self.retrying("PUT " + rel, go)

    def mkcol(self, rel):
        st, _ = self.request("MKCOL", rel)
        if st not in (200, 201, 405):
            raise DavError(f"MKCOL {rel}: HTTP {st}")

    def mkcol_all(self, rel, made):
        rel = rel.strip("/")
        if not rel or rel in made:
            return
        self.mkcol_all(posixpath.dirname(rel), made)
        self.retrying("MKCOL " + rel, lambda: self.mkcol(rel))
        made.add(rel)

    def delete(self, rel):
        def go():
            st, _ = self.request("DELETE", rel)
            if st not in (200, 204):
                raise DavError(f"DELETE {rel}: HTTP {st}")
        self.retrying("DELETE " + rel, go, delays=())

    def move(self, src, dst):
        def go():
            dest = self.scheme + "://" + self.host + self.url_path(dst)
            st, _ = self.request("MOVE", src, headers={"Destination": dest, "Overwrite": "T"})
            if st not in (200, 201, 204):
                raise DavError(f"MOVE {src} -> {dst}: HTTP {st}")
        self.retrying("MOVE " + src, go, delays=())

    def sum(self, rel, timing=None):
        """GET and return (sha256 hex, size); one attempt. timing, when given, gets
        "ttfb" (seconds to the first body byte; the whole time for an empty file)
        and "total"."""
        h = hashlib.sha256()
        n = [0]
        t0 = time.perf_counter()
        first = [None]

        def feed(b):
            if first[0] is None:
                first[0] = time.perf_counter() - t0
            h.update(b)
            n[0] += len(b)
        st, _ = self.request("GET", rel, stream=feed)
        total = time.perf_counter() - t0
        if st != 200:
            self._drop()
            raise DavError(f"GET {rel}: HTTP {st}")
        if timing is not None:
            timing["ttfb"] = first[0] if first[0] is not None else total
            timing["total"] = total
        return h.hexdigest(), n[0]

    def list(self, rel):
        """PROPFIND Depth:1. Returns [(name, is_dir, size)]."""
        def go():
            st, body = self.request("PROPFIND", rel, headers={"Depth": "1", "Content-Type": "application/xml"})
            if st != 207:
                raise DavError(f"PROPFIND {rel}: HTTP {st}")
            return body
        body = self.retrying("PROPFIND " + rel, go)
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            raise DavError(f"PROPFIND {rel}: bad XML: {e}")
        selfp = self.url_path(rel).rstrip("/")
        out = []
        for resp in root.iter("{DAV:}response"):
            href = resp.findtext("{DAV:}href") or ""
            p = urllib.parse.unquote(urllib.parse.urlparse(href).path).rstrip("/")
            if p == urllib.parse.unquote(selfp):
                continue
            pre = urllib.parse.unquote(selfp) + "/"
            if not p.startswith(pre):
                continue
            name = p[len(pre):]
            if "/" in name or not name:
                continue
            is_dir, size = False, 0
            for ps in resp.findall("{DAV:}propstat"):
                prop = ps.find("{DAV:}prop")
                if prop is None:
                    continue
                if prop.find("{DAV:}resourcetype/{DAV:}collection") is not None:
                    is_dir = True
                ln = prop.findtext("{DAV:}getcontentlength")
                if ln and ln.strip().isdigit():
                    size = int(ln)
            out.append((name, is_dir, size))
        return out

    def walk(self, workers=8):
        """All files below the root by Depth:1 recursion: {rel: size}, dir count."""
        files, dirs = {}, [0]
        level = [""]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while level:
                nxt = []
                for rel, items in zip(level, pool.map(self.list, level)):
                    for name, is_dir, size in items:
                        child = f"{rel}/{name}" if rel else name
                        if is_dir:
                            nxt.append(child)
                            dirs[0] += 1
                        else:
                            files[child] = size
                level = nxt
        return files, dirs[0]


# ---------------------------------------------------------------- manifest

def cmd_manifest(a):
    t0 = time.time()
    src = a.src
    paths, symlinks = [], 0
    for dp, dn, fn in os.walk(src, followlinks=False):
        reldir = os.path.relpath(dp, src).replace(os.sep, "/")
        reldir = "" if reldir == "." else reldir
        keep = []
        for d in dn:
            rel = f"{reldir}/{d}" if reldir else d
            if os.path.islink(os.path.join(dp, d)):
                symlinks += 1
            elif not excluded(rel, a.exclude):
                keep.append(d)
        dn[:] = keep
        for f in fn:
            rel = f"{reldir}/{f}" if reldir else f
            full = os.path.join(dp, f)
            if excluded(rel, a.exclude):
                continue
            if os.path.islink(full):
                symlinks += 1
                continue
            if os.path.isfile(full):
                paths.append(rel)
    m = {}
    if a.from_sha256sum:
        want = set(paths)
        with open(a.from_sha256sum, "r", encoding="utf-8", errors="surrogateescape") as f:
            for line in f:
                line = line.rstrip("\r\n")
                if len(line) < 66:
                    continue
                digest, rest = line[:64], line[64:]
                rest = rest[2:] if rest[:2] in ("  ", " *") else rest.lstrip()
                if rest.startswith("./"):
                    rest = rest[2:]
                rest = rest.replace("\\", "/") if os.sep == "\\" else rest
                if rest in want:
                    try:
                        m[rest] = {"size": os.path.getsize(os.path.join(src, rest)), "sha256": digest.lower()}
                    except OSError:
                        pass
        missing = sorted(want - set(m))
        if missing:
            log(f"manifest: {len(missing)} files not in the sha256sum file, hashing them")
            paths = missing
        else:
            paths = []

    def one(rel):
        full = os.path.join(src, rel)
        return rel, os.path.getsize(full), sha256_file(full)
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        for rel, size, digest in pool.map(one, paths):
            m[rel] = {"size": size, "sha256": digest}
    save_manifest(a.out, m)
    total = sum(v["size"] for v in m.values())
    log(f"manifest: {len(m)} files, {human(total)} ({total} bytes), {symlinks} symlinks skipped, "
        f"{sum(1 for v in m.values() if v['size'] >= LARGE)} files >= 8MiB, {time.time() - t0:.1f}s -> {a.out}")
    write_report(a.report, {"files": len(m), "bytes": total, "symlinks": symlinks})
    return 0


# ---------------------------------------------------------------- upload

def cmd_upload(a):
    m = load_manifest(a.manifest)
    dav = Dav(a.dav, a.auth)
    t0 = time.time()
    dirs = sorted({posixpath.dirname(r) for r in m if posixpath.dirname(r)}, key=lambda d: d.count("/"))
    made = set()
    for d in dirs:
        dav.mkcol_all(d, made)
    log(f"upload: {len(dirs)} directories ready in {time.time() - t0:.1f}s")
    errors, done, nbytes = [], [0], [0]
    lock = threading.Lock()

    def one(rel):
        full = os.path.join(a.src, rel)
        size = m[rel]["size"]
        try:
            if size < LARGE:
                with open(full, "rb") as f:
                    data = f.read()
                if len(data) != size:
                    raise DavError(f"{rel}: local size {len(data)} != manifest {size}")
                dav.put((a.prefix + rel), data=data)
            else:
                with open(full, "rb") as f:
                    dav.put((a.prefix + rel), fileobj=f, size=size)
        except (OSError, DavError) as e:
            with lock:
                errors.append(f"{rel}: {e}")
            return
        with lock:
            done[0] += 1
            nbytes[0] += size
            if done[0] % 500 == 0:
                log(f"  uploaded {done[0]}/{len(m)} files, {human(nbytes[0])}")
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        list(pool.map(one, sorted(m)))
    dt = time.time() - t0
    log(f"upload: {done[0]}/{len(m)} files, {human(nbytes[0])} in {dt:.1f}s ({human(nbytes[0] / max(dt, 1e-9))}/s, "
        f"{done[0] / max(dt, 1e-9):.0f} files/s), {len(errors)} errors")
    show_list("errors", errors)
    write_report(a.report, {"uploaded": done[0], "bytes": nbytes[0], "seconds": dt, "errors": errors})
    return 1 if errors else 0


# ---------------------------------------------------------------- verify

def pick_sample(m, n, seed):
    rng = random.Random(seed)
    keys = sorted(m)
    big = [k for k in keys if m[k]["size"] > LARGE]
    chosen = set()
    if big:
        biggest = max(big, key=lambda k: (m[k]["size"], k))
        chosen.add(biggest)
        rest = [k for k in big if k != biggest]
        rng.shuffle(rest)
        chosen.update(rest[:max(0, 4)])  # at least 5 large files (or all of them)
    pool = [k for k in keys if k not in chosen]
    rng.shuffle(pool)
    chosen.update(pool[:max(0, n - len(chosen))])
    return sorted(chosen)


def cmd_verify(a):
    m = load_manifest(a.manifest)
    dav = Dav(a.dav, a.auth)
    t0 = time.time()
    files, ndirs = dav.walk(workers=a.workers)
    t_list = time.time() - t0
    missing = sorted(set(m) - set(files))
    extra = sorted(set(files) - set(m))
    badsize = sorted(f"{k}: listed {files[k]}, expected {m[k]['size']}" for k in m if k in files and files[k] != m[k]["size"])
    log(f"verify: listing {len(files)} files in {ndirs} directories ({t_list:.1f}s); expected {len(m)}")
    show_list("missing", missing)
    show_list("extra", extra)
    show_list("size mismatch (listing)", badsize)

    targets = [k for k in sorted(m) if k in files]
    if not a.all:
        targets = [k for k in pick_sample(m, a.sample, a.seed) if k in files]
    if a.klass != "all":
        targets = [k for k in targets if class_of(m[k]["size"]) == a.klass]
    if a.order == "random":
        random.Random(a.seed).shuffle(targets)
    mism, readerr = [], []
    nbytes, ndone = [0], [0]
    lock = threading.Lock()
    timings = {"small": [], "big": []}  # (size, ttfb, total)
    spans = {"small": [None, None], "big": [None, None]}  # first start, last end
    stats_before = None
    if a.stats:
        stats_before = read_stats(a.stats)
        if stats_before is None:
            log(f"verify: --stats {a.stats!r} is not readable, no call counts")
    t1 = time.time()

    def one(rel):
        tm = {}
        t_start = time.perf_counter()
        w_start = time.time()
        waited = [0.0]

        def go():
            waited[0] = time.perf_counter() - t_start  # time spent in earlier failed attempts
            return dav.sum(a.prefix + rel, tm)
        try:
            digest, size = dav.retrying("GET " + rel, go)
        except DavError as e:
            with lock:
                readerr.append(str(e))
            return
        w_end = time.time()
        with lock:
            cl = class_of(m[rel]["size"])
            timings[cl].append((size, waited[0] + tm["ttfb"], time.perf_counter() - t_start))
            sp = spans[cl]
            sp[0] = w_start if sp[0] is None else min(sp[0], w_start)
            sp[1] = w_end if sp[1] is None else max(sp[1], w_end)
            nbytes[0] += size
            ndone[0] += 1
            if size != m[rel]["size"] or digest != m[rel]["sha256"]:
                mism.append(f"{rel}: got {size} bytes sha256 {digest[:12]}, expected {m[rel]['size']} bytes sha256 {m[rel]['sha256'][:12]}")
            if ndone[0] % 500 == 0:
                log(f"  read {ndone[0]}/{len(targets)} files, {human(nbytes[0])}")
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        list(pool.map(one, targets))
    dt = max(time.time() - t1, 1e-9)
    stats_after = None
    if stats_before is not None:
        time.sleep(0.8)  # the stats file is rewritten every 300 ms
        stats_after = read_stats(a.stats)
    log(f"verify: read {ndone[0]}/{len(targets)} files ({'all' if a.all else 'sample ' + str(len(targets))}), "
        f"{human(nbytes[0])} in {dt:.1f}s ({human(nbytes[0] / dt)}/s); "
        f"{len(mism)} content mismatches, {len(readerr)} read errors")
    classes = {}
    for cl in ("small", "big"):
        if timings[cl]:
            sp = spans[cl]
            classes[cl] = class_report(timings[cl], sp[1] - sp[0])
            log(class_line(cl, classes[cl]))
    calls = None
    if stats_before is not None and stats_after is not None:
        calls = stats_delta(stats_before, stats_after)
        calls["per_1000_files"] = calls["total"] * 1000.0 / max(ndone[0], 1)
        log(f"verify: underlying calls during the reads: {calls['total']} ({calls['per_1000_files']:.1f} per 1000 files) "
            + " ".join(f"{k}={v}" for k, v in calls["calls"].items()) + f"; bytes read {human(calls['bytes_read'])}")
    show_list("content mismatch", sorted(mism))
    show_list("read errors", sorted(readerr))
    bad = len(missing) + len(extra) + len(badsize) + len(mism) + len(readerr)
    log("verify: " + ("OK" if bad == 0 else f"FAILED ({bad} problems)"))
    write_report(a.report, {
        "listed_files": len(files), "listed_dirs": ndirs, "expected": len(m), "read": ndone[0], "bytes": nbytes[0],
        "read_seconds": dt, "list_seconds": t_list, "order": a.order, "class": a.klass,
        "classes": classes, "stats_delta": calls, "missing": missing, "extra": extra, "size_mismatch": badsize,
        "content_mismatch": sorted(mism), "read_errors": sorted(readerr), "ok": bad == 0})
    return 1 if bad else 0


# ---------------------------------------------------------------- mutate

class Mutator:
    def __init__(self, dav, m, seed):
        self.dav, self.m = dav, m
        self.rng = random.Random(seed)
        self.counts = {}
        self.made = set()

    def count(self, k, n=1):
        self.counts[k] = self.counts.get(k, 0) + n

    def put(self, rel, data):
        self.dav.mkcol_all(posixpath.dirname(rel), self.made)
        self.dav.put(rel, data=data)
        self.m[rel] = {"size": len(data), "sha256": sha_of(data)}

    def take(self, pool, n):
        n = min(n, len(pool))
        picked = self.rng.sample(pool, n)
        for p in picked:
            pool.remove(p)
        return picked

    def move_dir(self):
        dirs = {}
        for k, v in self.m.items():
            parts = k.split("/")
            for i in range(1, len(parts)):
                d = "/".join(parts[:i])
                e = dirs.setdefault(d, [0, 0, 0])
                e[0] += 1
                if v["size"] < LARGE:
                    e[1] += 1
                e[2] = max(e[2], len(parts) - i)  # path depth below d, file included
        cands = sorted(d for d, (n, small, depth) in dirs.items() if small >= 3 and depth >= 3 and n <= 300)
        if not cands:
            cands = sorted(d for d, (n, small, depth) in dirs.items() if small >= 1 and depth >= 3)
        if not cands:
            raise SystemExit("mutate: no directory with at least two levels of subdirectories to rename")
        d = self.rng.choice(cands)
        new = d + "-renamed"
        if any(k == new or k.startswith(new + "/") for k in self.m):
            raise SystemExit(f"mutate: {new} already exists in the manifest")
        self.dav.move(d, new)
        for k in [k for k in self.m if k.startswith(d + "/")]:
            self.m[new + k[len(d):]] = self.m.pop(k)
        self.count("dir_renamed")
        return d, new

    def run(self):
        rng, m = self.rng, self.m
        t = {}

        def step(name, fn):
            t0 = time.time()
            r = fn()
            t[name] = time.time() - t0
            log(f"  {name}: {r} ({t[name]:.1f}s)")
        orig_large = sorted(k for k, v in m.items() if v["size"] >= LARGE)

        step("rename directory", lambda: "%s -> %s" % self.move_dir())

        small = sorted(k for k, v in m.items() if v["size"] < LARGE)
        want_small = max(1, round(len(small) * 0.01))

        def overwrite():
            for k in self.take(small, want_small):
                self.put(k, new_content(rng, rng.choice((0, 1, 700, 5000, 200 * 1024))))
                self.count("small_overwritten")
            return f"{self.counts.get('small_overwritten', 0)} small files overwritten"
        step("overwrite 1% small", overwrite)

        def grow():
            picked = self.take(small, 3)
            for k in picked:
                self.put(k, new_content(rng, LARGE + 1))
                self.count("small_grown")
            return f"{len(picked)} small files grown to 8MiB+1"
        step("grow small to large", grow)

        def shrink():
            pool = list(orig_large)
            picked = self.take(pool, 2)
            for k in picked:
                self.put(k, new_content(rng, 5000))
                self.count("large_shrunk")
            return f"{len(picked)} large files shrunk to 5000 bytes"
        step("shrink large", shrink)

        def delete():
            small_set = set(small)
            pool = [k for k in sorted(m) if k in small_set]
            n = max(1, round(len(m) * 0.005))
            picked = self.take(pool, n)
            for k in picked:
                self.dav.delete(k)
                del m[k]
                small.remove(k) if k in small else None
                self.count("deleted")
            return f"{len(picked)} files deleted"
        step("delete 0.5%", delete)

        def atomic():
            pool = [k for k in small if k in m]
            picked = self.take(pool, 20)
            for k in picked:
                data = new_content(rng, rng.choice((1, 700, 5000, 30000)))
                self.dav.put(k + ".tmp", data=data)
                self.dav.move(k + ".tmp", k)
                m[k] = {"size": len(data), "sha256": sha_of(data)}
                self.count("atomic_saved")
            return f"{len(picked)} files saved via .tmp + MOVE"
        step("atomic save", atomic)

        def newdir():
            self.dav.mkcol_all("c6-new", self.made)
            for i in range(200):
                self.put(f"c6-new/f{i:03d}.bin", new_content(rng, rng.choice((0, 1, 700, 5000, 20000))))
            self.put("c6-new/big-20MiB.bin", new_content(rng, 20 * MiB))
            self.count("new_files", 201)
            return "c6-new/: 200 small files + 1 x 20MiB"
        step("new directory", newdir)
        return t


def cmd_mutate(a):
    m = load_manifest(a.manifest)
    dav = Dav(a.dav, a.auth)
    mu = Mutator(dav, m, a.seed)
    t0 = time.time()
    log(f"mutate: seed {a.seed}, {len(m)} files in the manifest")
    try:
        times = mu.run()
    except (DavError, SystemExit) as e:
        save_manifest(a.out + ".partial", m)
        log(f"mutate: FAILED: {e} (partial manifest in {a.out}.partial)")
        return 1
    save_manifest(a.out, m)
    total = sum(v["size"] for v in m.values())
    log(f"mutate: done in {time.time() - t0:.1f}s; {mu.counts}; manifest now {len(m)} files, {human(total)} -> {a.out}")
    write_report(a.report, {"counts": mu.counts, "seconds": times, "files": len(m), "bytes": total})
    return 0


# ---------------------------------------------------------------- burst

def cmd_burst(a):
    m = load_manifest(a.manifest)
    dav = Dav(a.dav, a.auth)
    rng = random.Random(a.seed)
    prefix = a.prefix if a.prefix.endswith("/") else a.prefix + "/"
    made = set()
    dav.mkcol_all(prefix.strip("/"), made)
    items = []
    for i in range(a.n):
        data = new_content(rng, rng.choice((1, 700, 2000, 5000, 20000)))
        items.append((f"{prefix}b{i:05d}.bin", data))
    acked, failed = {}, []
    lock = threading.Lock()
    t0 = time.time()

    def one(it):
        rel, data = it
        try:
            dav.put(rel, data=data)
        except DavError as e:
            with lock:
                failed.append(str(e))
            return
        with lock:
            acked[rel] = {"size": len(data), "sha256": sha_of(data)}
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(one, items))
    m.update(acked)
    save_manifest(a.out, m)
    log(f"burst: {len(acked)}/{a.n} acknowledged in {time.time() - t0:.2f}s, {len(failed)} failed -> {a.out}")
    show_list("failed", failed)
    write_report(a.report, {"acked": len(acked), "failed": failed})
    return 1 if failed else 0


# ---------------------------------------------------------------- wait-drained

def cmd_wait_drained(a):
    t0 = time.time()
    zero, last_print = 0, 0
    st = {}
    while time.time() - t0 < a.timeout:
        try:
            with open(a.stats, "r", encoding="utf-8") as f:
                st = json.load(f)
        except (OSError, ValueError):
            st = {}
        if st:
            if st.get("StagedDamaged", 0) != 0:
                log(f"wait-drained: FAILED: StagedDamaged={st['StagedDamaged']} (acknowledged staged files lost)")
                return 1
            if st.get("StagedFiles", 1) == 0:
                zero += 1
                if zero >= 3:
                    log(f"wait-drained: StagedFiles=0 after {time.time() - t0:.1f}s, StagedDamaged={st.get('StagedDamaged', 0)}")
                    write_report(a.report, {"seconds": time.time() - t0, "stats": st})
                    return 0
            else:
                zero = 0
                if time.time() - last_print > 10:
                    last_print = time.time()
                    log(f"  still staged: {st.get('StagedFiles')} files, {human(st.get('StagedBytes', 0))}")
        time.sleep(1)
    log(f"wait-drained: TIMEOUT after {a.timeout}s: StagedFiles={st.get('StagedFiles')}, "
        f"StagedBytes={st.get('StagedBytes')}, StagedDamaged={st.get('StagedDamaged')}")
    return 1


# ---------------------------------------------------------------- drive-check

class Admin:
    def __init__(self, api, auth_file):
        u = urllib.parse.urlparse(api)
        self.scheme, self.host = u.scheme, u.netloc
        user, pw = read_auth(auth_file)
        out = self.call("POST", "/api/auth/login", {"username": user, "password": pw}, auth=False)
        tok = (out.get("data") or {}).get("token")
        if not tok:
            raise SystemExit(f"admin login failed: code {out.get('code')}: {out.get('message')}")
        self.token = tok

    def call(self, method, path, body, auth=True):
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        c = cls(self.host, timeout=300)
        h = {"Content-Type": "application/json"}
        if auth:
            h["Authorization"] = self.token
        try:
            c.request(method, path, body=json.dumps(body).encode(), headers=h)
            r = c.getresponse()
            data = r.read()
        finally:
            c.close()
        try:
            return json.loads(data)
        except ValueError:
            raise SystemExit(f"{method} {path}: HTTP {r.status}, not JSON")

    def list(self, path):
        out = self.call("POST", "/api/fs/list", {"path": path, "password": "", "refresh": False, "page": 1, "per_page": 0})
        if out.get("code") != 200:
            raise SystemExit(f"list {path}: code {out.get('code')}: {out.get('message')}")
        return [(x["name"], bool(x["is_dir"]), int(x.get("size", 0))) for x in (out.get("data") or {}).get("content") or []]


def cmd_drive_check(a):
    m = load_manifest(a.manifest)
    adm = Admin(a.api, a.auth_admin)
    root = a.remote.rstrip("/")
    t0 = time.time()
    last = [0.0]
    files, dirs = {}, [0]
    stack = [""]
    while stack:
        rel = stack.pop()
        wait = 1.2 - (time.time() - last[0])
        if wait > 0:
            time.sleep(wait)
        last[0] = time.time()
        items = adm.list(root + ("/" + rel if rel else ""))
        dirs[0] += 1
        for name, is_dir, size in items:
            child = f"{rel}/{name}" if rel else name
            if is_dir:
                stack.append(child)
            else:
                files[child] = size
    packs = [k for k in files if k.startswith(".cfs/packs/")]
    segs = [k for k in files if k.startswith((".cfs/log/", ".cfs/ckpt/"))]
    other_meta = [k for k in files if k.startswith(".cfs/") and k not in packs and k not in segs]
    plain = {k: v for k, v in files.items() if not k.startswith(".cfs/")}
    log(f"drive-check: {len(files)} files in {dirs[0]} directories listed in {time.time() - t0:.1f}s")
    log(f"  packs: {len(packs)}, {human(sum(files[k] for k in packs))} ({sum(files[k] for k in packs)} bytes)")
    log(f"  log segments/checkpoints: {len(segs)} ({sum(1 for k in segs if k.startswith('.cfs/log/'))} log, "
        f"{sum(1 for k in segs if k.startswith('.cfs/ckpt/'))} ckpt); other .cfs files: {len(other_meta)}")
    log(f"  1:1 files: {len(plain)}, {human(sum(plain.values()))}")
    expect = {k: v["size"] for k, v in m.items() if v["size"] >= LARGE}
    missing = sorted(k for k in expect if k not in plain)
    badsize = sorted(f"{k}: drive {plain[k]}, expected {expect[k]}" for k in expect if k in plain and plain[k] != expect[k])
    extra = sorted(k for k in plain if k not in expect)
    residue = sorted(k for k in files if k.endswith((".cfs-tmp", ".partial")))
    log(f"  expected 1:1 files (>= 8MiB in the manifest): {len(expect)}")
    show_list("expected 1:1 file missing on the drive", missing)
    show_list("1:1 size mismatch", badsize)
    show_list("1:1 file not in the manifest", extra)
    show_list("residue (.cfs-tmp / .partial)", residue)
    bad = len(missing) + len(badsize) + len(extra) + len(residue)
    log("drive-check: " + ("OK" if bad == 0 else f"FAILED ({bad} problems)"))
    write_report(a.report, {
        "packs": len(packs), "pack_bytes": sum(files[k] for k in packs), "segments": len(segs), "one_to_one": len(plain),
        "expected_one_to_one": len(expect), "missing": missing, "size_mismatch": badsize, "extra": extra,
        "residue": residue, "ok": bad == 0})
    return 1 if bad else 0


# ---------------------------------------------------------------- main

def main():
    p = argparse.ArgumentParser(prog="c6bb", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp, dav=True):
        if dav:
            sp.add_argument("--dav", required=True, help="WebDAV root URL, e.g. http://127.0.0.1:5261/dav/cfs")
            sp.add_argument("--auth", required=True, help="file with one line user:password")
        sp.add_argument("--report", help="write a JSON report here")

    s = sub.add_parser("manifest", help="hash a local tree")
    s.add_argument("--src", required=True)
    s.add_argument("--exclude", action="append", default=[], help="relative path prefix to skip (repeatable)")
    s.add_argument("--out", required=True)
    s.add_argument("--from-sha256sum", help="read hashes from sha256sum output")
    s.add_argument("--workers", type=int, default=8)
    common(s, dav=False)
    s.set_defaults(fn=cmd_manifest)

    s = sub.add_parser("upload", help="PUT every manifest file from --src")
    common(s)
    s.add_argument("--src", required=True)
    s.add_argument("--manifest", required=True)
    s.add_argument("--workers", type=int, default=8)
    s.add_argument("--prefix", default="", help="directory under the DAV root to upload into")
    s.set_defaults(fn=cmd_upload)

    s = sub.add_parser("verify", help="compare the listing and the contents with a manifest")
    common(s)
    s.add_argument("--manifest", required=True)
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", type=int)
    g.add_argument("--all", action="store_true")
    s.add_argument("--workers", type=int, default=8)
    s.add_argument("--order", choices=("sorted", "random"), default="sorted", help="order of the reads (random: seeded shuffle)")
    s.add_argument("--class", dest="klass", choices=("small", "big", "all"), default="all",
                   help="only read files of this size class (small: below 8 MiB, packed; big: 8 MiB and up, stored 1:1)")
    s.add_argument("--stats", help="cfs stats JSON (file path or URL); reports the call-count delta over the read phase")
    s.add_argument("--seed", type=int, default=1)
    s.add_argument("--prefix", default="", help=argparse.SUPPRESS)
    s.set_defaults(fn=cmd_verify)

    s = sub.add_parser("mutate", help="deterministic changes, updating the expected manifest")
    common(s)
    s.add_argument("--manifest", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--seed", type=int, default=1)
    s.set_defaults(fn=cmd_mutate)

    s = sub.add_parser("burst", help="write N small files fast, then exit (for kill -9)")
    common(s)
    s.add_argument("--manifest", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--n", type=int, default=300)
    s.add_argument("--prefix", default="c6-burst/")
    s.add_argument("--seed", type=int, default=1)
    s.set_defaults(fn=cmd_burst)

    s = sub.add_parser("wait-drained", help="wait until the stats file shows nothing staged")
    s.add_argument("--stats", required=True)
    s.add_argument("--timeout", type=int, default=1800)
    common(s, dav=False)
    s.set_defaults(fn=cmd_wait_drained)

    s = sub.add_parser("drive-check", help="inspect the underlying volume through the admin API")
    s.add_argument("--api", required=True, help="OpenList base URL, e.g. http://127.0.0.1:5261")
    s.add_argument("--auth-admin", required=True)
    s.add_argument("--remote", required=True, help="OpenList path of the underlying volume directory")
    s.add_argument("--manifest", required=True)
    common(s, dav=False)
    s.set_defaults(fn=cmd_drive_check)

    a = p.parse_args()
    if not hasattr(a, "prefix"):
        a.prefix = ""
    if a.cmd in ("upload", "verify") and a.prefix:
        a.prefix = a.prefix.strip("/") + "/"
    sys.exit(a.fn(a))


if __name__ == "__main__":
    main()
