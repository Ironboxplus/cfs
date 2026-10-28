# c6bb: black-box checks for a cfs volume used through OpenList

`c6bb.py` is Python 3.12 stdlib only. It talks HTTP only (WebDAV, admin API) and reads
the `CFS_STATS_FILE` JSON; it never reads cfs state directories. Any inconsistency gives a non-zero exit.
Auth files hold one line, `user:password`. All subcommands accept `--report FILE.json`.

    manifest     --src DIR [--exclude REL ...] --out M.json [--from-sha256sum FILE] [--workers 8]
    upload       --src DIR --dav URL --auth FILE --manifest M.json [--workers 8] [--prefix DIR]
    verify       --dav URL --auth FILE --manifest M.json (--all | --sample N) [--workers 8] [--seed S]
    mutate       --dav URL --auth FILE --manifest M.json --out M2.json --seed S
    burst        --dav URL --auth FILE --manifest M.json --out M3.json --n 300 --prefix c6-burst/ --seed S
    wait-drained --stats FILE [--timeout 1800]
    drive-check  --api URL --auth-admin FILE --remote /mount/volume-dir --manifest M.json

`upload` is an addition to the spec. `drive-check` lists one directory per 1.2 s (about 5 min for 230 directories).
The volume mirrors every directory on the drive, so budget for that.

## Sequence (form 1, OpenList /dav; DAV=http://HOST:PORT/dav/cfs-sync)

    c6bb.py manifest --src /root/autodl-tmp --exclude venvs --exclude .autodl --out m1.json
    (upload by `openlist sync`, or: c6bb.py upload --src ... --dav $DAV --auth dav.auth --manifest m1.json)
    c6bb.py wait-drained --stats /root/cfs-c6/logs/cfs-stats.json --timeout 1800
    c6bb.py verify --dav $DAV --auth dav.auth --manifest m1.json --all
    c6bb.py mutate --dav $DAV --auth dav.auth --manifest m1.json --out m2.json --seed 7
    c6bb.py verify --dav $DAV --auth dav.auth --manifest m2.json --all
    c6bb.py burst  --dav $DAV --auth dav.auth --manifest m2.json --out m3.json --n 300 --prefix c6-burst/ --seed 9
    kill -9 <openlist pid>; restart
    c6bb.py verify --dav $DAV --auth dav.auth --manifest m3.json --all
    c6bb.py wait-drained ...; stop; delete the cfs state dir (<data>/cfs); start
    c6bb.py verify --dav $DAV --auth dav.auth --manifest m3.json --all
    c6bb.py drive-check --api http://HOST:PORT --auth-admin admin.auth --remote /115/.../cfs-sync --manifest m3.json

Form 2: same with `--dav` pointing at the VFS WebDAV of `openlist cfs serve webdav /cfs-mount`.

## Local run

`run_local.py` runs the whole sequence (both forms) against a scratch OpenList:

    python run_local.py --bin openlist-cfs-a38f4e98.exe --work E:\Go\Openlist\tmp\c6bb-local --src E:\Go\Openlist\OP-sync

Port 5261 (OpenList) and 5262 (VFS WebDAV); never 5244.
