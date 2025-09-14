#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merged AME pipeline: watcher (enqueue via root-level symlinks) + collector (move finished outputs).

Watcher:
  - Recursively scans --source_root
  - Waits for file stability
  - Drops a *file symlink* at:  <watch_root>/<basename>   (NO subfolders)
  - Records an entry in --queue_manifest: {"source","link_relpath","enqueue_ts"}
  - Tracks per-file mtime in --watcher_state

Collector:
  - Reads --queue_manifest
  - Waits until link is moved under <watch_root>/Source/**/<name>
  - Finds output under <output_base or watch_root/Output>/<timestamp>/** (fallback anywhere),
    ignoring sidecars and preferring sibling container if needed
  - Waits until output stable; moves+renames to:
      <dest_root>/<source relative path from --source_root>/<prefix><stem><suffix><out_ext>
  - Tracks processed records in --collector_state

Windows notes:
  - Creating file symlinks requires Administrator or Developer Mode.
  - If symlinks are blocked, use --link_mode=hardlink or --link_mode=copy (copy is safest).
"""

import os
import sys
import time
import json
import errno
import shutil
import zlib
from pathlib import Path
from typing import Dict, Optional, Tuple, Iterable
from absl import app, flags, logging

FLAGS = flags.FLAGS

flags.DEFINE_integer("scan_interval", 3, "Seconds between loop iterations when not --once.")

# ---- Roots / IO ----
flags.DEFINE_string("source_root", r"Q:\proxy test folders\ingest",
                    "Root containing source footage (recursively scanned).")
flags.DEFINE_string("watch_root",  r"Q:\proxy test folders\Watched Folder",
                    "AME Watch Folder root (symlinks dropped here, no subfolders).")
flags.DEFINE_string("dest_root",   r"Q:\proxy test folders\Proxies",
                    "Destination root for finished files (mirrors source tree).")
flags.DEFINE_string("queue_manifest", "queue.jsonl",
                    "Append-only manifest for watcher→collector (JSON Lines).")

# If AME outputs are NOT under watch_root/Output, override it:
flags.DEFINE_string("output_base", None,
                    "Override output base directory. Default: <watch_root>/Output")

# ---- Watcher behavior ----
flags.DEFINE_list("src_exts", [".mp4", ".mov", ".mxf", ".mkv", ".avi", ".wav", ".mp3"],
                  "Source media extensions (lowercase).")
flags.DEFINE_integer("stable_checks", 3, "Consecutive equal-size checks to consider a file stable.")
flags.DEFINE_float("stable_interval", 1.0, "Seconds between stability checks.")
flags.DEFINE_string("watcher_state", "watcher_state.json",
                    "Persistent state (last seen mtime per file).")
flags.DEFINE_enum("dedupe", "hash", ["hash", "increment", "error"],
                  "How to handle duplicate basenames at watch root.")
flags.DEFINE_enum("link_mode", "symlink", ["symlink", "hardlink", "copy"],
                  "How to enqueue: symlink (fast), hardlink (same volume), or copy (safest).")
flags.DEFINE_bool("dry_run", False, "Watcher: log actions without enqueuing.")

# ---- Collector behavior ----
flags.DEFINE_string("collector_state", "collector_state.json",
                    "State file to remember processed manifest records.")
flags.DEFINE_integer("timeout", 60*60, "Per-record timeout (seconds).")
flags.DEFINE_float("poll", 1.0, "Polling interval for waits (seconds).")
flags.DEFINE_float("retry_interval", 0.5, "Seconds between move retries.")
flags.DEFINE_integer("max_move_retries", 10, "Retries if file locked when moving.")
flags.DEFINE_enum("dest_structure", "mirror", ["mirror", "flat"],
                  "Destination structure (mirror original folders or flat).")
flags.DEFINE_string("rename_prefix", "", "Prefix for output filename.")
flags.DEFINE_string("rename_suffix", "_PROXY", "Suffix for output filename (before extension).")
flags.DEFINE_bool("overwrite", True, "Overwrite destination if it exists.")

# Output matching / filtering
flags.DEFINE_enum("output_match", "stem", ["stem", "any"],
                  "How to pick output in timestamp folder: 'stem'=name startswith source stem; 'any'=newest after enqueue.")
flags.DEFINE_bool("allow_global_fallback", True,
                  "If nothing in matched timestamp folder, search anywhere under Output/**.")
flags.DEFINE_list("out_exts",
                  [".mxf", ".mov", ".mp4", ".mkv", ".wav", ".m4a"],
                  "Allowed output container extensions (lowercase).")
flags.DEFINE_list("sidecar_exts",
                  [".xmp", ".tmp", ".log", ".xml", ".json", ".txt"],
                  "Extensions to ignore as sidecars.")

# ---------------- helpers: common ----------------

def ensure_dirs(*dirs: Path) -> None:
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logging.warning("Could not read JSON: %s (starting fresh)", path)
        return default

def save_json_atomic(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)

# ---------------- watcher ----------------

def is_media(p: Path) -> bool:
    return p.is_file() and p.suffix.lower() in set(FLAGS.src_exts)

def wait_file_stable(path: Path, checks: int, interval: float) -> bool:
    prev = -1
    stable = 0
    while True:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            stable = 0
            time.sleep(interval)
            continue
        if size == prev:
            stable += 1
        else:
            stable = 0
        prev = size
        if stable >= checks:
            return True
        time.sleep(interval)

def allocate_link_name(watch_root: Path, src_path: Path) -> str:
    '''Makes sure that files with same name e.g. clip2.mov dont collide, creates clip2_{crc}.mov'''
    stem, ext = src_path.stem, src_path.suffix
    base = f"{stem}{ext}"
    cand = watch_root / base
    if not cand.exists() and not cand.is_symlink():
        return base

    if FLAGS.dedupe == "error":
        raise FileExistsError(f"Duplicate basename at watch root: {base}")

    if FLAGS.dedupe == "hash":
        h = zlib.crc32(str(src_path).encode("utf-8")) & 0xFFFFFFFF
        suff = f"_{h:08x}"
        base2 = f"{stem}{suff}{ext}"
        cand2 = watch_root / base2
        if not cand2.exists() and not cand2.is_symlink():
            return base2
        # fallthrough to increment

    i = 1
    while True:
        base_i = f"{stem}_{i:03d}{ext}"
        cand_i = watch_root / base_i
        if not cand_i.exists() and not cand_i.is_symlink():
            return base_i
        i += 1

def enqueue_link(src_file: Path, watch_root: Path, link_name: str) -> Path:
    link_path = watch_root / link_name
    if FLAGS.link_mode == "symlink":
        try:
            os.symlink(str(src_file), str(link_path))
        except OSError as e:
            if getattr(e, "winerror", None) == 1314 or e.errno in (errno.EPERM, errno.EACCES):
                raise PermissionError("Symlink failed. Run as Admin or enable Developer Mode.") from e
            raise
    elif FLAGS.link_mode == "hardlink":
        # same volume only
        os.link(str(src_file), str(link_path))
    else:  # copy
        shutil.copy2(str(src_file), str(link_path))
    return link_path

def append_manifest(manifest: Path, record: dict) -> None:
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

def watcher_once(w_state: Dict[str, float]) -> int:
    source_root = Path(FLAGS.source_root).resolve()
    watch_root  = Path(FLAGS.watch_root).resolve()
    manifest    = Path(FLAGS.queue_manifest).resolve()

    ensure_dirs(source_root, watch_root, watch_root / "Source", watch_root / "Output", manifest.parent)

    added = 0
    for p in source_root.rglob("*"):
        if not is_media(p):
            continue
        key = str(p.resolve())
        try:
            mtime = p.stat().st_mtime
        except FileNotFoundError:
            continue
        if w_state.get(key) == mtime:
            continue

        logging.info("[watcher] waiting for stability: %s", p)
        if not wait_file_stable(p, FLAGS.stable_checks, FLAGS.stable_interval):
            continue

        link_name = allocate_link_name(watch_root, p)
        link_path = watch_root / link_name
        if FLAGS.dry_run:
            logging.info("[watcher][dry-run] would %s %s -> %s", FLAGS.link_mode, p, link_path)
        else:
            try:
                link_path.parent.mkdir(parents=True, exist_ok=True)
                enqueue_link(p, watch_root, link_name)
            except Exception as e:
                logging.exception("[watcher] enqueue failed: %s", e)
                continue

        enqueue_ts = time.time()
        append_manifest(manifest, {
            "source": str(p),
            "link_relpath": link_name,  # filename only at root
            "enqueue_ts": enqueue_ts,
        })

        w_state[key] = mtime if not FLAGS.dry_run else enqueue_ts
        logging.info("[watcher] enqueued: %s -> %s", p, link_path)
        added += 1

    return added

# ---------------- collector ----------------

def wait_for_moved_link_anywhere(watch_root: Path, link_name: str,
                                 timeout: int, poll: float) -> Path:
    start = time.time()
    drop_path = watch_root / link_name
    src_root = watch_root / "Source"

    logging.info("[collector] waiting for link to leave: %s", drop_path)
    while time.time() - start < timeout:
        if not drop_path.exists() and not drop_path.is_symlink():
            break
        time.sleep(poll)
    else:
        raise TimeoutError(f"Link never left drop location: {drop_path}")

    logging.info("[collector] searching under %s for %s …", src_root, link_name)
    while time.time() - start < timeout:
        for p in src_root.rglob(link_name):
            if p.is_symlink() or p.exists():
                return p
        time.sleep(poll)
    raise TimeoutError(f"Link not found under {src_root}: {link_name}")

def extract_timestamp_folder(moved_link: Path, source_root: Path) -> Optional[str]:
    try:
        parts = moved_link.relative_to(source_root).parts
        return parts[0] if parts else None
    except Exception:
        return None

def is_allowed_output(p: Path) -> bool:
    ext = p.suffix.lower()
    if ext in set(FLAGS.sidecar_exts):
        return False
    if FLAGS.out_exts and ext not in set(FLAGS.out_exts):
        return False
    return True

def prefer_container_sibling(p: Path) -> Path:
    if p.suffix.lower() in set(FLAGS.sidecar_exts):
        for ext in FLAGS.out_exts:
            cand = p.with_suffix(ext)
            if cand.exists():
                return cand
    return p

def choose_from_folder(root: Path, stem: str, since_ts: float, mode: str) -> Optional[Path]:
    if not root.exists():
        return None
    best_mtime = -1.0
    best_path: Optional[Path] = None
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        if st.st_mtime < since_ts:
            continue

        p_eff = prefer_container_sibling(p)
        try:
            st_eff = p_eff.stat()
        except FileNotFoundError:
            continue

        if not is_allowed_output(p_eff):
            continue

        if mode == "stem":
            ps = p_eff.stem
            if not (ps == stem or ps.startswith(stem)):
                continue

        if st_eff.st_mtime > best_mtime:
            best_mtime = st_eff.st_mtime
            best_path = p_eff
    return best_path

def find_output(watch_root: Path, stem: str, ts_folder: Optional[str],
                since_ts: float, mode: str, allow_global_fallback: bool) -> Optional[Path]:
    out_base = Path(FLAGS.output_base).resolve() if FLAGS.output_base else (watch_root / "Output")
    # Prefer timestamp folder
    if ts_folder:
        cand = choose_from_folder(out_base / ts_folder, stem, since_ts, mode)
        if cand:
            return cand
    # Fallback: anywhere under Output/**
    if allow_global_fallback:
        return choose_from_folder(out_base, stem, since_ts, mode)
    return None

def wait_file_stable_timeout(path: Path, checks: int, interval: float, timeout: int) -> bool:
    start = time.time()
    prev = -1
    stable = 0
    while time.time() - start < timeout:
        if not path.exists():
            time.sleep(interval)
            continue
        size = path.stat().st_size
        if size == prev:
            stable += 1
        else:
            stable = 0
        prev = size
        if stable >= checks:
            return True
        time.sleep(interval)
    return False

def compute_dest_path(dest_root: Path, source_root: Path, source_path: Path, out_ext: str) -> Path:
    stem = source_path.stem
    new_name = f"{FLAGS.rename_prefix}{stem}{FLAGS.rename_suffix}{out_ext}"
    if FLAGS.dest_structure == "mirror":
        try:
            rel_parent = source_path.parent.relative_to(source_root)
            return dest_root / rel_parent / new_name
        except Exception:
            return dest_root / new_name
    else:
        return dest_root / new_name

def move_with_retries(src: Path, dst: Path, overwrite: bool, retries: int, interval: float) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    for i in range(retries + 1):
        try:
            if dst.exists():
                if not overwrite:
                    raise FileExistsError(f"Destination exists and overwrite=False: {dst}")
                dst.unlink()
            shutil.move(str(src), str(dst))
            return
        except Exception:
            if i == retries:
                raise
            time.sleep(interval)

def iter_manifest(path: Path) -> Iterable[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logging.warning("Skipping bad manifest line: %r", line)

def record_id(rec: dict) -> str:
    return f"{rec.get('link_relpath','')}|{rec.get('enqueue_ts','')}"

def collector_once(c_state: Dict[str, bool]) -> int:
    manifest   = Path(FLAGS.queue_manifest).resolve()
    watch_root = Path(FLAGS.watch_root).resolve()
    source_root= Path(FLAGS.source_root).resolve()
    dest_root  = Path(FLAGS.dest_root).resolve()

    successes = 0
    for rec in iter_manifest(manifest):
        rid = record_id(rec)
        if c_state.get(rid):
            continue

        src_path = Path(rec["source"]).resolve()
        link_name = Path(rec["link_relpath"]).name
        enqueue_ts = float(rec.get("enqueue_ts", time.time()))
        logging.info("[collector] record: %s (link=%s)", src_path, link_name)

        # 1) wait for moved link
        try:
            moved_link = wait_for_moved_link_anywhere(
                watch_root=watch_root, link_name=link_name,
                timeout=FLAGS.timeout, poll=FLAGS.poll
            )
            logging.info("[collector] moved link: %s", moved_link)
        except TimeoutError as te:
            logging.error("[collector] timeout (moved link): %s", te)
            continue

        ts_folder = extract_timestamp_folder(moved_link, watch_root / "Source")

        # 2) find output
        deadline = time.time() + FLAGS.timeout
        out_candidate = None
        while time.time() < deadline:
            out_candidate = find_output(
                watch_root=watch_root,
                stem=src_path.stem,
                ts_folder=ts_folder,
                since_ts=enqueue_ts - 1,
                mode=FLAGS.output_match,
                allow_global_fallback=FLAGS.allow_global_fallback,
            )
            if out_candidate:
                break
            time.sleep(FLAGS.poll)

        if not out_candidate:
            logging.error("[collector] no output found (stem=%s, mode=%s, ts=%r, base=%s)",
                          src_path.stem, FLAGS.output_match, ts_folder,
                          FLAGS.output_base or str(watch_root / "Output"))
            continue

        logging.info("[collector] output candidate: %s", out_candidate)

        # 3) wait for stable
        if not wait_file_stable_timeout(out_candidate, FLAGS.stable_checks, FLAGS.stable_interval, FLAGS.timeout):
            logging.error("[collector] output never stabilized: %s", out_candidate)
            continue

        # 4) move to destination (mirror)
        dest_path = compute_dest_path(Path(FLAGS.dest_root), Path(FLAGS.source_root), src_path, out_candidate.suffix)
        try:
            move_with_retries(out_candidate, dest_path, overwrite=FLAGS.overwrite,
                              retries=FLAGS.max_move_retries, interval=FLAGS.retry_interval)
        except Exception as e:
            logging.exception("[collector] move failed: %s", e)
            continue

        logging.info("✅ [collector] moved to destination: %s", dest_path)
        c_state[rid] = True
        successes += 1

    return successes

# ---------------- main loop ----------------

def main(argv):
    del argv
    logging.set_stderrthreshold("info")

    logging.info("Scan=%ss", FLAGS.scan_interval)
    logging.info("Roots: source=%s | watch=%s | dest=%s",
                 Path(FLAGS.source_root).resolve(), Path(FLAGS.watch_root).resolve(), Path(FLAGS.dest_root).resolve())
    logging.info("Output base: %s", FLAGS.output_base or str(Path(FLAGS.watch_root) / "Output"))
    logging.info("Watcher: link_mode=%s dedupe=%s stable=%dx@%ss",
                 FLAGS.link_mode, FLAGS.dedupe, FLAGS.stable_checks, FLAGS.stable_interval)
    logging.info("Collector: match=%s fallback=%s out_exts=%s sidecars=%s overwrite=%s",
                 FLAGS.output_match, FLAGS.allow_global_fallback, FLAGS.out_exts, FLAGS.sidecar_exts, FLAGS.overwrite)

    # Load states
    w_state_path = Path(FLAGS.watcher_state).resolve()
    c_state_path = Path(FLAGS.collector_state).resolve()
    w_state = load_json(w_state_path, {})
    c_state = load_json(c_state_path, {})

    def do_watcher():
        changed = watcher_once(w_state)
        if changed:
            save_json_atomic(w_state_path, w_state)
        return changed

    def do_collector():
        changed = collector_once(c_state)
        if changed:
            save_json_atomic(c_state_path, c_state)
        return changed
    
    # Loop modes
    logging.info("Running loop. Press Ctrl+C to stop.")
    try:
        while True:
            do_watcher()
            do_collector()
            time.sleep(FLAGS.scan_interval)
    except KeyboardInterrupt:
        logging.info("Stopped by user.")
        save_json_atomic(w_state_path, w_state)
        save_json_atomic(c_state_path, c_state)

if __name__ == "__main__":
    app.run(main)
