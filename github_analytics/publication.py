"""Complete immutable local Parquet generations, publication and crash recovery.

Only cooperative Linux-container readers/writers on a local filesystem are
supported. A generation is published by same-filesystem directory rename,
then a small pointer is replaced atomically. Replacement never deletes the
previous generation. This protects lazy Spark reads after their discovery
lock is released. There is no transaction across datasets or automatic GC.
Legacy direct-write partitions are deliberately rejected, never adopted.
"""

from contextlib import contextmanager
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import uuid


class PublicationError(ValueError):
    """Incomplete, ambiguous, unsupported or damaged publication."""


def day_string(day):
    value = str(day)
    if str(date.fromisoformat(value)) != value:
        raise ValueError("Use a canonical ISO date")
    return value


def local_root(root):
    if "://" in str(root):
        raise PublicationError("Publication supports local filesystem paths only")
    return Path(root).absolute()


def date_directory(root, day):
    return local_root(root) / f"event_date={day_string(day)}"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path, value, fault=None):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    if fault:
        fault("pointer_temp_written")
    os.replace(temporary, path)
    if fault:
        fault("pointer_replaced")
    sync_directory(path.parent)


@contextmanager
def partition_lock(root, day, exclusive=False):
    # Never unlink lock files: different inodes would allow two active locks.
    try:
        import fcntl
    except ImportError as error:
        raise PublicationError("Run storage operations in the Linux Docker runtime") from error
    locks = local_root(root) / "_locks"
    if exclusive:
        locks.mkdir(parents=True, exist_ok=True)
    lock_path = locks / (day_string(day) + ".lock")
    if not exclusive and not lock_path.is_file():
        raise PublicationError("No publication lock: legacy or unpublished dataset")
    with lock_path.open("a+b" if exclusive else "rb") as stream:
        try:
            fcntl.flock(stream, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise PublicationError("Partition busy: active writer or reader discovery") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def read_json(path):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise PublicationError(f"Missing or malformed publication metadata: {path}") from error
    if not isinstance(value, dict):
        raise PublicationError(f"Invalid publication metadata: {path}")
    return value


def validate_pointer(pointer):
    if (set(pointer) != {"generation", "manifest_sha256"}
            or not isinstance(pointer["generation"], str)
            or not re.fullmatch(r"[0-9a-f]{32}", pointer["generation"])
            or not isinstance(pointer["manifest_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", pointer["manifest_sha256"])):
        raise PublicationError("Invalid generation pointer")


def inventory(directory):
    files = sorted(directory.glob("*.parquet"))
    if not files or not (directory / "_SUCCESS").is_file():
        raise PublicationError("Missing Parquet files or completion marker")
    allowed = {path.name for path in files} | {"_SUCCESS", "_manifest.json"}
    for path in directory.iterdir():
        # Hadoop LocalFileSystem checksum sidecars are not data.
        checksum = path.name.startswith(".") and path.name.endswith(".crc")
        if path.is_symlink() or not path.is_file() or (path.name not in allowed and not checksum):
            raise PublicationError(f"Unexpected generation entry: {path.name}")
    return [{"name": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)} for path in files]


def validate_generation(directory, day, pointer):
    validate_pointer(pointer)
    if directory.is_symlink() or not directory.is_dir():
        raise PublicationError("Missing immutable generation directory")
    manifest_path = directory / "_manifest.json"
    if not manifest_path.is_file() or sha256(manifest_path) != pointer["manifest_sha256"]:
        raise PublicationError("Manifest missing or checksum differs")
    manifest = read_json(manifest_path)
    if (set(manifest) != {"version", "date", "generation", "rows", "schema", "files"}
            or manifest["version"] != 1 or manifest["date"] != day_string(day)
            or manifest["generation"] != pointer["generation"]
            or type(manifest["rows"]) is not int or manifest["rows"] < 0
            or not isinstance(manifest["schema"], dict)
            or manifest["files"] != inventory(directory)):
        raise PublicationError("Invalid manifest, date, inventory or Parquet checksum")
    return manifest


def current_pointer(partition, recovering=False):
    if partition.is_symlink():
        raise PublicationError("Partition directory must not be a symlink")
    allowed = {"_generations", "_publication.json"}
    if recovering:
        allowed.add("_publication.json.tmp")
    if partition.exists() and any(path.is_symlink() or path.name not in allowed for path in partition.iterdir()):
        raise PublicationError("Legacy, unexpected or ambiguous partition entries")
    path = partition / "_publication.json"
    if path.exists():
        pointer = read_json(path)
        validate_pointer(pointer)
        return pointer
    return None


def published_partition(root, day):
    """Return validated immutable files; never discover staging by recursive glob.

    SHA256 verification costs a bounded-memory sequential pass over the active
    files. It checks corruption as well as extra/missing/stale files. An external
    process modifying immutable files bypasses the cooperative storage contract.
    """
    root, day = local_root(root), day_string(day)
    with partition_lock(root, day):
        if (root / "_transactions" / day).exists():
            raise PublicationError("Interrupted publication: explicit recovery required")
        partition = date_directory(root, day)
        pointer = current_pointer(partition)
        if pointer is None:
            raise PublicationError("No published partition")
        generation = partition / "_generations" / pointer["generation"]
        manifest = validate_generation(generation, day, pointer)
        return [str(generation / item["name"]) for item in manifest["files"]], manifest


def published_dates(root):
    """All discovered dates must be complete; an interrupted first write fails."""
    root = local_root(root)
    days = {day_string(path.name.split("=", 1)[1]) for path in root.glob("event_date=*")}
    transactions = root / "_transactions"
    if transactions.exists() and any(transactions.iterdir()):
        raise PublicationError("Interrupted publication in dataset")
    for day in sorted(days):
        published_partition(root, day)
    return sorted(days)


def publish(root, day, writer, fault=None):
    """Writer creates stage and validates it, returning scalar rows and schema.

    Exceptions deliberately leave transaction evidence. No catch-and-delete can
    hide a partial write. recover_partition must run after the writer stops.
    fault is a test hook, never enabled by environment variables or normal CLI.
    """
    root, day = local_root(root), day_string(day)
    hook = fault or (lambda phase: None)
    # Reject direct-write legacy data without even adding a lock file to it.
    if not (root / "_transactions" / day).exists():
        current_pointer(date_directory(root, day))
    with partition_lock(root, day, exclusive=True):
        transaction = root / "_transactions" / day
        if transaction.exists():
            raise PublicationError("Recover the interrupted partition before retrying")
        partition = date_directory(root, day)
        old = current_pointer(partition)
        if old:
            validate_generation(partition / "_generations" / old["generation"], day, old)
        transaction.parent.mkdir(parents=True, exist_ok=True)
        transaction.mkdir()
        sync_directory(transaction.parent)
        hook("transaction_created")
        identifier = uuid.uuid4().hex
        journal = {"version": 1, "date": day, "generation": identifier, "old": old, "new": None}
        atomic_json(transaction / "journal.json", journal)
        hook("journal_written")
        stage = transaction / "stage"
        rows, schema = writer(stage)
        hook("stage_written")
        manifest = {"version": 1, "date": day, "generation": identifier,
                    "rows": rows, "schema": schema, "files": inventory(stage)}
        hook("validated")
        atomic_json(stage / "_manifest.json", manifest)
        # Flush files before metadata references them; no power-loss guarantee is
        # claimed for the host/VM disk cache beyond these filesystem requests.
        for path in stage.iterdir():
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
        sync_directory(stage)
        pointer = {"generation": identifier, "manifest_sha256": sha256(stage / "_manifest.json")}
        hook("manifest_written")
        journal["new"] = pointer
        atomic_json(transaction / "journal.json", journal)
        hook("prepared")
        generations = partition / "_generations"
        generations.mkdir(parents=True, exist_ok=True)
        if stage.stat().st_dev != generations.stat().st_dev:
            raise PublicationError("Staging and publication must share a filesystem")
        os.rename(stage, generations / identifier)
        sync_directory(generations)
        sync_directory(transaction)
        hook("generation_renamed")
        validate_generation(generations / identifier, day, pointer)
        atomic_json(partition / "_publication.json", pointer, hook)
        hook("committed")
        shutil.rmtree(transaction)
        sync_directory(transaction.parent)
        hook("cleaned")
        return manifest


def recover_partition(root, day, action="rollback"):
    """Recover only a stopped writer; default retains last complete publication.

    Already-replaced pointer means committed, even if cleanup was interrupted.
    Explicit commit can finish a fully validated prepared generation. Unready
    staging is discarded only in the named transaction; old generations remain.
    Ambiguous/corrupt metadata is never guessed or automatically overwritten.
    """
    if action not in ("rollback", "commit"):
        raise ValueError("Recovery action must be rollback or commit")
    root, day = local_root(root), day_string(day)
    if not (root / "_transactions" / day).exists():
        current_pointer(date_directory(root, day))
    with partition_lock(root, day, exclusive=True):
        transaction = root / "_transactions" / day
        partition = date_directory(root, day)
        current = current_pointer(partition, recovering=transaction.exists())
        if not transaction.exists():
            if current:
                validate_generation(partition / "_generations" / current["generation"], day, current)
            return "no transaction"
        journal_path = transaction / "journal.json"
        if not journal_path.exists():
            # This window precedes all data or pointer changes.
            if any(path.name != "journal.json.tmp" for path in transaction.iterdir()):
                raise PublicationError("Missing journal with unexpected transaction contents")
            if current:
                validate_generation(partition / "_generations" / current["generation"], day, current)
            outcome = "rolled back before write"
        else:
            journal = read_json(journal_path)
            if (set(journal) != {"version", "date", "generation", "old", "new"}
                    or journal["version"] != 1 or journal["date"] != day
                    or not isinstance(journal["generation"], str)
                    or not re.fullmatch(r"[0-9a-f]{32}", journal["generation"])):
                raise PublicationError("Invalid recovery journal")
            old, new = journal["old"], journal["new"]
            for pointer in (old, new):
                if pointer is not None:
                    validate_pointer(pointer)
            if new and new["generation"] != journal["generation"]:
                raise PublicationError("Journal generation mismatch")
            if new and current == new:
                validate_generation(partition / "_generations" / new["generation"], day, new)
                outcome = "committed; cleanup completed"
            elif current != old:
                raise PublicationError("Ambiguous pointer: differs from journal old/new")
            elif action == "commit":
                if new is None:
                    raise PublicationError("No validated prepared generation to commit")
                destination = partition / "_generations" / new["generation"]
                if not destination.exists():
                    stage = transaction / "stage"
                    validate_generation(stage, day, new)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if stage.stat().st_dev != destination.parent.stat().st_dev:
                        raise PublicationError("Recovery rename requires same filesystem")
                    os.rename(stage, destination)
                    sync_directory(destination.parent)
                    sync_directory(transaction)
                validate_generation(destination, day, new)
                atomic_json(partition / "_publication.json", new)
                outcome = "prepared generation committed"
            else:
                if old:
                    validate_generation(partition / "_generations" / old["generation"], day, old)
                outcome = "rolled back; previous publication retained" if old else "rolled back; no publication"
        (partition / "_publication.json.tmp").unlink(missing_ok=True)
        shutil.rmtree(transaction)
        sync_directory(transaction.parent)
        return outcome
