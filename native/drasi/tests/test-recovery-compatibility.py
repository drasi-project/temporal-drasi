import ctypes
import hashlib
import json
from pathlib import Path
import sys
import tempfile


class Buffer(ctypes.Structure):
    _fields_ = [
        ("allocation", ctypes.c_uint64),
        ("data", ctypes.c_void_p),
        ("length", ctypes.c_size_t),
    ]


class Status(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_int32),
        ("reserved", ctypes.c_uint32),
        ("buffer", Buffer),
    ]


def load(path):
    library = ctypes.CDLL(str(path))
    library.temporal_drasi_v1_cancel_new.restype = ctypes.c_uint64
    library.temporal_drasi_v1_cancel_free.argtypes = [ctypes.c_uint64]
    library.temporal_drasi_v1_buffer_free.argtypes = [ctypes.c_uint64]
    library.temporal_drasi_v1_open.argtypes = [
        ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_uint32
    ]
    library.temporal_drasi_v1_apply.argtypes = [
        ctypes.c_uint64, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_uint32
    ]
    library.temporal_drasi_v1_close.argtypes = [
        ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint32
    ]
    for name in ("open", "apply", "close"):
        getattr(library, f"temporal_drasi_v1_{name}").restype = Status
    return library


def call(library, operation, value=None, handle=None):
    cancel = library.temporal_drasi_v1_cancel_new()
    try:
        function = getattr(library, f"temporal_drasi_v1_{operation}")
        if operation == "close":
            status = function(handle, cancel, 30000)
        else:
            encoded = json.dumps(value).encode("utf-8")
            data = ctypes.create_string_buffer(encoded)
            arguments = [data, len(encoded), cancel, 30000]
            if handle is not None:
                arguments.insert(0, handle)
            status = function(*arguments)
        try:
            assert status.reserved == 0
            result = json.loads(ctypes.string_at(status.buffer.data, status.buffer.length))
            return status.code, result
        finally:
            assert library.temporal_drasi_v1_buffer_free(status.buffer.allocation) == 0
    finally:
        assert library.temporal_drasi_v1_cancel_free(cancel) == 0


old_path, new_path, workspace = map(Path, sys.argv[1:])
old, new = load(old_path), load(new_path)
with tempfile.TemporaryDirectory(prefix="legacy-retirement-", dir=workspace.resolve()) as directory:
    root = Path(directory).resolve()
    config = {
        "version": 1,
        "state_dir": str(root / "owner"),
        "owner_epoch": 1,
        "instance_id": "legacy-retirement-proof",
        "start_mode": "dormant",
    }
    code, opened = call(old, "open", config)
    assert code == 0, opened
    assert call(old, "close", handle=opened["handle"])[0] == 0
    recovery_config = dict(config, start_mode="checkpoint_recovery")
    code, recovery = call(new, "open", recovery_config)
    assert code == 0, recovery
    code, checkpoint = call(
        new, "apply", {"op": "checkpoint", "destination": str(root / "cut")},
        handle=recovery["handle"],
    )
    assert code == 0, checkpoint
    assert checkpoint["checkpoint"]["retirement_id"]
    assert call(new, "close", handle=recovery["handle"])[0] == 0
    code, stale = call(old, "open", config)
    assert code == 3 and "definition changed" in stale["message"], stale
    code, unsupported = call(old, "open", recovery_config)
    assert code == 1 and "unknown variant" in unsupported["message"], unsupported
    successor = dict(
        config, state_dir=str(root / "successor"), owner_epoch=2,
        restore_from=str(root / "cut"),
    )
    code, opened = call(new, "open", successor)
    assert code == 0, opened
    assert call(new, "close", handle=opened["handle"])[0] == 0
    print(json.dumps({
        "legacy_library_sha256": hashlib.sha256(old_path.read_bytes()).hexdigest(),
        "legacy_created_state": "recovered",
        "legacy_stale_opener": "refused before plugin/SDK construction",
        "legacy_new_mode": "refused without fallback",
        "isolated_successor": "opened",
    }, indent=2))
