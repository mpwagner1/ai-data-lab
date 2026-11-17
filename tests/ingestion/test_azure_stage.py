"""Tests for ingestion.azure_stage with a fake container (no network, no credential)."""

import hashlib
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from ingestion import azure_stage
from ingestion.azure_stage import BlobInfo, StageSummary

SECRET_CONTENT = b"id,name\n1,SECRET-CONTENT-ROW\n"


class FakeContainer:
    """In-memory container: blobs are name -> bytes. Records every call."""

    def __init__(self, blobs: dict[str, bytes], prefixes: tuple[str, ...] = ()) -> None:
        self.blobs = blobs
        self.prefixes = prefixes
        self.calls: list[str] = []

    def list_blobs(self, prefix: str) -> Iterator[BlobInfo]:
        self.calls.append(f"list:{prefix}")
        for name in self.prefixes:
            yield BlobInfo(name, is_prefix=True)
        for name, data in self.blobs.items():
            yield BlobInfo(name, size=len(data))

    def read_blob(self, name: str) -> Iterator[bytes]:
        self.calls.append(f"read:{name}")
        data = self.blobs[name]
        for start in range(0, len(data), 4):
            yield data[start : start + 4]


class LyingContainer(FakeContainer):
    """Reports a small size in the listing but streams more bytes."""

    def list_blobs(self, prefix: str) -> Iterator[BlobInfo]:
        yield BlobInfo("orders/big.csv", size=1)


def _run(container: FakeContainer, tmp_path: Path, **kwargs: int) -> tuple[StageSummary, Path]:
    summary = azure_stage.stage(container, "orders", "orders/", tmp_path, **kwargs)
    return summary, tmp_path / "orders"


def test_stages_blobs_under_the_prefix(tmp_path: Path) -> None:
    container = FakeContainer({"orders/a.csv": b"id\n1\n", "orders/b.csv": b"id\n2\n"})

    summary, folder = _run(container, tmp_path)

    assert summary == StageSummary(staged=2, skipped=0, ignored=0, failed=0)
    assert (folder / "a.csv").read_bytes() == b"id\n1\n"
    assert (folder / "b.csv").read_bytes() == b"id\n2\n"
    assert not list(folder.glob("*.tmp"))
    assert container.calls[0] == "list:orders/"


def test_skips_identical_local_file(tmp_path: Path) -> None:
    container = FakeContainer({"orders/a.csv": b"id\n1\n"})
    _run(container, tmp_path)

    summary, folder = _run(container, tmp_path)

    assert summary == StageSummary(staged=0, skipped=1, ignored=0, failed=0)
    assert (folder / "a.csv").read_bytes() == b"id\n1\n"


def test_collision_fails_and_keeps_the_local_file(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "a.csv").write_bytes(b"local bytes")
    container = FakeContainer({"orders/a.csv": b"blob bytes", "orders/b.csv": b"ok"})

    summary, _ = _run(container, tmp_path)

    assert summary.failed == 1
    assert summary.staged == 1  # the other blob still goes through
    assert (folder / "a.csv").read_bytes() == b"local bytes"
    assert not list(folder.glob("*.tmp"))


def test_symlink_in_the_way_is_a_collision(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    real = tmp_path / "real.csv"
    real.write_bytes(b"same")
    (folder / "a.csv").symlink_to(real)

    summary, _ = _run(FakeContainer({"orders/a.csv": b"same"}), tmp_path)

    assert summary.failed == 1


@pytest.mark.parametrize(
    "name",
    [
        "orders/..",
        "orders/../escape.csv",
        "orders/a..b.csv",
        "orders/sub/a.csv",
        "orders/back\\slash.csv",
        "orders/bad\x00name.csv",
        "orders/bad\nname.csv",
        "orders/a.csv.tmp",
        "orders/.",
    ],
)
def test_rejected_names_fail_and_write_nothing(tmp_path: Path, name: str) -> None:
    container = FakeContainer({name: b"x", "orders/good.csv": b"ok"})

    summary, folder = _run(container, tmp_path)

    assert summary.failed == 1
    assert summary.staged == 1
    assert sorted(p.name for p in folder.iterdir()) == ["good.csv"]
    assert not (tmp_path / "escape.csv").exists()


def test_empty_local_name_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(azure_stage.RejectedNameError):
        azure_stage.local_name("orders/", "orders/")


def test_folder_markers_and_sub_prefixes_are_ignored_and_counted(tmp_path: Path) -> None:
    container = FakeContainer(
        {"orders/": b"", "orders/archive/": b"", "orders/a.csv": b"id\n1\n"},
        prefixes=("orders/2024/",),
    )

    summary, folder = _run(container, tmp_path)

    assert summary == StageSummary(staged=1, skipped=0, ignored=3, failed=0)
    assert [p.name for p in folder.iterdir()] == ["a.csv"]


def test_blob_over_size_cap_in_properties_is_not_downloaded(tmp_path: Path) -> None:
    container = FakeContainer({"orders/big.csv": b"x" * 10})

    summary, folder = _run(container, tmp_path, max_blob_bytes=5)

    assert summary.failed == 1
    assert "read:orders/big.csv" not in container.calls
    assert not (folder / "big.csv").exists()


def test_blob_over_size_cap_while_streaming_leaves_no_file(tmp_path: Path) -> None:
    container = LyingContainer({"orders/big.csv": b"x" * 10})
    # Properties say 1 byte (under the cap) but 10 bytes arrive.

    summary, folder = _run(container, tmp_path, max_blob_bytes=5)

    assert summary.failed == 1
    assert not list(folder.iterdir())


def test_blob_count_cap_stages_the_first_blobs_and_counts_one_failure(tmp_path: Path) -> None:
    container = FakeContainer({f"orders/{n}.csv": b"x" for n in "abc"})

    summary, folder = _run(container, tmp_path, max_blobs=2)

    assert summary.staged == 2
    assert summary.failed == 1
    assert sorted(p.name for p in folder.iterdir()) == ["a.csv", "b.csv"]


def test_nothing_to_stage_exits_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")

    code = azure_stage.main(
        ["orders", "orders"], lambda a, c: FakeContainer({}), landing_root=tmp_path
    )

    assert code == 0


def test_main_exit_one_when_a_blob_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")
    container = FakeContainer({"orders/sub/a.csv": b"x"})

    code = azure_stage.main(["orders", "orders/"], lambda a, c: container, landing_root=tmp_path)

    assert code == 1


@pytest.mark.parametrize("missing", ["AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_CONTAINER"])
def test_missing_env_var_exits_two_without_building_a_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")
    monkeypatch.delenv(missing)

    def factory(account: str, container: str) -> FakeContainer:
        raise AssertionError("client must not be built")

    assert azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path) == 2


@pytest.mark.parametrize(
    ("source", "prefix"), [("../x", "orders/"), ("orders", "../x/"), ("orders", "/")]
)
def test_invalid_source_or_prefix_exits_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str, prefix: str
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")

    code = azure_stage.main([source, prefix], lambda a, c: FakeContainer({}), landing_root=tmp_path)

    assert code == 2


def test_sign_in_or_access_error_exits_two(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")

    def factory(account: str, container: str) -> FakeContainer:
        raise PermissionError("https://testaccount.blob.core.windows.net/?sig=TOKEN123")

    assert azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path) == 2


def test_logs_never_hold_tokens_urls_content_or_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")
    container = FakeContainer({"orders/a.csv": SECRET_CONTENT, "orders/b/c.csv": b"x"})
    caplog.set_level(logging.DEBUG)

    azure_stage.main(["orders", "orders/"], lambda a, c: container, landing_root=tmp_path)
    failing = azure_stage.main(
        ["orders", "orders/"],
        lambda a, c: (_ for _ in ()).throw(RuntimeError("sig=TOKEN123 https://x?sv=1")),
        landing_root=tmp_path,
    )

    assert failing == 2
    text = caplog.text
    assert "event=azure_stage_write" in text
    assert hashlib.sha256(SECRET_CONTENT).hexdigest() in text
    for forbidden in (
        "SECRET-CONTENT",
        "TOKEN123",
        "sig=",
        "https://",
        "?sv",
        "testaccount",
        "testcontainer",
    ):
        assert forbidden not in text


def test_azure_sdk_logger_is_kept_quiet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "testcontainer")

    azure_stage.main(["orders", "orders/"], lambda a, c: FakeContainer({}), landing_root=tmp_path)

    assert logging.getLogger("azure").level == logging.WARNING


def test_no_write_calls_exist_in_the_module() -> None:
    source = Path(azure_stage.__file__).read_text()
    for call in (
        "upload_blob",
        "delete_blob",
        "set_blob",
        "acquire_lease",
        "start_copy",
        "stage_block",
    ):
        assert call not in source
