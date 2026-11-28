"""Tests for ingestion.azure_stage with a fake container (no network, no credential)."""

import hashlib
import logging
import os
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from azure.core.exceptions import AzureError, ClientAuthenticationError
from azure.identity import CredentialUnavailableError
from azure.storage.blob import BlobPrefix

from ingestion import azure_stage, bronze
from ingestion.azure_stage import BlobInfo, StageSummary

SECRET_CONTENT = b"id,name\n1,SECRET-CONTENT-ROW\n"
SENTINEL_ACCOUNT = "sentinelacct01"
SENTINEL_CONTAINER = "sentinel-ctr-01"
SENTINEL_TOKEN = "SENTINEL-TOKEN-9f3a"
SENTINEL_BLOB_BYTES = b"id\nSENTINEL-BLOB-BYTES-77\n"


class FakeContainer:
    """In-memory container: blobs are name -> bytes. Records every call."""

    chunk_size = 4  # small, so tests cross chunk boundaries

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
        for start in range(0, len(data), self.chunk_size):
            yield data[start : start + self.chunk_size]


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
        lambda a, c: (_ for _ in ()).throw(AzureError("sig=TOKEN123 https://x?sv=1")),
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


# ---------------------------------------------------------------------------
# Shared helpers for the tests below
# ---------------------------------------------------------------------------


class FailingReadContainer(FakeContainer):
    """Yields one chunk of a blob, then raises (a dropped connection)."""

    def read_blob(self, name: str) -> Iterator[bytes]:
        self.calls.append(f"read:{name}")
        yield b"part"
        raise ConnectionError(SENTINEL_TOKEN)


class OnlyOneFailsContainer(FakeContainer):
    """Reads fail for one named blob; all others stream normally."""

    def __init__(self, blobs: dict[str, bytes], bad: str) -> None:
        super().__init__(blobs)
        self.bad = bad

    def read_blob(self, name: str) -> Iterator[bytes]:
        if name == self.bad:
            self.calls.append(f"read:{name}")
            raise TimeoutError(SENTINEL_TOKEN)
        yield from super().read_blob(name)


class RaisingListingContainer:
    """`list_blobs` raises when iterated, as a real listing does on an access error."""

    def __init__(self, error: Exception, first: list[BlobInfo] | None = None) -> None:
        self.error = error
        self.first = first or []

    def list_blobs(self, prefix: str) -> Iterator[BlobInfo]:
        yield from self.first
        raise self.error

    def read_blob(self, name: str) -> Iterator[bytes]:
        raise AssertionError("nothing should be read")


class CountingStreamContainer(FakeContainer):
    """Counts the chunks handed out, to prove streaming stops at the cap."""

    def __init__(self, blobs: dict[str, bytes], listed_size: int) -> None:
        super().__init__(blobs)
        self.listed_size = listed_size
        self.chunks_served = 0

    def list_blobs(self, prefix: str) -> Iterator[BlobInfo]:
        for name in self.blobs:
            yield BlobInfo(name, size=self.listed_size)

    def read_blob(self, name: str) -> Iterator[bytes]:
        for _ in range(1000):
            self.chunks_served += 1
            yield b"xxxx"


@pytest.fixture
def azure_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, "testaccount")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, "testcontainer")


def _files(folder: Path) -> list[str]:
    return sorted(p.name for p in folder.iterdir())


def _main(argv: list[str], container: object, tmp_path: Path) -> int:
    return azure_stage.main(argv, lambda a, c: container, landing_root=tmp_path)  # type: ignore[arg-type,return-value]


# ---------------------------------------------------------------------------
# Prefix rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("orders", "orders/"),
        ("orders/", "orders/"),
        ("a/b", "a/b/"),
        ("a/b/", "a/b/"),
    ],
)
def test_normalize_prefix_adds_exactly_one_trailing_slash(raw: str, expected: str) -> None:
    assert azure_stage.normalize_prefix(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "/",
        "/orders",
        "..",
        "orders/..",
        "../orders",
        "a/../b",
        "a\\b",
        "bad\x00",
        "bad\n",
        "\x7f",
    ],
)
def test_normalize_prefix_rejects_unsafe_values(raw: str) -> None:
    with pytest.raises(ValueError, match="invalid prefix"):
        azure_stage.normalize_prefix(raw)


def test_main_lists_with_the_normalized_prefix(tmp_path: Path, azure_env: None) -> None:
    container = FakeContainer({"orders/a.csv": b"x"})

    code = _main(["orders", "orders"], container, tmp_path)

    assert code == 0
    assert container.calls[0] == "list:orders/"


def test_blobs_in_sub_prefixes_are_not_staged(tmp_path: Path) -> None:
    container = FakeContainer(
        {"orders/a.csv": b"x", "orders/2024/deep.csv": b"y"}, prefixes=("orders/2024/",)
    )

    summary, folder = _run(container, tmp_path)

    assert _files(folder) == ["a.csv"]
    assert summary.ignored == 1  # the sub-prefix entry
    assert summary.failed == 1  # the deep blob that leaked into the listing is rejected, not staged
    assert "read:orders/2024/deep.csv" not in container.calls


def test_stage_writes_into_a_folder_named_for_the_source(tmp_path: Path) -> None:
    container = FakeContainer({"shipments/a.csv": b"x"})

    azure_stage.stage(container, "ship", "shipments/", tmp_path)

    assert _files(tmp_path) == ["ship"]
    assert _files(tmp_path / "ship") == ["a.csv"]


def test_empty_listing_creates_nothing_and_succeeds(tmp_path: Path) -> None:
    summary, folder = _run(FakeContainer({}), tmp_path)

    assert summary == StageSummary()
    assert not folder.exists()


# ---------------------------------------------------------------------------
# Name rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "...",
        "a..b.csv",
        "..hidden",
        "trailing..",
        "sub/a.csv",
        "/a.csv",
        "a/",
        "back\\slash.csv",
        "..\\x.csv",
        "nul\x00.csv",
        "tab\t.csv",
        "new\nline.csv",
        "cr\r.csv",
        "esc\x1b.csv",
        "del\x7f.csv",
        "a.tmp",
        ".tmp",
        "a.csv.tmp",
        "a.TMP",
        "a.csv.Tmp",
        ".TMP",
    ],
)
def test_local_name_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(azure_stage.RejectedNameError):
        azure_stage.local_name(f"orders/{name}", "orders/")


@pytest.mark.parametrize(
    "name",
    [
        "a.csv",
        "A B.csv",
        ".hidden",
        "a.tmpx",
        "tmp",
        "a.tmp.csv",
        "%2e%2e",
        "a.b.c.csv",
        "unié.csv",
    ],
)
def test_local_name_accepts_ordinary_names_unchanged(name: str) -> None:
    assert azure_stage.local_name(f"orders/{name}", "orders/") == name


def test_percent_encoded_dots_are_literal_not_decoded(tmp_path: Path) -> None:
    summary, folder = _run(FakeContainer({"orders/%2e%2e": b"x"}), tmp_path)

    assert summary.staged == 1
    assert _files(folder) == ["%2e%2e"]


@pytest.mark.parametrize("name", ["orders//a.csv", "orders/x/../a.csv", "orders/orders/a.csv"])
def test_names_that_look_close_to_safe_are_rejected_and_write_nothing(
    tmp_path: Path, name: str
) -> None:
    summary, folder = _run(FakeContainer({name: b"x"}), tmp_path)

    assert summary == StageSummary(failed=1)
    assert not folder.exists() or _files(folder) == []
    assert _files(tmp_path) in ([], ["orders"])


def test_traversal_name_never_writes_outside_the_landing_folder(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    container = FakeContainer({"orders/../../evil.csv": b"x", "orders/..\\evil2.csv": b"x"})

    azure_stage.stage(container, "orders", "orders/", landing)

    assert not list(tmp_path.rglob("evil*"))


def test_listed_name_outside_the_prefix_is_not_staged(tmp_path: Path) -> None:
    summary, folder = _run(FakeContainer({"other.csv": b"x"}), tmp_path)

    assert summary == StageSummary(failed=1)
    assert not folder.exists()


def test_local_name_raises_when_the_blob_is_outside_the_prefix() -> None:
    with pytest.raises(azure_stage.RejectedNameError):
        azure_stage.local_name("other.csv", "orders/")


def test_mixed_case_tmp_suffix_cannot_clobber_a_publish_temp_file(tmp_path: Path) -> None:
    container = FakeContainer({"o/A.CSV.TMP": b"evil", "o/a.csv": b"good"})

    summary = azure_stage.stage(container, "o", "o/", tmp_path)

    assert summary == StageSummary(staged=1, failed=1)  # the .TMP blob is not silently dropped
    assert "read:o/A.CSV.TMP" not in container.calls
    assert _files(tmp_path / "o") == ["a.csv"]
    assert (tmp_path / "o" / "a.csv").read_bytes() == b"good"


def test_rejected_name_is_not_downloaded(tmp_path: Path) -> None:
    container = FakeContainer({"orders/sub/a.csv": b"x"})

    _run(container, tmp_path)

    assert not any(call.startswith("read:") for call in container.calls)


# ---------------------------------------------------------------------------
# Folder markers
# ---------------------------------------------------------------------------


def test_folder_marker_is_never_read_or_written(tmp_path: Path) -> None:
    container = FakeContainer({"orders/": b"", "orders/sub/": b""})

    summary, folder = _run(container, tmp_path)

    assert summary == StageSummary(ignored=2)
    assert not any(call.startswith("read:") for call in container.calls)
    assert not folder.exists()


def test_folder_markers_do_not_count_toward_the_blob_cap(tmp_path: Path) -> None:
    blobs = {"orders/": b"", "orders/x/": b"", "orders/a.csv": b"1", "orders/b.csv": b"2"}

    summary, _ = _run(FakeContainer(blobs), tmp_path, max_blobs=2)

    assert summary == StageSummary(staged=2, ignored=2, failed=0)


# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------


def test_blob_exactly_at_the_size_cap_is_staged(tmp_path: Path) -> None:
    summary, folder = _run(FakeContainer({"orders/a.csv": b"12345"}), tmp_path, max_blob_bytes=5)

    assert summary.staged == 1
    assert (folder / "a.csv").read_bytes() == b"12345"


def test_blob_one_byte_over_the_listed_cap_fails_without_reading(tmp_path: Path) -> None:
    container = FakeContainer({"orders/a.csv": b"123456"})

    summary, folder = _run(container, tmp_path, max_blob_bytes=5)

    assert summary.failed == 1
    assert container.calls == ["list:orders/"]
    assert not folder.exists()


def test_streamed_bytes_one_over_the_cap_fail_and_leave_no_file_or_temp(tmp_path: Path) -> None:
    container = LyingContainer({"orders/big.csv": b"123456"})

    summary, folder = _run(container, tmp_path, max_blob_bytes=5)

    assert summary == StageSummary(failed=1)
    assert _files(folder) == []


def test_streaming_stops_reading_once_the_cap_is_passed(tmp_path: Path) -> None:
    container = CountingStreamContainer({"orders/big.csv": b""}, listed_size=1)

    summary, folder = _run(container, tmp_path, max_blob_bytes=8)

    assert summary.failed == 1
    assert container.chunks_served < 10  # not the full 1000
    assert _files(folder) == []


def test_oversize_blob_does_not_stop_the_others(tmp_path: Path) -> None:
    container = FakeContainer({"orders/big.csv": b"x" * 10, "orders/ok.csv": b"ok"})

    summary, folder = _run(container, tmp_path, max_blob_bytes=5)

    assert (summary.staged, summary.failed) == (1, 1)
    assert _files(folder) == ["ok.csv"]


def test_exactly_max_blobs_is_not_a_failure(tmp_path: Path) -> None:
    container = FakeContainer({f"orders/{n}.csv": b"x" for n in "abc"})

    summary, _ = _run(container, tmp_path, max_blobs=3)

    assert summary == StageSummary(staged=3)


def test_more_than_max_blobs_stages_first_n_and_adds_one_failure(tmp_path: Path) -> None:
    container = FakeContainer({f"orders/{n}.csv": b"x" for n in "abcdef"})

    summary, folder = _run(container, tmp_path, max_blobs=2)

    assert summary == StageSummary(staged=2, failed=1)
    assert _files(folder) == ["a.csv", "b.csv"]
    assert "read:orders/c.csv" not in container.calls


def test_default_blob_cap_is_one_hundred(tmp_path: Path) -> None:
    container = FakeContainer({f"orders/{n:03}.csv": b"x" for n in range(101)})

    summary, folder = _run(container, tmp_path)

    assert summary == StageSummary(staged=100, failed=1)
    assert len(_files(folder)) == 100


def test_cli_cap_flags_are_honoured(tmp_path: Path, azure_env: None) -> None:
    container = FakeContainer({f"orders/{n}.csv": b"x" for n in "abc"})

    code = _main(["orders", "orders/", "--max-blobs", "1"], container, tmp_path)

    assert code == 1
    assert _files(tmp_path / "orders") == ["a.csv"]


def test_cli_max_blob_mib_flag_is_honoured(tmp_path: Path, azure_env: None) -> None:
    two_mib = b"x" * (2 * 1024 * 1024)
    container = FakeContainer({"orders/big.csv": two_mib})

    code = _main(["orders", "orders/", "--max-blob-mib", "1"], container, tmp_path)

    assert code == 1
    assert not (tmp_path / "orders" / "big.csv").exists()


@pytest.mark.parametrize("flag", ["--max-blobs", "--max-blob-mib"])
@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_cli_rejects_non_positive_caps_with_exit_two(
    tmp_path: Path, azure_env: None, flag: str, value: str
) -> None:
    with pytest.raises(SystemExit) as raised:
        _main(["orders", "orders/", flag, value], FakeContainer({}), tmp_path)

    assert raised.value.code == 2


def test_cli_without_arguments_exits_two(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as raised:
        azure_stage.main([], lambda a, c: FakeContainer({}), landing_root=tmp_path)

    assert raised.value.code == 2


# ---------------------------------------------------------------------------
# Collisions, identical skips, idempotence
# ---------------------------------------------------------------------------


def test_rerun_is_idempotent_and_does_not_touch_files(tmp_path: Path) -> None:
    container = FakeContainer({"orders/a.csv": b"id\n1\n", "orders/b.csv": b"id\n2\n"})
    _, folder = _run(container, tmp_path)
    before = {p.name: p.stat().st_ino for p in folder.iterdir()}

    summary, _ = _run(container, tmp_path)

    assert summary == StageSummary(skipped=2)
    assert {p.name: p.stat().st_ino for p in folder.iterdir()} == before
    assert (folder / "a.csv").read_bytes() == b"id\n1\n"


def test_rerun_after_collision_still_fails_and_never_overwrites(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "a.csv").write_bytes(b"local")
    container = FakeContainer({"orders/a.csv": b"remote"})

    first, _ = _run(container, tmp_path)
    second, _ = _run(container, tmp_path)

    assert first.failed == second.failed == 1
    assert (folder / "a.csv").read_bytes() == b"local"
    assert _files(folder) == ["a.csv"]


def test_collision_on_empty_blob_versus_non_empty_local_file(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "a.csv").write_bytes(b"data")

    summary, _ = _run(FakeContainer({"orders/a.csv": b""}), tmp_path)

    assert summary.failed == 1
    assert (folder / "a.csv").read_bytes() == b"data"


def test_empty_blob_identical_to_empty_local_file_is_skipped(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "a.csv").write_bytes(b"")

    summary, _ = _run(FakeContainer({"orders/a.csv": b""}), tmp_path)

    assert summary == StageSummary(skipped=1)


def test_directory_at_the_target_name_is_a_collision_and_stays(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    (folder / "a.csv").mkdir(parents=True)

    summary, _ = _run(FakeContainer({"orders/a.csv": b"x", "orders/b.csv": b"y"}), tmp_path)

    assert (summary.staged, summary.failed) == (1, 1)
    assert (folder / "a.csv").is_dir()
    assert _files(folder) == ["a.csv", "b.csv"]


def test_symlink_to_different_bytes_is_a_collision_and_target_is_untouched(
    tmp_path: Path,
) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    real = tmp_path / "real.csv"
    real.write_bytes(b"other")
    (folder / "a.csv").symlink_to(real)

    summary, _ = _run(FakeContainer({"orders/a.csv": b"remote"}), tmp_path)

    assert summary.failed == 1
    assert real.read_bytes() == b"other"
    assert (folder / "a.csv").is_symlink()


def test_dangling_symlink_at_the_target_is_a_collision(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "a.csv").symlink_to(tmp_path / "does-not-exist")

    summary, _ = _run(FakeContainer({"orders/a.csv": b"x"}), tmp_path)

    assert summary.failed == 1
    assert not (tmp_path / "does-not-exist").exists()


def test_symlink_is_not_followed_to_write_outside_the_folder(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    outside = tmp_path / "outside.csv"
    (folder / "a.csv").symlink_to(outside)

    _run(FakeContainer({"orders/a.csv": b"x"}), tmp_path)

    assert not outside.exists()


def test_landing_folder_path_that_is_a_file_fails_the_blob_and_keeps_the_file(
    tmp_path: Path,
) -> None:
    (tmp_path / "orders").write_bytes(b"i am a file")

    summary, _ = _run(FakeContainer({"orders/a.csv": b"x"}), tmp_path)

    assert summary == StageSummary(failed=1)
    assert (tmp_path / "orders").read_bytes() == b"i am a file"


def test_stale_temp_file_from_a_crash_does_not_block_staging(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "a.csv.tmp").write_bytes(b"stale partial")

    summary, _ = _run(FakeContainer({"orders/a.csv": b"fresh"}), tmp_path)

    assert summary.staged == 1
    assert (folder / "a.csv").read_bytes() == b"fresh"
    assert _files(folder) == ["a.csv", "a.csv.tmp"]  # the leftover is not this run's to remove


def test_staged_file_bytes_equal_blob_bytes_across_chunk_boundaries(tmp_path: Path) -> None:
    data = bytes(range(256)) * 3 + b"\x00\xff"

    _, folder = _run(FakeContainer({"orders/bin.dat": data}), tmp_path)

    assert (folder / "bin.dat").read_bytes() == data


# ---------------------------------------------------------------------------
# One failing blob does not stop the others
# ---------------------------------------------------------------------------


def test_read_error_mid_stream_fails_that_blob_and_leaves_no_partial_file(tmp_path: Path) -> None:
    container = OnlyOneFailsContainer({"orders/a.csv": b"1", "orders/b.csv": b"2"}, "orders/a.csv")

    summary, folder = _run(container, tmp_path)

    assert (summary.staged, summary.failed) == (1, 1)
    assert _files(folder) == ["b.csv"]


def test_connection_drop_after_first_chunk_leaves_no_partial_file(tmp_path: Path) -> None:
    summary, folder = _run(FailingReadContainer({"orders/a.csv": b"12345678"}), tmp_path)

    assert summary == StageSummary(failed=1)
    assert _files(folder) == []


def test_blobs_after_a_failing_one_are_still_staged(tmp_path: Path) -> None:
    blobs = {"orders/a.csv": b"1", "orders/sub/b.csv": b"2", "orders/c.csv": b"3"}

    summary, folder = _run(FakeContainer(blobs), tmp_path)

    assert (summary.staged, summary.failed) == (2, 1)
    assert _files(folder) == ["a.csv", "c.csv"]


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------


def test_main_exit_zero_when_all_staged(tmp_path: Path, azure_env: None) -> None:
    assert _main(["orders", "orders/"], FakeContainer({"orders/a.csv": b"x"}), tmp_path) == 0


def test_main_exit_zero_on_identical_rerun(tmp_path: Path, azure_env: None) -> None:
    container = FakeContainer({"orders/a.csv": b"x"})
    _main(["orders", "orders/"], container, tmp_path)

    assert _main(["orders", "orders/"], container, tmp_path) == 0


def test_main_exit_one_for_collision_even_when_others_staged(
    tmp_path: Path, azure_env: None
) -> None:
    (tmp_path / "orders").mkdir()
    (tmp_path / "orders" / "a.csv").write_bytes(b"local")
    container = FakeContainer({"orders/a.csv": b"remote", "orders/b.csv": b"ok"})

    assert _main(["orders", "orders/"], container, tmp_path) == 1
    assert (tmp_path / "orders" / "b.csv").read_bytes() == b"ok"


def test_main_exit_one_when_blob_count_cap_is_exceeded(tmp_path: Path, azure_env: None) -> None:
    container = FakeContainer({"orders/a.csv": b"1", "orders/b.csv": b"2"})

    assert _main(["orders", "orders/", "--max-blobs", "1"], container, tmp_path) == 1


@pytest.mark.parametrize("value", ["", "BAD_ACCOUNT", "ab", "UPPER", "a" * 25, "has space", "a/b"])
def test_invalid_account_exits_two_and_never_echoes_the_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    value: str,
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, value)
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, "testcontainer")
    caplog.set_level(logging.DEBUG)

    code = _main(["orders", "orders/"], AssertionError("must not be built"), tmp_path)

    captured = capsys.readouterr()
    assert code == 2
    if value:
        assert value not in caplog.text + captured.out + captured.err


@pytest.mark.parametrize(
    "value", ["BAD_CONTAINER", "-start", "end-", "UPPER", "x" * 64, "has space", "ab", "a--b"]
)
def test_invalid_container_exits_two_and_never_echoes_the_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    value: str,
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, "testaccount")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, value)
    caplog.set_level(logging.DEBUG)

    code = _main(["orders", "orders/"], AssertionError("must not be built"), tmp_path)

    captured = capsys.readouterr()
    assert code == 2
    assert value not in caplog.text + captured.out + captured.err


@pytest.mark.parametrize("value", ["a", "ab", "a--b", "my--container", "x" * 64])
def test_container_name_shorter_than_three_characters_is_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, "testaccount")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, value)
    built: list[str] = []

    def factory(account: str, container: str) -> FakeContainer:
        built.append(container)
        return FakeContainer({})

    code = azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path)

    assert code == 2
    assert built == []


@pytest.mark.parametrize("value", ["abc", "a-b", "a" * 63, "a-" + "b" * 59 + "-c", "1a2"])
def test_container_name_of_three_to_sixty_three_characters_is_accepted(value: str) -> None:
    settings = {azure_stage.ENV_ACCOUNT: "testaccount", azure_stage.ENV_CONTAINER: value}

    assert azure_stage._read_settings(settings) == ("testaccount", value)


def test_container_name_with_consecutive_hyphens_is_invalid() -> None:
    settings = {azure_stage.ENV_ACCOUNT: "testaccount", azure_stage.ENV_CONTAINER: "my--box"}

    with pytest.raises(ValueError, match="invalid setting"):
        azure_stage._read_settings(settings)


def test_invalid_env_value_with_a_secret_shape_is_never_logged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, f"https://x.example/?sig={SENTINEL_TOKEN}")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, "testcontainer")
    caplog.set_level(logging.DEBUG)

    code = _main(["orders", "orders/"], AssertionError("must not be built"), tmp_path)

    assert code == 2
    assert SENTINEL_TOKEN not in caplog.text
    assert "sig=" not in caplog.text


@pytest.mark.parametrize("missing", [azure_stage.ENV_ACCOUNT, azure_stage.ENV_CONTAINER])
def test_missing_env_var_is_named_in_the_log_and_exits_two(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    missing: str,
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, "testaccount")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, "testcontainer")
    monkeypatch.delenv(missing)
    caplog.set_level(logging.INFO)

    code = _main(["orders", "orders/"], AssertionError("must not be built"), tmp_path)

    assert code == 2
    assert missing in caplog.text


def test_empty_env_var_counts_as_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, "")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, "testcontainer")
    caplog.set_level(logging.INFO)

    assert _main(["orders", "orders/"], AssertionError("x"), tmp_path) == 2
    assert "missing setting" in caplog.text


def test_invalid_source_never_builds_a_client_or_writes(tmp_path: Path, azure_env: None) -> None:
    def factory(account: str, container: str) -> FakeContainer:
        raise AssertionError("client must not be built")

    code = azure_stage.main(["../evil", "orders/"], factory, landing_root=tmp_path)

    assert code == 2
    assert _files(tmp_path) == []


@pytest.mark.parametrize("prefix", ["/", "..", "orders/..", "/abs", "a\\b", "bad\nprefix"])
def test_invalid_prefix_never_builds_a_client(
    tmp_path: Path, azure_env: None, caplog: pytest.LogCaptureFixture, prefix: str
) -> None:
    def factory(account: str, container: str) -> FakeContainer:
        raise AssertionError("client must not be built")

    code = azure_stage.main(["orders", prefix], factory, landing_root=tmp_path)

    assert code == 2
    assert "InvalidPrefix" in caplog.text


def test_factory_error_exits_two_logging_the_type_only(
    tmp_path: Path, azure_env: None, caplog: pytest.LogCaptureFixture
) -> None:
    def factory(account: str, container: str) -> FakeContainer:
        raise PermissionError(f"https://testaccount.blob.core.windows.net/?sig={SENTINEL_TOKEN}")

    caplog.set_level(logging.DEBUG)

    code = azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path)

    assert code == 2
    assert "PermissionError" in caplog.text
    assert SENTINEL_TOKEN not in caplog.text
    assert "https://" not in caplog.text


def test_listing_error_exits_two_logging_the_type_only(
    tmp_path: Path, azure_env: None, caplog: pytest.LogCaptureFixture
) -> None:
    container = RaisingListingContainer(AzureError(f"AuthorizationFailure {SENTINEL_TOKEN}"))
    caplog.set_level(logging.DEBUG)

    code = _main(["orders", "orders/"], container, tmp_path)

    assert code == 2
    assert "AzureError" in caplog.text
    assert SENTINEL_TOKEN not in caplog.text


@pytest.mark.parametrize("error", [ClientAuthenticationError("x"), CredentialUnavailableError("x")])
def test_sign_in_failures_exit_two(tmp_path: Path, azure_env: None, error: Exception) -> None:
    def factory(account: str, container: str) -> FakeContainer:
        raise error

    assert azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path) == 2


def test_sign_in_failure_during_listing_exits_two(tmp_path: Path, azure_env: None) -> None:
    container = RaisingListingContainer(CredentialUnavailableError("x"))

    assert _main(["orders", "orders/"], container, tmp_path) == 2


def test_programming_error_in_the_factory_propagates(tmp_path: Path, azure_env: None) -> None:
    def factory(account: str, container: str) -> FakeContainer:
        raise RuntimeError("a bug, not an Azure failure")

    with pytest.raises(RuntimeError):
        azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path)


def test_programming_error_in_the_listing_propagates(tmp_path: Path, azure_env: None) -> None:
    container = RaisingListingContainer(KeyError("a bug"))

    with pytest.raises(KeyError):
        _main(["orders", "orders/"], container, tmp_path)


def test_listing_error_after_some_entries_stages_nothing(tmp_path: Path, azure_env: None) -> None:
    container = RaisingListingContainer(OSError("boom"), first=[BlobInfo("orders/a.csv", size=1)])

    code = _main(["orders", "orders/"], container, tmp_path)

    assert code == 2
    assert not (tmp_path / "orders").exists()


def test_stage_propagates_listing_errors(tmp_path: Path) -> None:
    container = RaisingListingContainer(OSError("boom"))

    with pytest.raises(OSError, match="boom"):
        azure_stage.stage(container, "orders", "orders/", tmp_path)


def test_main_does_not_catch_keyboard_interrupt(tmp_path: Path, azure_env: None) -> None:
    def factory(account: str, container: str) -> FakeContainer:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path)


def test_main_passes_env_values_to_the_container_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, "acctone")
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, "ctr-one")
    seen: list[tuple[str, str]] = []

    def factory(account: str, container: str) -> FakeContainer:
        seen.append((account, container))
        return FakeContainer({})

    azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path)

    assert seen == [("acctone", "ctr-one")]


# ---------------------------------------------------------------------------
# The real adapter against a fake ContainerClient
# ---------------------------------------------------------------------------


class FakeDownload:
    """Mimics `StorageStreamDownloader.chunks()`, a lazy generator."""

    def __init__(self, parts: list[bytes], log: list[str], name: str) -> None:
        self.parts = parts
        self.log = log
        self.name = name

    def chunks(self) -> Iterator[bytes]:
        for part in self.parts:
            self.log.append(f"chunk:{self.name}")
            yield part


class FakeContainerClient:
    """Allows exactly `walk_blobs` and `download_blob`; any other method raises."""

    def __init__(self, items: list[object], data: dict[str, list[bytes]] | None = None) -> None:
        self.items = items
        self.data = data or {}
        self.calls: list[str] = []
        self.walk_kwargs: list[dict[str, object]] = []

    def walk_blobs(self, **kwargs: object) -> Iterator[object]:
        self.calls.append("walk_blobs")
        self.walk_kwargs.append(kwargs)
        return iter(self.items)

    def download_blob(self, name: str) -> FakeDownload:
        self.calls.append(f"download_blob:{name}")
        return FakeDownload(self.data[name], self.calls, name)

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"adapter used a forbidden method: {name}")


def _blob_item(name: str, size: int | None) -> SimpleNamespace:
    return SimpleNamespace(name=name, size=size)


def test_adapter_maps_a_prefix_item_and_a_blob_item() -> None:
    client = FakeContainerClient(
        [BlobPrefix(prefix="orders/2024/", delimiter="/"), _blob_item("orders/a.csv", 12)]
    )

    listed = list(azure_stage.AzureContainer(client).list_blobs("orders/"))  # type: ignore[arg-type]

    assert listed == [
        BlobInfo("orders/2024/", is_prefix=True),
        BlobInfo("orders/a.csv", size=12),
    ]


def test_adapter_treats_a_missing_size_as_zero() -> None:
    client = FakeContainerClient([_blob_item("orders/a.csv", None)])

    listed = list(azure_stage.AzureContainer(client).list_blobs("orders/"))  # type: ignore[arg-type]

    assert listed == [BlobInfo("orders/a.csv", size=0)]


def test_adapter_lists_non_recursively_with_the_prefix_and_slash_delimiter() -> None:
    client = FakeContainerClient([])

    list(azure_stage.AzureContainer(client).list_blobs("orders/"))  # type: ignore[arg-type]

    assert client.walk_kwargs == [{"name_starts_with": "orders/", "delimiter": "/"}]


def test_adapter_read_streams_chunks_lazily_in_order() -> None:
    client = FakeContainerClient([], {"orders/a.csv": [b"ab", b"cd", b"ef"]})

    stream = azure_stage.AzureContainer(client).read_blob("orders/a.csv")  # type: ignore[arg-type]
    assert client.calls == []  # nothing happens until the stream is consumed
    first = next(stream)

    assert first == b"ab"
    assert client.calls == ["download_blob:orders/a.csv", "chunk:orders/a.csv"]
    assert list(stream) == [b"cd", b"ef"]


def test_adapter_end_to_end_with_stage_only_uses_list_and_download(tmp_path: Path) -> None:
    client = FakeContainerClient(
        [
            BlobPrefix(prefix="orders/sub/", delimiter="/"),
            _blob_item("orders/", 0),
            _blob_item("orders/a.csv", 8),
        ],
        {"orders/a.csv": [b"id\n", b"1\n"]},
    )

    summary = azure_stage.stage(
        azure_stage.AzureContainer(client),  # type: ignore[arg-type]
        "orders",
        "orders/",
        tmp_path,
    )

    assert summary == StageSummary(staged=1, ignored=2)
    assert (tmp_path / "orders" / "a.csv").read_bytes() == b"id\n1\n"
    assert {call.split(":")[0] for call in client.calls} <= {
        "walk_blobs",
        "download_blob",
        "chunk",
    }


class RecordingDownload:
    def __init__(self, parts: list[bytes], used: list[str]) -> None:
        self.parts = parts
        self.used = used

    def chunks(self) -> Iterator[bytes]:
        self.used.append("chunks")
        yield from self.parts

    def __getattr__(self, name: str) -> object:
        self.used.append(name)
        raise AssertionError(f"download used a method outside the allow-list: {name}")


class RecordingContainerClient:
    """Records the name of every method the adapter calls; any unknown attribute is recorded too."""

    def __init__(self, items: list[object], data: dict[str, list[bytes]]) -> None:
        self.items = items
        self.data = data
        self.used: list[str] = []

    def walk_blobs(self, **kwargs: object) -> Iterator[object]:
        self.used.append("walk_blobs")
        return iter(self.items)

    def download_blob(self, name: str) -> RecordingDownload:
        self.used.append("download_blob")
        return RecordingDownload(self.data[name], self.used)

    def __getattr__(self, name: str) -> object:
        self.used.append(name)
        raise AssertionError(f"adapter used a method outside the allow-list: {name}")


def test_adapter_only_uses_walk_blobs_download_blob_and_chunks(tmp_path: Path) -> None:
    client = RecordingContainerClient(
        [BlobPrefix(prefix="orders/sub/", delimiter="/"), _blob_item("orders/a.csv", 5)],
        {"orders/a.csv": [b"id\n", b"1\n"]},
    )

    summary = azure_stage.stage(
        azure_stage.AzureContainer(client),  # type: ignore[arg-type]
        "orders",
        "orders/",
        tmp_path,
    )

    assert summary.staged == 1
    assert set(client.used) == {"walk_blobs", "download_blob", "chunks"}


def test_open_container_builds_an_azure_cli_credential_and_never_the_default_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}
    closed: list[str] = []

    class FakeCredential:
        def close(self) -> None:
            closed.append("credential")

    class FakeService:
        def __init__(self, url: str, credential: object) -> None:
            seen["url"] = url
            seen["credential"] = credential

        def get_container_client(self, container: str) -> FakeContainerClient:
            seen["container"] = container
            return FakeContainerClient([])

        def close(self) -> None:
            closed.append("service")

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("DefaultAzureCredential must never be built")

    monkeypatch.setattr(azure_stage, "BlobServiceClient", FakeService)
    monkeypatch.setattr(azure_stage, "AzureCliCredential", FakeCredential)
    monkeypatch.setattr("azure.identity.DefaultAzureCredential", forbidden)
    assert not hasattr(azure_stage, "DefaultAzureCredential")

    result = azure_stage.open_container("testaccount", "testcontainer")

    assert isinstance(result, azure_stage.AzureContainer)
    assert isinstance(seen["credential"], FakeCredential)
    assert seen["url"] == "https://testaccount.blob.core.windows.net"
    assert seen["container"] == "testcontainer"
    assert "?" not in str(seen["url"])
    result.close()
    assert sorted(closed) == ["credential", "service"]


def test_main_closes_the_real_adapter_after_the_run(tmp_path: Path, azure_env: None) -> None:
    closed: list[str] = []
    client = FakeContainerClient([])

    def factory(account: str, container: str) -> azure_stage.AzureContainer:
        return azure_stage.AzureContainer(client, (lambda: closed.append("closed"),))  # type: ignore[arg-type]

    assert azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path) == 0
    assert closed == ["closed"]


def test_main_closes_the_real_adapter_when_listing_fails(tmp_path: Path, azure_env: None) -> None:
    closed: list[str] = []

    class BrokenClient(FakeContainerClient):
        def walk_blobs(self, **kwargs: object) -> Iterator[object]:
            raise AzureError("x")

    def factory(account: str, container: str) -> azure_stage.AzureContainer:
        return azure_stage.AzureContainer(
            BrokenClient([]),  # type: ignore[arg-type]
            (lambda: closed.append("closed"),),
        )

    assert azure_stage.main(["orders", "orders/"], factory, landing_root=tmp_path) == 2
    assert closed == ["closed"]


# ---------------------------------------------------------------------------
# Logging: nothing sensitive at any level
# ---------------------------------------------------------------------------


def _everything_logged(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> str:
    captured = capsys.readouterr()
    return "\n".join([caplog.text, captured.out, captured.err])


FORBIDDEN = (
    SENTINEL_ACCOUNT,
    SENTINEL_CONTAINER,
    SENTINEL_TOKEN,
    "SENTINEL-BLOB-BYTES",
    "sig=",
    "sv=",
    "https://",
    "http://",
    "AccountKey",
    "DefaultEndpointsProtocol",
    "Bearer",
)


def test_logs_hold_no_sensitive_value_over_a_mixed_run_at_debug_level(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, SENTINEL_ACCOUNT)
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, SENTINEL_CONTAINER)
    (tmp_path / "orders").mkdir()
    (tmp_path / "orders" / "clash.csv").write_bytes(b"different")
    container = FakeContainer(
        {
            "orders/ok.csv": SENTINEL_BLOB_BYTES,
            "orders/clash.csv": SENTINEL_BLOB_BYTES,
            "orders/sub/deep.csv": SENTINEL_BLOB_BYTES,
            "orders/big.csv": SENTINEL_BLOB_BYTES * 50_000,  # over 1 MiB
            "orders/": b"",
        }
    )
    container.chunk_size = 1 << 20  # whole blob in one chunk, so a logged chunk would show it
    caplog.set_level(logging.DEBUG)
    logging.getLogger("azure").setLevel(logging.DEBUG)

    first = _main(["orders", "orders/", "--max-blob-mib", "1"], container, tmp_path)
    second = _main(["orders", "orders/", "--max-blob-mib", "1"], container, tmp_path)

    text = _everything_logged(caplog, capsys)
    assert (first, second) == (1, 1)
    assert "event=azure_stage_write" in text
    assert "event=azure_stage_skip" in text
    for forbidden in FORBIDDEN:
        assert forbidden not in text


def test_sdk_logger_records_are_dropped_even_if_it_was_set_to_debug_before(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv(azure_stage.ENV_ACCOUNT, SENTINEL_ACCOUNT)
    monkeypatch.setenv(azure_stage.ENV_CONTAINER, SENTINEL_CONTAINER)
    sdk_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")

    class ChattyContainer(FakeContainer):
        def read_blob(self, name: str) -> Iterator[bytes]:
            sdk_logger.info("Request URL: 'https://%s.blob.core.windows.net/?sig=%s'", "x", "T")
            sdk_logger.debug("Authorization: Bearer %s", SENTINEL_TOKEN)
            yield from super().read_blob(name)

    caplog.set_level(logging.DEBUG)
    logging.getLogger("azure").setLevel(logging.DEBUG)

    _main(["orders", "orders/"], ChattyContainer({"orders/a.csv": b"x"}), tmp_path)

    assert "Request URL" not in caplog.text
    assert SENTINEL_TOKEN not in caplog.text
    assert logging.getLogger("azure").getEffectiveLevel() >= logging.WARNING


def test_failure_logs_carry_the_error_type_not_the_message(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    _run(FailingReadContainer({"orders/a.csv": b"12345678"}), tmp_path)

    assert 'error="ConnectionError"' in caplog.text
    assert SENTINEL_TOKEN not in caplog.text


def test_write_log_has_name_size_hash_and_no_content(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    container = FakeContainer({"orders/a.csv": SENTINEL_BLOB_BYTES})
    container.chunk_size = 1 << 20

    _run(container, tmp_path)

    digest = hashlib.sha256(SENTINEL_BLOB_BYTES).hexdigest()
    lines = [r.getMessage() for r in caplog.records if "azure_stage_write" in r.getMessage()]
    assert len(lines) == 1
    assert 'blob_name="orders/a.csv"' in lines[0]
    assert f"size={len(SENTINEL_BLOB_BYTES)}" in lines[0]
    assert f'file_hash="{digest}"' in lines[0]
    assert "SENTINEL-BLOB-BYTES" not in caplog.text


def test_log_lines_are_single_line_key_value_even_for_odd_blob_names(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    _run(FakeContainer({"orders/evil\nevent=forged.csv": b"x"}), tmp_path)

    for record in caplog.records:
        assert "\n" not in record.getMessage()
    assert not any(r.getMessage().startswith("event=forged") for r in caplog.records)


def test_run_summary_is_logged_once_with_counts(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    _run(FakeContainer({"orders/a.csv": b"1", "orders/": b""}), tmp_path)

    runs = [r.getMessage() for r in caplog.records if "azure_stage_run" in r.getMessage()]
    assert len(runs) == 1
    assert "staged=1" in runs[0]
    assert "ignored=1" in runs[0]
    assert "failed=0" in runs[0]


def test_error_events_are_logged_at_error_level(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    _run(FakeContainer({"orders/sub/a.csv": b"x"}), tmp_path)

    errors = [r for r in caplog.records if "azure_stage_error" in r.getMessage()]
    assert errors
    assert all(r.levelno == logging.ERROR for r in errors)


def test_module_never_reads_the_process_environment_beyond_its_two_settings() -> None:
    source = Path(azure_stage.__file__).read_text()

    assert "os.environ" in source
    assert "AZURE_STORAGE_KEY" not in source
    assert "CONNECTION_STRING" not in source
    assert "sas" not in source.lower().replace("base", "")


def test_staging_does_not_leave_temp_files_after_any_outcome(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "clash.csv").write_bytes(b"local")
    container = FakeContainer(
        {
            "orders/ok.csv": b"ok",
            "orders/clash.csv": b"remote",
            "orders/big.csv": b"x" * 20,
            "orders/sub/x.csv": b"x",
        }
    )

    _run(container, tmp_path, max_blob_bytes=10)

    assert not [name for name in os.listdir(folder) if name.endswith(".tmp")]


def test_write_and_skip_logs_report_the_bytes_received_not_the_listed_size(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    container = LyingContainer({"orders/big.csv": b"x" * 10})  # listed as 1 byte

    _run(container, tmp_path)
    _run(container, tmp_path)

    lines = [r.getMessage() for r in caplog.records if "azure_stage_write" in r.getMessage()]
    lines += [r.getMessage() for r in caplog.records if "azure_stage_skip" in r.getMessage()]
    assert len(lines) == 2
    assert all("size=10 " in line for line in lines)
    assert not any("size=1 " in line for line in lines)


# ---------------------------------------------------------------------------
# Total size cap
# ---------------------------------------------------------------------------


def test_total_cap_default_is_two_gib() -> None:
    assert azure_stage.DEFAULT_MAX_TOTAL_MIB * 1024 * 1024 == 2 * 1024**3


def test_total_exactly_at_the_cap_is_staged(tmp_path: Path) -> None:
    container = FakeContainer({"orders/a.csv": b"1234", "orders/b.csv": b"5678"})

    summary, folder = _run(container, tmp_path, max_total_bytes=8)

    assert summary == StageSummary(staged=2)
    assert _files(folder) == ["a.csv", "b.csv"]


def test_total_one_byte_over_the_cap_stops_before_that_blob(tmp_path: Path) -> None:
    container = FakeContainer({"orders/a.csv": b"1234", "orders/b.csv": b"56789"})

    summary, folder = _run(container, tmp_path, max_total_bytes=8)

    assert summary == StageSummary(staged=1, failed=1)
    assert _files(folder) == ["a.csv"]  # already staged blobs stay
    assert "read:orders/b.csv" not in container.calls


def test_total_cap_stop_skips_later_blobs_even_small_ones(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    container = FakeContainer(
        {"orders/a.csv": b"1234", "orders/b.csv": b"x" * 20, "orders/c.csv": b"1"}
    )

    summary, folder = _run(container, tmp_path, max_total_bytes=10)

    assert summary == StageSummary(staged=1, failed=1)
    assert _files(folder) == ["a.csv"]
    assert "read:orders/c.csv" not in container.calls
    errors = [r.getMessage() for r in caplog.records if 'error="TotalSizeCap"' in r.getMessage()]
    assert len(errors) == 1


def test_identical_skipped_bytes_count_toward_the_total(tmp_path: Path) -> None:
    first = FakeContainer({"orders/a.csv": b"1234"})
    _run(first, tmp_path)
    second = FakeContainer({"orders/a.csv": b"1234", "orders/b.csv": b"5678"})

    summary, folder = _run(second, tmp_path, max_total_bytes=7)

    assert summary == StageSummary(skipped=1, failed=1)
    assert _files(folder) == ["a.csv"]


def test_total_cap_is_enforced_on_streamed_bytes(tmp_path: Path) -> None:
    container = CountingStreamContainer({"orders/a.csv": b"", "orders/b.csv": b""}, listed_size=1)

    summary, folder = _run(container, tmp_path, max_total_bytes=8)

    assert summary.failed == 2  # the blob that overflowed, plus the extra total-cap failure
    assert summary.staged == 0
    assert container.chunks_served < 10
    assert _files(folder) == []


def test_cli_total_cap_flag_exits_one_and_keeps_staged_blobs(
    tmp_path: Path, azure_env: None
) -> None:
    one_mib = b"x" * (1024 * 1024)
    container = FakeContainer({"orders/a.csv": one_mib, "orders/b.csv": one_mib})

    code = _main(["orders", "orders/", "--max-total-mib", "1"], container, tmp_path)

    assert code == 1
    assert _files(tmp_path / "orders") == ["a.csv"]


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_cli_rejects_a_non_positive_total_cap(tmp_path: Path, azure_env: None, value: str) -> None:
    with pytest.raises(SystemExit) as raised:
        _main(["orders", "orders/", "--max-total-mib", value], FakeContainer({}), tmp_path)

    assert raised.value.code == 2


def test_help_documents_the_total_cap(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        azure_stage.main(["--help"])

    assert "--max-total-mib" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Interrupted runs and leftovers at the temp name
# ---------------------------------------------------------------------------

# A staging run that stops mid-download, after the first part has reached the disk.
_STOP_MID_DOWNLOAD = """
    import os, sys
    from pathlib import Path
    from ingestion import azure_stage
    from ingestion.azure_stage import BlobInfo

    class Stops:
        def list_blobs(self, prefix):
            yield BlobInfo("demo/events.ndjson", size=24)

        def read_blob(self, name):
            yield b'{"id": "1"}\\n{"id": "2"}\\n' * 20000
            os._exit(9)

    azure_stage.stage(Stops(), "demo", "demo/", Path(sys.argv[1]))
"""

# A staging run that stops right after the target is linked, before any cleanup.
_STOP_AFTER_LINK = """
    import os, sys
    from pathlib import Path
    from ingestion import azure_stage
    from ingestion.azure_stage import BlobInfo

    class One:
        def list_blobs(self, prefix):
            yield BlobInfo("demo/a.csv", size=8)

        def read_blob(self, name):
            yield b"id\\nORIG\\n"

    real_link = os.link

    def link_then_stop(source, target):
        real_link(source, target)
        os._exit(9)

    os.link = link_then_stop
    azure_stage.stage(One(), "demo", "demo/", Path(sys.argv[1]))
"""


def test_bronze_loads_nothing_from_a_staging_run_stopped_mid_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., int]
) -> None:
    monkeypatch.chdir(tmp_path)
    landing = tmp_path / "data" / "landing"
    assert run_python(_STOP_MID_DOWNLOAD, landing) == 9

    bronze.main(["demo"])

    lake = tmp_path / "lake"
    assert list(lake.glob("bronze/demo/ingest_date=*/*.parquet")) == []
    assert list(lake.glob("raw/demo/receive_date=*/*")) == []


def test_staging_after_a_run_stopped_after_linking_keeps_the_landing_file(
    tmp_path: Path, run_python: Callable[..., int]
) -> None:
    assert run_python(_STOP_AFTER_LINK, tmp_path) == 9
    target = tmp_path / "demo" / "a.csv"
    assert target.read_bytes() == b"id\nORIG\n"

    summary = azure_stage.stage(
        FakeContainer({"demo/a.csv": b"id\nNEW!\n"}), "demo", "demo/", tmp_path
    )

    assert target.read_bytes() == b"id\nORIG\n"
    assert summary == StageSummary(failed=1)


def test_staging_does_not_write_through_a_symlink_at_the_temp_name(tmp_path: Path) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"keep me")
    (folder / "a.csv.tmp").symlink_to(outside)

    _run(FakeContainer({"orders/a.csv": b"id\n1\n"}), tmp_path)

    assert outside.read_bytes() == b"keep me"
