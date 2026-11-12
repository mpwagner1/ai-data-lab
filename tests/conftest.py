import zipfile
from collections.abc import Callable
from io import BytesIO
from xml.sax.saxutils import escape

import pytest

_ODS_NS = (
    'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" '
    'xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" '
    'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"'
)
_ODS_MANIFEST = (
    '<?xml version="1.0"?><manifest:manifest '
    'xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.2">'
    '<manifest:file-entry manifest:full-path="/" '
    'manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/>'
    '<manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>'
    "</manifest:manifest>"
)


def _ods_cell(value: str | tuple[str, str]) -> str:
    """A string cell, or a typed cell given as (`float` or `date`, stored value)."""
    if isinstance(value, str):
        return (
            f'<table:table-cell office:value-type="string"><text:p>{escape(value)}</text:p>'
            "</table:table-cell>"
        )
    kind, stored = value
    attribute = "date-value" if kind == "date" else "value"
    return (
        f'<table:table-cell office:value-type="{kind}" office:{attribute}="{escape(stored)}">'
        f"<text:p>shown</text:p></table:table-cell>"
    )


def _build_ods(rows: list[list[str | tuple[str, str]]], sheet: str = "S") -> bytes:
    """A minimal single-sheet .ods (string or typed cells), standard library only."""
    body = "".join(
        "<table:table-row>" + "".join(_ods_cell(cell) for cell in row) + "</table:table-row>"
        for row in rows
    )
    content = (
        f'<?xml version="1.0" encoding="UTF-8"?><office:document-content {_ODS_NS} '
        'office:version="1.2"><office:body><office:spreadsheet>'
        f'<table:table table:name="{escape(sheet)}">{body}</table:table>'
        "</office:spreadsheet></office:body></office:document-content>"
    )
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "mimetype",
            "application/vnd.oasis.opendocument.spreadsheet",
            compress_type=zipfile.ZIP_STORED,
        )
        archive.writestr("content.xml", content)
        archive.writestr("META-INF/manifest.xml", _ODS_MANIFEST)
    return buffer.getvalue()


@pytest.fixture
def ods_bytes() -> Callable[..., bytes]:
    """Build the bytes of a minimal .ods from rows of strings (no new dependency)."""
    return _build_ods
