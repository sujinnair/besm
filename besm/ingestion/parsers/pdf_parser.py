from __future__ import annotations

import io

import pdfplumber


class PDFParseError(Exception):
    pass


class ParsedPDF:
    __slots__ = ("raw_text", "publication_date", "issuing_authority")

    def __init__(
        self, raw_text: str, publication_date: str, issuing_authority: str
    ) -> None:
        self.raw_text = raw_text
        self.publication_date = publication_date
        self.issuing_authority = issuing_authority


def parse_pdf(data: bytes) -> ParsedPDF:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages_text = [p.extract_text() or "" for p in pdf.pages]
            raw_text = "\n".join(pages_text).strip()
            metadata = pdf.metadata or {}
    except Exception as exc:
        raise PDFParseError(f"Failed to open PDF: {exc}") from exc

    if not raw_text:
        raise PDFParseError("PDF yielded no extractable text")

    publication_date = (
        _coerce_str(metadata.get("CreationDate"))
        or _coerce_str(metadata.get("ModDate"))
        or ""
    )
    issuing_authority = (
        _coerce_str(metadata.get("Author"))
        or _coerce_str(metadata.get("Creator"))
        or ""
    )

    if not publication_date:
        raise PDFParseError("PDF missing publication_date in metadata")
    if not issuing_authority:
        raise PDFParseError("PDF missing issuing_authority in metadata")

    return ParsedPDF(
        raw_text=raw_text,
        publication_date=publication_date,
        issuing_authority=issuing_authority,
    )


def _coerce_str(value: object) -> str:
    return str(value).strip() if value is not None else ""
