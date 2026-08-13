"""A tool's domain knowledge, loaded from the pack that ships inside its image.

Which workflows a tool supports, their preconditions, valid orderings, refusal
conditions and how to read a result are part of what the tool *is*, so they
version with it rather than being copied into every consuming project. See
docs/infra-cleanup-2026-08.md S-2b.

A pack is a directory of markdown files with a small `key: value` header:

    ---
    title: Equation of state
    description: Fit E(V) to get equilibrium volume and bulk modulus.
    requires_operation: relax
    ---

    # Equation of state
    ...

`requires_operation` is what makes startup reconciliation possible: a document
describing a workflow the running image cannot perform is not served at all, so
a consumer never has to weigh a document against an endpoint.

The header is parsed by hand rather than with PyYAML — the fields are flat, and
the thermocalc image has no YAML dependency to borrow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

_DELIMITER = "---"
_LIST_FIELDS = ("requires_backends",)


class KnowledgeDoc(NamedTuple):
    """One markdown document from a tool's knowledge pack."""

    slug: str
    title: str
    description: str
    requires_operation: str | None
    requires_backends: list[str]
    body: str


def _parse_header(text: str) -> tuple[dict[str, str], str]:
    """Split a leading `---` delimited `key: value` block from the body."""
    lines = text.splitlines()
    if not lines or lines[0].strip() != _DELIMITER:
        return {}, text

    header: dict[str, str] = {}
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == _DELIMITER:
            return header, "\n".join(lines[index + 1 :]).lstrip("\n")
        key, sep, value = line.partition(":")
        if sep:
            header[key.strip()] = value.strip()
    # Unterminated header: treat the whole file as body rather than silently
    # swallowing it as metadata.
    return {}, text


def load_knowledge(directory: Path) -> list[KnowledgeDoc]:
    """Read every markdown document in a knowledge pack, deepest path last.

    Args:
        directory: Pack root, typically `knowledge/` beside the instance's
            `server/`. A missing directory yields an empty pack — an instance
            without knowledge is valid, it just advertises no resources.

    Returns:
        Documents sorted by slug, so resource listings are stable between runs.
    """
    if not directory.is_dir():
        return []

    docs: list[KnowledgeDoc] = []
    for path in sorted(directory.rglob("*.md")):
        header, body = _parse_header(path.read_text(encoding="utf-8"))
        slug = path.relative_to(directory).with_suffix("").as_posix()
        backends = [b.strip() for b in header.get("requires_backends", "").split(",") if b.strip()]
        docs.append(
            KnowledgeDoc(
                slug=slug,
                title=header.get("title") or slug,
                description=header.get("description", ""),
                requires_operation=header.get("requires_operation") or None,
                requires_backends=backends,
                body=body,
            )
        )
    return docs


def reconcile(docs: Iterable[KnowledgeDoc], available_operations: dict[str, list[str]]) -> list[KnowledgeDoc]:
    """Drop documents describing work this image cannot actually perform.

    Args:
        docs: Everything in the pack.
        available_operations: Operation name → backends, from the live
            Capabilities descriptor.

    Returns:
        Only documents whose required operation is advertised and, where they
        name specific backends, whose backends are present. A pack entry for an
        unwired workflow is therefore inert rather than a lie — wiring the
        operation later turns it on with no client change.
    """
    kept = []
    for doc in docs:
        if doc.requires_operation is None:
            kept.append(doc)
            continue
        backends = available_operations.get(doc.requires_operation)
        if backends is None:
            continue
        if doc.requires_backends and not set(doc.requires_backends) & set(backends):
            continue
        kept.append(doc)
    return kept


def search(docs: Iterable[KnowledgeDoc], query: str, limit: int = 5) -> list[KnowledgeDoc]:
    """Rank documents against a query by naive term overlap.

    A lookup, not a decision — `search_workflows` exists so an agent can find
    the right document, while the planning it does with that document stays on
    the agent side (S-2b constraint 3).

    ponytail: substring counting, not TF-IDF. The pack is a few dozen documents
    read by a model that will fetch the full text anyway; add real ranking only
    if a pack grows past what that can carry.
    """
    terms = [t for t in query.lower().split() if t]
    if not terms:
        return []

    scored = []
    for doc in docs:
        haystack = f"{doc.slug} {doc.title} {doc.description} {doc.body}".lower()
        # Title and description count for more than body mentions: a document
        # *about* EOS should beat one that mentions it in passing.
        score = sum(haystack.count(term) + 5 * f"{doc.slug} {doc.title} {doc.description}".lower().count(term) for term in terms)
        if score:
            scored.append((score, doc))

    scored.sort(key=lambda pair: (-pair[0], pair[1].slug))
    return [doc for _, doc in scored[:limit]]
