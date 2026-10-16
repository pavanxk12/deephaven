"""The services must not depend on each other, so one breaking or being busy cannot slow or stop another.

This reads the import statements of every module (nothing is run) and fails if a package imports one it must not know about:

    core      knows nothing of the others
    data      master, candles, EOD                   may use core only
    series    the straddle                           may use core and data (it is built from candles)
    comms     the NATS service                       may use core only
    strategy  registry, book, bridge, algos          the registry may use core only; the rest may use core, comms, data
    ui        the console                            may use anything
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "algo"

ALLOWED = {
    "core": set(),
    "data": {"core"},
    "series": {"core", "data"},
    "comms": {"core"},
    "strategy": {"core", "comms", "data", "strategy"},
    "ui": {"core", "comms", "data", "series", "strategy", "ui"},
}
ONLY_CORE = {"strategy/registry.py", "comms/nats_service.py"}


def imported_packages(path: Path) -> set:
    found = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        names = []
        if isinstance(node, ast.ImportFrom) and node.module:
            names = [node.module]
        elif isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        for n in names:
            parts = n.split(".")
            if parts[0] == "algo" and len(parts) > 1:
                found.add(parts[1])
    return found


def modules():
    for p in sorted(ROOT.rglob("*.py")):
        rel = p.relative_to(ROOT).as_posix()
        if "/" in rel:
            yield rel.split("/")[0], rel, p


def test_no_package_imports_one_it_is_not_allowed_to_know_about():
    bad = []
    for package, rel, path in modules():
        if package not in ALLOWED:
            continue
        extra = imported_packages(path) - ALLOWED[package] - {package}
        if extra:
            bad.append(f"{rel} imports {sorted(extra)}")
    assert not bad, "\n".join(bad)


def test_the_registry_and_the_nats_service_know_only_the_core():
    for rel in ONLY_CORE:
        extra = imported_packages(ROOT / rel) - {"core"}
        assert not extra, f"{rel} imports {sorted(extra)}"


def test_the_data_services_never_mention_nats_the_registry_or_a_strategy():
    for package in ("data", "series"):
        for _, rel, path in (m for m in modules() if m[0] == package):
            text = path.read_text(encoding="utf-8")
            for word in ("comms", "nats_service", "registry", "algo.strategy"):
                assert word not in text.replace("`", ""), f"{rel} mentions {word}"


def test_each_service_finds_the_others_by_name_in_the_app_and_never_holds_them_for_good():
    """The bridge looks the NATS service up every time; the data plugins only read tables by name."""
    text = (ROOT / "strategy" / "bridge.py").read_text(encoding="utf-8")
    assert "self._app.get(self.nats_name)" in text
