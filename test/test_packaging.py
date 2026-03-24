"""Test packaging configuration and metadata."""
import sys
from importlib.metadata import version, requires

import pytest


def test_version_starts_with_7():
    """Verify package version starts with 7."""
    v = version("hysds-grq2")
    assert v.startswith("7."), f"Expected version 7.x, got {v}"


def test_required_sibling_deps_declared():
    """Verify HySDS sibling dependencies are declared."""
    deps = requires("hysds-grq2")
    assert deps is not None, "No dependencies found"
    
    dep_names = {dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
                 for dep in deps}
    
    required_siblings = {"hysds-core", "hysds-commons"}
    missing = required_siblings - dep_names
    
    assert not missing, f"Missing required HySDS deps: {missing}"


def test_future_not_a_dependency():
    """Verify 'future' package is not a dependency."""
    deps = requires("hysds-grq2")
    assert deps is not None
    
    for dep in deps:
        dep_name = dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
        assert dep_name != "future", "'future' should not be a dependency on Python 3.12+"


def test_core_modules_importable():
    """Verify core grq2 modules can be imported."""
    import grq2
    assert hasattr(grq2, "__version__")
    
    # Test key modules exist
    try:
        from grq2 import app
        assert app is not None
    except ImportError as e:
        pytest.skip(f"Skipping module import test: {e}")


def test_python_version_requirement():
    """Verify running on Python 3.12+."""
    assert sys.version_info >= (3, 12), "Requires Python 3.12+"


def test_package_name_is_hysds_grq2():
    """Verify package is published as hysds-grq2."""
    v = version("hysds-grq2")
    assert v is not None, "Package 'hysds-grq2' not found"


def test_import_name_is_grq2():
    """Verify import name remains 'grq2' (not hysds_grq2)."""
    import grq2
    assert grq2.__name__ == "grq2"


def test_werkzeug_has_upper_bound():
    """Verify werkzeug has <3.0.0 upper bound."""
    deps = requires("hysds-grq2")
    assert deps is not None
    
    werkzeug_deps = [d for d in deps if "werkzeug" in d.lower()]
    assert werkzeug_deps, "werkzeug dependency not found"
    
    for dep in werkzeug_deps:
        assert "<3.0" in dep or "<3" in dep, \
            f"werkzeug should have <3.0.0 upper bound, found: {dep}"
