"""Placeholder test to ensure pytest runs successfully."""


def test_placeholder():
    """Basic test to verify pytest setup works."""
    assert True


def test_repo_structure():
    """Verify basic repo structure exists."""
    import os
    
    repo_root = os.path.dirname(os.path.dirname(__file__))
    
    # Check key directories exist
    assert os.path.exists(os.path.join(repo_root, "frcs"))
    assert os.path.exists(os.path.join(repo_root, "adapters"))
    assert os.path.exists(os.path.join(repo_root, "scripts"))
    assert os.path.exists(os.path.join(repo_root, "tests"))
    
    # Check key files exist
    assert os.path.exists(os.path.join(repo_root, "frcs", "models.py"))
    assert os.path.exists(os.path.join(repo_root, "adapters", "madden_26.yaml"))
    assert os.path.exists(os.path.join(repo_root, "scripts", "build_players_index.py"))
    assert os.path.exists(os.path.join(repo_root, "pyproject.toml"))