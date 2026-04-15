"""
Pytest configuration and shared fixtures.
"""

import sys
from pathlib import Path

# Ensure the project root is on sys.path for all test modules
sys.path.insert(0, str(Path(__file__).parent.parent))
