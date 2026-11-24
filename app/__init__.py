"""Context Trust Score app package.

This package contains the reusable production pieces behind the Databricks notebook.
The notebook remains a reference implementation for exploration and ad hoc runs,
while the app layer reuses the same scoring functions for scheduled and interactive use.
"""

from .config import AppConfig
from .scorer import ContextTrustScoreEngine

__all__ = ["AppConfig", "ContextTrustScoreEngine"]
