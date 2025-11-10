"""
Finder module for identifying table relationships by data content.

This module provides functionality to identify similarities between tables based on actual data content 
rather than metadata, helping to find tables that are subsets, supersets or otherwise related.
"""

from column_matcher import ColumnContentMatcher

__all__ = ["ColumnContentMatcher"] 