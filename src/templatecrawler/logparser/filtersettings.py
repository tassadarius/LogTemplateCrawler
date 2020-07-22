"""
Define filter rules as regexes.
"""
import re

filter_rules = [
    re.compile(r'^.{0,5}$'),        # Filter entries shorter than 6 characters e.g. "Done"
    re.compile(r'(.)\1{5,}')        # Filter lines with only one repeated character e.g. "----------------"
]