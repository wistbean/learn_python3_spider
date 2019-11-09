"""Matchers of collections."""
from __future__ import absolute_import
from .isdict_containing import has_entry
from .isdict_containingentries import has_entries
from .isdict_containingkey import has_key
from .isdict_containingvalue import has_value
from .isin import is_in
from .issequence_containing import has_item, has_items
from .issequence_containinginanyorder import contains_inanyorder
from .issequence_containinginorder import contains
from .issequence_onlycontaining import only_contains
from .is_empty import empty

__author__ = "Chris Rose"
__copyright__ = "Copyright 2013 hamcrest.org"
__license__ = "BSD, see License.txt"
