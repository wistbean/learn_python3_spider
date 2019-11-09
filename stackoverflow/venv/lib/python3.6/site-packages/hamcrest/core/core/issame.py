__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher


class IsSame(BaseMatcher):

    def __init__(self, object):
        self.object = object

    def _matches(self, item):
        return item is self.object

    def describe_to(self, description):
        description.append_text('same instance as ')            \
                   .append_text(hex(id(self.object)))           \
                   .append_text(' ')                            \
                   .append_description_of(self.object)

    def describe_mismatch(self, item, mismatch_description):
        mismatch_description.append_text('was ')
        if item is not None:
            mismatch_description.append_text(hex(id(item)))         \
                                .append_text(' ')
        mismatch_description.append_description_of(item)


def same_instance(obj):
    """Matches if evaluated object is the same instance as a given object.

    :param obj: The object to compare against as the expected value.

    This matcher invokes the ``is`` identity operator to determine if the
    evaluated object is the the same object as ``obj``.

    """
    return IsSame(obj)
