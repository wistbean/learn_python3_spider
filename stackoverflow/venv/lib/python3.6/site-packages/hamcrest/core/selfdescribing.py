__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class SelfDescribing(object):
    """The ability of an object to describe itself."""

    def describe_to(self, description):
        """Generates a description of the object.

        The description may be part of a description of a larger object of
        which this is just a component, so it should be worded appropriately.

        :param description: The description to be built or appended to.

        """
        raise NotImplementedError('describe_to')
