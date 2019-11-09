
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.


from twisted.trial import unittest
from twisted.trial.unittest import SynchronousTestCase
from twisted.protocols import dict

paramString = b"\"This is a dqstring \\w\\i\\t\\h boring stuff like: \\\"\" and t\\hes\\\"e are a\\to\\ms"
goodparams = [b"This is a dqstring with boring stuff like: \"", b"and", b"thes\"e", b"are", b"atoms"]

class ParamTests(unittest.TestCase):
    def testParseParam(self):
        """Testing command response handling"""
        params = []
        rest = paramString
        while 1:
            (param, rest) = dict.parseParam(rest)
            if param == None:
                break
            params.append(param)
        self.assertEqual(params, goodparams)#, "DictClient.parseParam returns unexpected results")



class DictDeprecationTests(SynchronousTestCase):
    """
    L{twisted.protocols.dict} is deprecated.
    """
    def test_dictDeprecation(self):
        """
        L{twisted.protocols.dict} is deprecated since Twisted 17.9.0.
        """
        from twisted.protocols import dict
        dict

        warningsShown = self.flushWarnings([self.test_dictDeprecation])
        self.assertEqual(1, len(warningsShown))
        self.assertEqual(
            ("twisted.protocols.dict was deprecated in Twisted 17.9.0:"
             " There is no replacement for this module."),
            warningsShown[0]['message'])
