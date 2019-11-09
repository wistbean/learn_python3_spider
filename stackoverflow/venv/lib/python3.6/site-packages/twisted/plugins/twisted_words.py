# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

from zope.interface import provider

from twisted.plugin import IPlugin

from twisted.application.service import ServiceMaker
from twisted.words import iwords


NewTwistedWords = ServiceMaker(
    "New Twisted Words",
    "twisted.words.tap",
    "A modern words server",
    "words")

TwistedXMPPRouter = ServiceMaker(
    "XMPP Router",
    "twisted.words.xmpproutertap",
    "An XMPP Router server",
    "xmpp-router")



@provider(IPlugin, iwords.IProtocolPlugin)
class RelayChatInterface(object):

    name = 'irc'

    def getFactory(cls, realm, portal):
        from twisted.words import service
        return service.IRCFactory(realm, portal)
    getFactory = classmethod(getFactory)



@provider(IPlugin, iwords.IProtocolPlugin)
class PBChatInterface(object):

    name = 'pb'

    def getFactory(cls, realm, portal):
        from twisted.spread import pb
        return pb.PBServerFactory(portal, True)
    getFactory = classmethod(getFactory)

