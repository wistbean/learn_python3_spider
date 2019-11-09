# Copyright 2018 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings

try:
    import snappy
    _HAVE_SNAPPY = True
except ImportError:
    # python-snappy isn't available.
    _HAVE_SNAPPY = False

try:
    import zlib
    _HAVE_ZLIB = True
except ImportError:
    # Python built without zlib support.
    _HAVE_ZLIB = False

try:
    from zstandard import ZstdCompressor, ZstdDecompressor
    _HAVE_ZSTD = True
except ImportError:
    _HAVE_ZSTD = False

from pymongo.monitoring import _SENSITIVE_COMMANDS

_SUPPORTED_COMPRESSORS = set(["snappy", "zlib", "zstd"])
_NO_COMPRESSION = set(['ismaster'])
_NO_COMPRESSION.update(_SENSITIVE_COMMANDS)


def validate_compressors(dummy, value):
    try:
        # `value` is string.
        compressors = value.split(",")
    except AttributeError:
        # `value` is an iterable.
        compressors = list(value)

    for compressor in compressors[:]:
        if compressor not in _SUPPORTED_COMPRESSORS:
            compressors.remove(compressor)
            warnings.warn("Unsupported compressor: %s" % (compressor,))
        elif compressor == "snappy" and not _HAVE_SNAPPY:
            compressors.remove(compressor)
            warnings.warn(
                "Wire protocol compression with snappy is not available. "
                "You must install the python-snappy module for snappy support.")
        elif compressor == "zlib" and not _HAVE_ZLIB:
            compressors.remove(compressor)
            warnings.warn(
                "Wire protocol compression with zlib is not available. "
                "The zlib module is not available.")
        elif compressor == "zstd" and not _HAVE_ZSTD:
            compressors.remove(compressor)
            warnings.warn(
                "Wire protocol compression with zstandard is not available. "
                "You must install the zstandard module for zstandard support.")
    return compressors


def validate_zlib_compression_level(option, value):
    try:
        level = int(value)
    except:
        raise TypeError("%s must be an integer, not %r." % (option, value))
    if level < -1 or level > 9:
        raise ValueError(
            "%s must be between -1 and 9, not %d." % (option, level))
    return level


class CompressionSettings(object):
    def __init__(self, compressors, zlib_compression_level):
        self.compressors = compressors
        self.zlib_compression_level = zlib_compression_level

    def get_compression_context(self, compressors):
        if compressors:
            chosen = compressors[0]
            if chosen == "snappy":
                return SnappyContext()
            elif chosen == "zlib":
                return ZlibContext(self.zlib_compression_level)
            elif chosen == "zstd":
                return ZstdContext()


def _zlib_no_compress(data):
    """Compress data with zlib level 0."""
    cobj = zlib.compressobj(0)
    return b"".join([cobj.compress(data), cobj.flush()])


class SnappyContext(object):
    compressor_id = 1

    @staticmethod
    def compress(data):
        return snappy.compress(data)


class ZlibContext(object):
    compressor_id = 2

    def __init__(self, level):
        # Jython zlib.compress doesn't support -1
        if level == -1:
            self.compress = zlib.compress
        # Jython zlib.compress also doesn't support 0
        elif level == 0:
            self.compress = _zlib_no_compress
        else:
            self.compress = lambda data: zlib.compress(data, level)


class ZstdContext(object):
    compressor_id = 3

    @staticmethod
    def compress(data):
        # ZstdCompressor is not thread safe.
        # TODO: Use a pool?
        return ZstdCompressor().compress(data)


def decompress(data, compressor_id):
    if compressor_id == SnappyContext.compressor_id:
        # python-snappy doesn't support the buffer interface.
        # https://github.com/andrix/python-snappy/issues/65
        # This only matters when data is a memoryview since
        # id(bytes(data)) == id(data) when data is a bytes.
        # NOTE: bytes(memoryview) returns the memoryview repr
        # in Python 2.7. The right thing to do in 2.7 is call
        # memoryview.tobytes(), but we currently only use
        # memoryview in Python 3.x.
        return snappy.uncompress(bytes(data))
    elif compressor_id == ZlibContext.compressor_id:
        return zlib.decompress(data)
    elif compressor_id == ZstdContext.compressor_id:
        # ZstdDecompressor is not thread safe.
        # TODO: Use a pool?
        return ZstdDecompressor().decompress(data)
    else:
        raise ValueError("Unknown compressorId %d" % (compressor_id,))
