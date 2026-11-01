#!/usr/bin/env python3
"""Wrap a raw disk image as an uncompressed-layout UDZO disk image.

macOS reads the koly trailer, then the blkx table inside the XML.
Each run is zlib-compressed. This is the Linux path. On a Mac the
packaging script uses hdiutil instead.
"""

import struct
import sys
import zlib

SECTOR = 512
CHUNK = 1024 * 1024
ZLIB = 0x80000005
END = 0xFFFFFFFF


def mish(sector_count, runs):
    # The blkx table is 0xCC bytes, then one 40-byte run per block.
    # The block count sits at 0xC8, after the unused checksum.
    out = bytearray(0xCC + 40 * len(runs))
    struct.pack_into(">II", out, 0, 0x6D697368, 1)
    struct.pack_into(">QQQ", out, 8, 0, sector_count, 0)
    struct.pack_into(">II", out, 0x20, CHUNK // SECTOR, 0xFFFFFFFF)
    struct.pack_into(">I", out, 0xC8, len(runs))
    offset = 0xCC
    for kind, start, count, comp_offset, length in runs:
        struct.pack_into(">IIQQQQ", out, offset, kind, 0, start, count, comp_offset, length)
        offset += 40
    return bytes(out)


def koly(data_length, xml_offset, xml_length, sector_count):
    out = bytearray(512)
    struct.pack_into(">4sIII", out, 0, b"koly", 4, 512, 1)
    struct.pack_into(">QQQQ", out, 0x10, 0, 0, data_length, 0)
    struct.pack_into(">Q", out, 0x30, 0)
    struct.pack_into(">II", out, 0x38, 1, 1)
    struct.pack_into(">II", out, 0x50, 0, 0)
    struct.pack_into(">QQ", out, 0xD8, xml_offset, xml_length)
    struct.pack_into(">II", out, 0x160, 0, 0)
    struct.pack_into(">I", out, 0x1E8, 2)
    struct.pack_into(">QII", out, 0x1EC, sector_count, 0, 0)
    return bytes(out)


def plist(table):
    import base64

    encoded = base64.b64encode(table).decode("ascii")
    lines = "\n".join("        " + encoded[i : i + 52] for i in range(0, len(encoded), 52))
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>resource-fork</key>
  <dict>
    <key>blkx</key>
    <array>
      <dict>
        <key>Attributes</key>
        <string>0x0050</string>
        <key>CFName</key>
        <string>whole disk</string>
        <key>Data</key>
        <data>
{lines}
        </data>
        <key>ID</key>
        <string>0</string>
        <key>Name</key>
        <string>whole disk</string>
      </dict>
    </array>
  </dict>
</dict>
</plist>
"""
    return xml.encode("utf-8")


def wrap(raw):
    if len(raw) % SECTOR:
        raw += b"\x00" * (SECTOR - (len(raw) % SECTOR))
    sector_count = len(raw) // SECTOR
    runs = []
    pieces = []
    comp_at = 0
    sector_at = 0
    pos = 0
    while pos < len(raw):
        piece = raw[pos : pos + CHUNK]
        compressed = zlib.compress(piece, 9)
        count = len(piece) // SECTOR
        runs.append((ZLIB, sector_at, count, comp_at, len(compressed)))
        pieces.append(compressed)
        comp_at += len(compressed)
        sector_at += count
        pos += CHUNK
    runs.append((END, sector_count, 0, comp_at, 0))
    data = b"".join(pieces)
    xml = plist(mish(sector_count, runs))
    trailer = koly(len(data), len(data), len(xml), sector_count)
    return data + xml + trailer


def main():
    if len(sys.argv) != 3:
        sys.stderr.write("usage: udif.py INPUT.img OUTPUT.dmg\n")
        return 1
    raw = open(sys.argv[1], "rb").read()
    open(sys.argv[2], "wb").write(wrap(raw))
    return 0


if __name__ == "__main__":
    sys.exit(main())
