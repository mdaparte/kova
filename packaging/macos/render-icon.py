#!/usr/bin/env python3
"""Draw the Kova app icon and the disk image background.

The mark is the same four rectangles as site/favicon.svg. Slabs stay
#ECEEEB and the core stays #C2461B, on #17191C. An .icns file has one
color, so the dark-mode swap in the SVG is not used here.
"""

import struct
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

SLAB = (0xEC, 0xEE, 0xEB, 255)
CORE = (0xC2, 0x46, 0x1B, 255)
INK = (0x17, 0x19, 0x1C, 255)

# PNG payloads inside an .icns. The second size of a pair is the @2x image.
ICNS = (
    (b"icp4", 16),
    (b"icp5", 32),
    (b"ic11", 32),
    (b"icp6", 64),
    (b"ic12", 64),
    (b"ic07", 128),
    (b"ic08", 256),
    (b"ic13", 256),
    (b"ic09", 512),
    (b"ic14", 512),
    (b"ic10", 1024),
)


def layout(size):
    """Integer cells so the bars stay sharp. Small sizes fill the canvas."""
    if size <= 64:
        unit = max(1, size // 16)
    else:
        unit = max(1, int(size * 0.70) // 16)
    margin = (size - 16 * unit) // 2
    return margin, unit


def draw_mark(size):
    image = Image.new("RGBA", (size, size), INK)
    pen = ImageDraw.Draw(image)
    margin, unit = layout(size)

    def block(x, y, w, h, color):
        left = margin + x * unit
        top = margin + y * unit
        pen.rectangle((left, top, left + w * unit - 1, top + h * unit - 1), fill=color)

    block(0, 0, 4, 16, SLAB)
    block(9, 0, 7, 4, SLAB)
    block(5, 6, 7, 4, CORE)
    block(9, 12, 7, 4, SLAB)
    return image


def png_bytes(image):
    from io import BytesIO

    buf = BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


def icns(images):
    chunks = []
    for kind, payload in images:
        chunks.append(kind + struct.pack(">I", 8 + len(payload)) + payload)
    body = b"".join(chunks)
    return b"icns" + struct.pack(">I", 8 + len(body)) + body


def background():
    width, height = 720, 420
    image = Image.new("RGBA", (width, height), INK)
    pen = ImageDraw.Draw(image)
    pen.rectangle((0, height - 8, width, height), fill=CORE)
    font_path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
    if font_path.is_file():
        font = ImageFont.truetype(str(font_path), 22)
        label = "Drag Kova to Applications"
        box = pen.textbbox((0, 0), label, font=font)
        text_w = box[2] - box[0]
        pen.text(((width - text_w) / 2, height - 56), label, fill=SLAB, font=font)
    return image


def main():
    here = Path(__file__).resolve().parent
    master = draw_mark(1024)
    master.save(here / "icon-1024.png")
    payloads = []
    for kind, size in ICNS:
        payloads.append((kind, png_bytes(draw_mark(size))))
    (here / "kova.icns").write_bytes(icns(payloads))
    background().save(here / "background.png")


if __name__ == "__main__":
    main()
