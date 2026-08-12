#!/usr/bin/env python3
"""Regenerate favicons from favicon.svg using librsvg + pycairo."""

import os
import gi
gi.require_version('Rsvg', '2.0')
from gi.repository import Rsvg
import cairo
from PIL import Image

STATIC_DIR = os.path.dirname(os.path.abspath(__file__))
SVG_PATH = os.path.join(STATIC_DIR, 'favicon.svg')

svg = Rsvg.Handle.new_from_file(SVG_PATH)

for size in [32, 16]:
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, size, size)
    ctx = cairo.Context(surface)
    vp = Rsvg.Rectangle()
    vp.x, vp.y, vp.width, vp.height = 0, 0, size, size
    svg.render_document(ctx, vp)
    png_path = os.path.join(STATIC_DIR, f'favicon-{size}x{size}.png')
    surface.write_to_png(png_path)
    print(f'Written {png_path}')

img32 = Image.open(os.path.join(STATIC_DIR, 'favicon-32x32.png'))
img32.save(os.path.join(STATIC_DIR, 'favicon.ico'), format='ICO', sizes=[(16, 16), (32, 32)])
print('Written favicon.ico')

