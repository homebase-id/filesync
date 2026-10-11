# Tray assets

From the Homebase design manual v2 (`Homebase Logos`):

- `mark.svg`: the mark (`homebase_org_rgb_2.svg`) with rounded corners, as the manual shows it,
  and without the glyph's hairline stroke. `mark-64.png` (tray on Windows and Linux) and
  `mark-256.png` (folder icon on macOS and Linux, the login page) are rendered from it.
- `glyph.svg`: the glyph alone, black on transparent; `glyph-36.png` is the macOS menu bar
  template image.
- `homebase.ico`: the manual's `icons/icon.ico`, the folder icon on Windows.

The PNGs were rendered with resvg 0.48 (`resvg::render` scaled to the size). Re-render them
when the SVGs change.
