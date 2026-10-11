//! The tray icon: the Homebase mark with a dot telling the state, or on macOS the glyph alone
//! as a template image (the menu bar colours it).

use crate::model::Look;

#[cfg(any(not(target_os = "macos"), test))]
const MARK: &[u8] = include_bytes!("../assets/mark-64.png");
#[cfg(target_os = "macos")]
const GLYPH: &[u8] = include_bytes!("../assets/glyph-36.png");
/// For pages and folder icons.
pub const MARK_256: &[u8] = include_bytes!("../assets/mark-256.png");
/// The synced folder's icon.
pub const FOLDER: hbsync_app::shell::Icons<'static> = hbsync_app::shell::Icons {
    png: MARK_256,
    ico: include_bytes!("../assets/homebase.ico"),
};

/// An image as RGBA rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rgba {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

pub fn decode(png_bytes: &[u8]) -> Result<Rgba, png::DecodingError> {
    let mut decoder = png::Decoder::new(std::io::Cursor::new(png_bytes));
    decoder.set_transformations(png::Transformations::normalize_to_color8());
    let mut reader = decoder.read_info()?;
    let mut pixels = vec![0; reader.output_buffer_size().unwrap_or(0)];
    let info = reader.next_frame(&mut pixels)?;
    pixels.truncate(info.buffer_size());
    if info.color_type == png::ColorType::Rgb {
        pixels = pixels
            .chunks(3)
            .flat_map(|p| [p[0], p[1], p[2], 255])
            .collect();
    }
    Ok(Rgba {
        width: info.width,
        height: info.height,
        pixels,
    })
}

/// The dot's colour (design manual: green, blue; grey; the magenta accent).
fn dot(look: Look) -> [u8; 3] {
    match look {
        Look::UpToDate => [0x00, 0x96, 0x6d],
        Look::Busy => [0x47, 0x6a, 0x92],
        Look::Quiet => [0x93, 0x93, 0x93],
        Look::Attention => [0xeb, 0x00, 0xeb],
    }
}

/// `image` with a dot in its bottom right corner: a third of its size, with a white ring so it
/// stands out on the mark and on any panel.
pub fn with_dot(image: &Rgba, look: Look) -> Rgba {
    let mut out = image.clone();
    let size = image.width.min(image.height) as f32;
    let r = size / 6.0;
    let ring = r + size / 24.0;
    let (cx, cy) = (image.width as f32 - ring, image.height as f32 - ring);
    let [red, green, blue] = dot(look);
    for y in 0..image.height {
        for x in 0..image.width {
            let d = ((x as f32 + 0.5 - cx).powi(2) + (y as f32 + 0.5 - cy).powi(2)).sqrt();
            let colour = if d <= r {
                [red, green, blue]
            } else if d <= ring {
                [255, 255, 255]
            } else {
                continue;
            };
            let i = ((y * image.width + x) * 4) as usize;
            out.pixels[i..i + 4].copy_from_slice(&[colour[0], colour[1], colour[2], 255]);
        }
    }
    out
}

/// The tray's icon for `look`, and whether it is a template image (macOS).
pub fn tray(look: Look) -> Result<(Rgba, bool), png::DecodingError> {
    // The menu bar shows monochrome glyphs; the state shows in the menu's first lines. A
    // problem still gets the dot, in colour.
    #[cfg(target_os = "macos")]
    let icon = match look {
        Look::Attention => (with_dot(&decode(GLYPH)?, look), false),
        _ => (decode(GLYPH)?, true),
    };
    #[cfg(not(target_os = "macos"))]
    let icon = (with_dot(&decode(MARK)?, look), false);
    Ok(icon)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icons_decode_and_get_their_dot() -> Result<(), png::DecodingError> {
        let mark = decode(MARK)?;
        assert_eq!((mark.width, mark.height), (64, 64));
        assert_eq!(mark.pixels.len(), 64 * 64 * 4);
        let dotted = with_dot(&mark, Look::Attention);
        // The corner's centre is magenta, the top left untouched.
        let at = |img: &Rgba, x: u32, y: u32| {
            let i = ((y * img.width + x) * 4) as usize;
            [img.pixels[i], img.pixels[i + 1], img.pixels[i + 2]]
        };
        let c = (64.0 - 64.0 / 6.0 - 64.0 / 24.0) as u32;
        assert_eq!(at(&dotted, c, c), [0xeb, 0x00, 0xeb]);
        assert_eq!(at(&dotted, 10, 10), at(&mark, 10, 10));
        assert_eq!(decode(MARK_256)?.width, 256);
        Ok(())
    }
}
