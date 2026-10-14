//! The tray icon: the Filesync cloud on the Homebase tile, its dot telling the state; on
//! macOS the cloud alone as a template image (the menu bar colours it). The PNGs are rendered
//! from the SVGs beside them (`assets/README.md`).

use crate::model::Look;

/// The app icon, for pages and the Linux launcher.
pub const APP_256: &[u8] = include_bytes!("../assets/app-icon-256.png");
/// The synced folder's icon: the open cloud (Linux, macOS), the tile (Windows).
pub const FOLDER: hbsync_app::shell::Icons<'static> = hbsync_app::shell::Icons {
    png: include_bytes!("../assets/folder-256.png"),
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

/// The macOS menu bar's template image, and the tile it shows instead when there is a problem.
#[cfg(any(target_os = "macos", test))]
const MENU_BAR: &[u8] = include_bytes!("../assets/menu-bar-36.png");
#[cfg(any(target_os = "macos", test))]
const MENU_BAR_PROBLEM: &[u8] = include_bytes!("../assets/tray-problem-36.png");

/// The tile with the dot in the state's colour (design manual: green; the cyan and magenta
/// accents; grey).
#[cfg(any(not(target_os = "macos"), test))]
fn tile(look: Look) -> &'static [u8] {
    match look {
        Look::UpToDate => include_bytes!("../assets/tray-synced-64.png"),
        Look::Busy => include_bytes!("../assets/tray-syncing-64.png"),
        Look::Quiet => include_bytes!("../assets/tray-paused-64.png"),
        Look::Attention => include_bytes!("../assets/tray-problem-64.png"),
    }
}

/// The tray's icon for `look`, and whether it is a template image (macOS).
pub fn tray(look: Look) -> Result<(Rgba, bool), png::DecodingError> {
    // The menu bar shows one colour; the state shows in the menu's first lines. A problem
    // still shows, on the tile with its magenta dot.
    #[cfg(target_os = "macos")]
    let icon = match look {
        Look::Attention => (decode(MENU_BAR_PROBLEM)?, false),
        _ => (decode(MENU_BAR)?, true),
    };
    #[cfg(not(target_os = "macos"))]
    let icon = (decode(tile(look))?, false);
    Ok(icon)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icons_decode() -> Result<(), png::DecodingError> {
        let at = |img: &Rgba, x: u32, y: u32| {
            let i = ((y * img.width + x) * 4) as usize;
            [img.pixels[i], img.pixels[i + 1], img.pixels[i + 2]]
        };
        for look in [Look::UpToDate, Look::Busy, Look::Quiet, Look::Attention] {
            let icon = decode(tile(look))?;
            assert_eq!((icon.width, icon.height), (64, 64));
            assert_eq!(icon.pixels.len(), 64 * 64 * 4);
        }
        // The dot (upper right) tells the state.
        let problem = decode(tile(Look::Attention))?;
        assert_eq!(at(&problem, 48, 18), [0xeb, 0x00, 0xeb]);
        assert_eq!(
            at(&decode(tile(Look::UpToDate))?, 48, 18),
            [0x00, 0x96, 0x6d]
        );
        assert_eq!(decode(APP_256)?.width, 256);
        assert_eq!(decode(FOLDER.png)?.width, 256);
        assert_eq!(decode(MENU_BAR)?.width, 36);
        assert_eq!(decode(MENU_BAR_PROBLEM)?.width, 36);
        Ok(())
    }
}
