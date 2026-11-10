use crate::protocol::{Error, Result};
use std::{
    ffi::{c_char, c_int, c_void, CStr},
    path::PathBuf,
};

#[repr(C)]
struct Image {
    filename: *const c_char,
    base: *mut c_void,
    symbol: *const c_char,
    address: *mut c_void,
}

extern "C" {
    fn dladdr(address: *const c_void, image: *mut Image) -> c_int;
}

pub(super) fn path() -> Result<PathBuf> {
    let mut image = Image {
        filename: std::ptr::null(),
        base: std::ptr::null_mut(),
        symbol: std::ptr::null(),
        address: std::ptr::null_mut(),
    };
    let found = unsafe {
        dladdr(
            crate::ffi::temporal_drasi_v1_abi_version as *const () as *const c_void,
            &mut image,
        )
    };
    if found == 0 || image.filename.is_null() {
        return Err(Error::new(3, "native library image could not be located"));
    }
    let filename = unsafe { CStr::from_ptr(image.filename) }
        .to_str()
        .map_err(|_| Error::new(3, "native library image path is not UTF-8"))?;
    let library = std::fs::canonicalize(filename)
        .map_err(|_| Error::new(3, "native library image path could not be resolved"))?;
    #[cfg(test)]
    if library.parent().and_then(std::path::Path::file_name) == Some(std::ffi::OsStr::new("deps")) {
        return super::worker::companion_for_library(
            &library
                .parent()
                .expect("deps")
                .parent()
                .expect("profile")
                .join("library"),
        )
        .map_err(|_| Error::new(3, "test plugin worker companion is missing"));
    }
    super::worker::companion_for_library(&library)
        .map_err(|_| Error::new(3, "native library plugin worker companion is missing"))
}
