use std::error::Error;
use std::ffi::CString;
use std::os::raw;
use std::path::PathBuf;

#[link(name = "zip")]
extern {
    fn zip_open(path: *const raw::c_char, flags: raw::c_int, errorp: *mut raw::c_int) -> *mut raw::c_void;
    fn zip_file_add(archive: *mut raw::c_void, name: *const raw::c_char, source: *mut raw::c_void, flags: raw::c_uint) -> raw::c_longlong;
    fn zip_close(archive: *mut raw::c_void) -> raw::c_int;
    fn zip_source_buffer_create(data: *const raw::c_void, len: raw::c_ulonglong, freep: raw::c_int, error: *mut raw::c_void) -> *mut raw::c_void;
    fn zip_set_file_compression(archive: *mut raw::c_void, index: raw::c_ulonglong, comp: raw::c_int, comp_flags: raw::c_uint) -> raw::c_int;
}

static ZIP_CREATE: i32 = 1;
static ZIP_TRUNCATE: i32 = 8;
static ZIP_CM_DEFLATE: i32 = 8;

#[derive(Debug)]
pub struct ZipError(&'static str);

impl std::fmt::Display for ZipError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Zip Error: `{}`", self.0)
    }
}

impl Error for ZipError {}

pub struct Zip {
    ptr: *mut raw::c_void,
    compression: u8,
}

impl Zip {
    pub fn new(path: PathBuf, compression: u8) -> Result<Self, ZipError> {
        let mut errorp = 0;
        unsafe {
            let cpath = CString::new(path.to_str().unwrap()).unwrap();
            let zip = zip_open(cpath.as_ptr(), ZIP_CREATE | ZIP_TRUNCATE, &mut errorp);
            if zip.is_null() {
                return Result::Err(ZipError("Could not open os file"));
            }
            Result::Ok(Self{ptr: zip, compression})
        }
    }

    pub fn file_add(&self, name: &str, data: &[u8]) -> Result<(), ZipError> {
        let mut error = vec![]; // Not used, but have to bring sth valid to the call
        unsafe {
            let source = zip_source_buffer_create(
                data.as_ptr() as *const raw::c_void,
                data.len() as u64,
                0,
                error.as_mut_ptr() as *mut raw::c_void,
            );
            if source.is_null() {
                return Result::Err(ZipError("Could not allocate memort for source"));
            }

            let cname = CString::new(name).unwrap();
            let idx = zip_file_add(self.ptr, cname.as_ptr(), source, 0);
            if idx < 0 {
                return Result::Err(ZipError("Could not allocate memory for file"));
            }
            zip_set_file_compression(self.ptr, idx as u64, ZIP_CM_DEFLATE, self.compression.into());
            Result::Ok(())
        }
    }

    pub fn close(&self) -> Result<(), ZipError> {
        unsafe {
            if zip_close(self.ptr) < 0 {
                return Result::Err(ZipError("Could not write archive"));
            }
        }
        Result::Ok(())
    }
}
