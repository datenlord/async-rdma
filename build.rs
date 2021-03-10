fn main() {
    cc::Build::new()
        .no_default_flags(true)
        // .cpp(false)
        // .include("src/osxfuse/common") // for fuse_param.h etc
        // .include("src/osxfuse/example")
        // .include("src/osxfuse/include")
        // .include("/usr/include")
        // .define("FUSE_USE_VERSION", "26") // fuse version
        // .define("FUSERMOUNT_DIR", "\"/bin\"") // fusermount folder
        // .define("FUSERMOUNT_PROG", "\"fusermount\"") // override fusermount3
        // .define("_FILE_OFFSET_BITS", "64")
        // .define("_REENTRANT", None)
        // .define("HAVE_CONFIG_H", None)
        // .define("__APPLE__", None)
        // .define("__NetBSD__", None) // avoid include mntent.h
        // .define("SPECNAMELEN", "255") // max length of devicename
        // .opt_level(0)
        // .flag("-fdiagnostics-color=auto")
        // .flag("-fno-strict-aliasing")
        // .flag("-pipe") // avoid temporary files, speeding up builds
        // .flag("-pthread")
        // .flag("-c") // just compile
        .flag("-v") // verbose
        .flag("-g") // debug version binary
        .flag("-fPIE")
        // .flag("-MD")
        // .flag("-static")
        // .flag("-X c")
        // .flag("-std=c99")
        // .flag("-Winvalid-pch")
        // .flag("-Wmissing-declarations")
        // .flag("-Wno-sign-compare")
        // .flag("-Wno-unused-result")
        // .flag("-Wstrict-prototypes")
        // .flag("-Wwrite-strings")
        // .flag("-Wl,-framework,CoreFoundation") // mac framework
        // .flag("-Wl,-framework,CoreServices") // mac framework
        // .flag("-Wl,-framework,DiskArbitration") // mac framework
        // .flag("-Wl,-framework,IOKit") // mac framework
        // .flag("-Xlinker -liconv") // for iconv
        .warnings(true)
        .extra_warnings(true)
        .file("src/basic/client.c")
        .file("src/basic/server.c")
        .compile("basic");

    println!("cargo:rustc-link-lib=rdmacm");
    println!("cargo:rustc-link-lib=ibverbs");
}
