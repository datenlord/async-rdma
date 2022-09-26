/// Convert `BuilderError`s to `io::Error`.
/// Impl `From<OtherKindError>` for `io::Error`.
#[macro_export]
macro_rules! impl_into_io_error {
    ($x:ty) => {
        impl From<$x> for io::Error {
            #[inline]
            fn from(e: $x) -> Self {
                io::Error::new(io::ErrorKind::Other, e.to_string())
            }
        }
    };
}

/// Convert different `BuilderError`s.
/// Impl `From<XBuilderError>` for `YBuilderError`.
#[macro_export]
macro_rules! impl_from_buidler_error_for_another {
    ($x:ty ,$y:ty) => {
        impl From<$x> for $y {
            #[inline]
            fn from(e: $x) -> $y {
                <$y>::ValidationError(e.to_string())
            }
        }
    };
}

/// Sub builder setter methord helper.
/// Impl `From<XBuilderError>` for `YBuilderError`.
#[macro_export]
macro_rules! impl_sub_builder_setter {
    ($builder:ty ,$sub_builder:ty) => {
        impl $builder {
            /// Get sub builder's mutable reference
            pub(crate) fn address_handler(&mut self) -> &mut $sub_builder {
                &mut $builder.address_handler
            }
        }
    };
}
