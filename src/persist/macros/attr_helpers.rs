#[macro_export]
#[doc(hidden)]
macro_rules! __persist_attr_contains_unique {
    () => {
        false
    };
    (unique $(, $($rest:tt)*)?) => {
        true
    };
    ($_head:tt $(, $($rest:tt)*)?) => {
        $crate::__persist_attr_contains_unique!($($($rest)*)?)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __persist_attr_contains_index {
    () => {
        false
    };
    (index $(, $($rest:tt)*)?) => {
        true
    };
    ($_head:tt $(, $($rest:tt)*)?) => {
        $crate::__persist_attr_contains_index!($($($rest)*)?)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __persist_field_is_unique {
    () => {
        false
    };
    (#[persist($($args:tt)*)] $($rest:tt)*) => {
        $crate::__persist_attr_contains_unique!($($args)*) || $crate::__persist_field_is_unique!($($rest)*)
    };
    (#[ $($_other:tt)* ] $($rest:tt)*) => {
        $crate::__persist_field_is_unique!($($rest)*)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __persist_field_is_indexed {
    () => {
        false
    };
    (#[persist($($args:tt)*)] $($rest:tt)*) => {
        $crate::__persist_attr_contains_index!($($args)*) || $crate::__persist_field_is_indexed!($($rest)*)
    };
    (#[ $($_other:tt)* ] $($rest:tt)*) => {
        $crate::__persist_field_is_indexed!($($rest)*)
    };
}
