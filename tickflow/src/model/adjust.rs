use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjustType {
    /// 前复权 (default).
    #[default]
    Forward,
    /// 后复权.
    Backward,
    /// 前复权加法.
    ForwardAdditive,
    /// 后复权加法.
    BackwardAdditive,
    /// 不复权.
    None,
}

impl AdjustType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Backward => "backward",
            Self::ForwardAdditive => "forward_additive",
            Self::BackwardAdditive => "backward_additive",
            Self::None => "none",
        }
    }
}

impl fmt::Display for AdjustType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
