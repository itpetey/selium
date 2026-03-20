use std::future::Future;

use crate::context::GuestContext;
use crate::error::GuestError;

pub type GuestResult<T, E = GuestError> = Result<T, E>;

pub trait Contract: Send + Sync + 'static {
    type Input: Clone + Send + 'static;
    type Output: Clone + Send + 'static;

    fn call_sync(&self, ctx: &GuestContext, input: Self::Input) -> GuestResult<Self::Output>;
}
