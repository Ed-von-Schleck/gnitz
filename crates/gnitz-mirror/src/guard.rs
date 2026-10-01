//! The poison guard.

use gnitz_core::MirrorError;

/// A `T` that is refused once it may be torn.
pub(crate) struct Guarded<T> {
    inner: T,
    poison: Option<String>,
}

impl<T> Guarded<T> {
    pub(crate) fn new(inner: T) -> Self {
        Guarded { inner, poison: None }
    }

    /// Run `f` on the state, unless it is poisoned. A panic in `f` poisons it
    /// before the unwind continues, and so does a [`MirrorError::Poisoned`] `f`
    /// returns.
    pub(crate) fn touching<R>(
        &mut self,
        what: &str,
        f: impl FnOnce(&mut T) -> Result<R, MirrorError>,
    ) -> Result<R, MirrorError> {
        if let Some(why) = &self.poison {
            return Err(MirrorError::Poisoned(why.clone()));
        }
        let inner = &mut self.inner;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(inner))) {
            Ok(done) => done.inspect_err(|e| {
                if let MirrorError::Poisoned(why) = e {
                    self.poison = Some(why.clone());
                }
            }),
            Err(payload) => {
                self.poison = Some(format!("panic while {what}; a store may be torn"));
                std::panic::resume_unwind(payload)
            }
        }
    }

    /// The message that poisoned the state, if any.
    pub(crate) fn poisoned(&self) -> Option<&str> {
        self.poison.as_deref()
    }

    /// The state, poisoned or not.
    pub(crate) fn even_if_poisoned(&self) -> &T {
        &self.inner
    }

    /// The state, poisoned or not.
    pub(crate) fn even_if_poisoned_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}
