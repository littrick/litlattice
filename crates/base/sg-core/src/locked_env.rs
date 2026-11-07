#![forbid(unsafe_code)]

use sg_env::EnvReader;

/// The environment after `env.Lock()`. Services only see this type while
/// running, so they cannot register or re-read variables after startup.
#[derive(Debug)]
pub struct LockedEnv {
    inner: EnvReader,
}

impl LockedEnv {
    pub fn new(inner: EnvReader) -> Self {
        Self { inner }
    }

    /// Read a variable registered during `configure`. Unregistered names yield
    /// an empty string, matching registration-driven access only.
    pub fn value(&self, name: &str) -> String {
        self.inner.value(name)
    }

    pub fn process_name(&self) -> &str {
        self.inner.process_name()
    }

    pub fn help_string(&self) -> String {
        self.inner.help_string()
    }
}
