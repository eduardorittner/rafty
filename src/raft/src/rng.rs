use rand::{SeedableRng, RngExt};
use rand::rngs::StdRng;
use std::sync::{Arc, Mutex};

pub trait RngProvider: Send + Sync + Clone + std::fmt::Debug + 'static {
    /// Generates a random u64 in the range [low, high).
    fn random_range(&mut self, low: u64, high: u64) -> u64;
}

#[derive(Clone, Debug, Default)]
pub struct DefaultRng;

impl RngProvider for DefaultRng {
    fn random_range(&mut self, low: u64, high: u64) -> u64 {
        rand::rng().random_range(low..high)
    }
}

#[derive(Clone)]
pub struct DeterministicRng {
    rng: Arc<Mutex<StdRng>>,
}

impl std::fmt::Debug for DeterministicRng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeterministicRng").finish()
    }
}

impl DeterministicRng {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        }
    }
}

impl RngProvider for DeterministicRng {
    fn random_range(&mut self, low: u64, high: u64) -> u64 {
        self.rng.lock().unwrap().random_range(low..high)
    }
}
