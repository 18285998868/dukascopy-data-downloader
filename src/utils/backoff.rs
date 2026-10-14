use backoff::ExponentialBackoff;

pub fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_backoff_has_max_elapsed() {
        let b = default_backoff();
        assert_eq!(b.max_elapsed_time, Some(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn default_backoff_has_initial_interval() {
        let b = default_backoff();
        assert!(b.initial_interval.as_millis() >= 100);
    }

    #[test]
    fn default_backoff_multiplication() {
        let b = default_backoff();
        assert!(b.multiplier > 1.0);
    }
}
