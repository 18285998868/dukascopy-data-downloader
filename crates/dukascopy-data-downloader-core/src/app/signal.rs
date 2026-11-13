//! Termination-signal handling.

/// Wait for a signal that asks the process to stop.
///
/// Returns the name of the signal that arrived.
///
/// The OS handlers are installed when this function is *called*, not when
/// the returned future is first polled: `tokio::signal::unix::signal`
/// registers the handler at call time, while `tokio::signal::ctrl_c` would
/// only do so on the first poll. The distinction matters — the handler has
/// to be in place before the run lock is taken.
pub(super) fn termination_signal() -> impl Future<Output = &'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let interrupt = signal(SignalKind::interrupt());
        let terminate = signal(SignalKind::terminate());
        async move {
            match (interrupt, terminate) {
                (Ok(mut interrupt), Ok(mut terminate)) => {
                    tokio::select! {
                        _ = interrupt.recv() => "SIGINT",
                        _ = terminate.recv() => "SIGTERM",
                    }
                }
                (Ok(mut interrupt), Err(e)) => {
                    tracing::warn!(
                        error = %e,
                        "could not install the SIGTERM handler; Ctrl+C still works"
                    );
                    let _ = interrupt.recv().await;
                    "SIGINT"
                }
                (Err(e), _) => {
                    tracing::warn!(
                        error = %e,
                        "could not install the SIGINT handler; the run cannot be interrupted cleanly"
                    );
                    // Nothing to wait for: park forever so this branch of
                    // the outer select never wins.
                    std::future::pending::<&'static str>().await
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let ctrl_c = tokio::signal::ctrl_c();
        async move {
            let _ = ctrl_c.await;
            "SIGINT"
        }
    }
}
