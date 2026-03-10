//! Minimal request/reply RPC inside one Selium guest module.
//! Startup proves the request path and response path both work before the module idles.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::DataValue;
use selium_guest::{io, shutdown, spawn};

#[allow(dead_code)]
mod bindings;

use bindings::{EchoRequest, EchoResponse};

const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

type ServiceTask = Pin<Box<dyn Future<Output = Result<()>> + 'static>>;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    // The client and server still show the request path and reply path explicitly, but now
    // they bind to contract-defined public endpoints instead of guest-created queues.
    let server = spawn_service("rpc server", run_server(bindings.clone()));

    let mut request_writer = io::managed_event_writer(&bindings, bindings::EVENT_ECHO_REQUESTED, 1)
        .await
        .context("attach request writer")?;
    let mut response_reader = io::managed_event_reader(&bindings, bindings::EVENT_ECHO_RESPONDED)
        .await
        .context("attach response reader")?;

    let request = EchoRequest {
        correlation_id: 7,
        body: "hello from selium".to_string(),
    };
    request_writer
        .send_typed(&request, SEND_TIMEOUT_MS)
        .await
        .context("send request")?;

    let response = response_reader
        .recv_typed::<EchoResponse>(RECV_TIMEOUT_MS)
        .await
        .context("receive response")?
        .ok_or_else(|| anyhow!("rpc response timed out"))?
        .payload;

    ensure!(
        response.correlation_id == request.correlation_id,
        "unexpected correlation id in response"
    );
    ensure!(
        response.echoed == request.body,
        "echo response body mismatch"
    );

    await_services_or_shutdown([server]).await
}

async fn run_server(bindings: DataValue) -> Result<()> {
    let mut request_reader = io::managed_event_reader(&bindings, bindings::EVENT_ECHO_REQUESTED)
        .await
        .context("attach request reader")?;
    let mut response_writer =
        io::managed_event_writer(&bindings, bindings::EVENT_ECHO_RESPONDED, 41)
            .await
            .context("attach response writer")?;

    loop {
        let Some(request) = request_reader
            .recv_typed::<EchoRequest>(RECV_TIMEOUT_MS)
            .await
            .context("receive request")?
        else {
            continue;
        };

        // The contract-generated types stay plain Rust structs at the guest I/O boundary.
        let response = EchoResponse {
            correlation_id: request.payload.correlation_id,
            echoed: request.payload.body,
        };

        response_writer
            .send_typed(&response, SEND_TIMEOUT_MS)
            .await
            .context("send response")?;
    }
}

fn spawn_service<F>(name: &'static str, future: F) -> ServiceTask
where
    F: Future<Output = Result<()>> + 'static,
{
    Box::pin(spawn(async move {
        future.await.with_context(|| format!("{name} failed"))
    }))
}

async fn await_services_or_shutdown<const N: usize>(mut services: [ServiceTask; N]) -> Result<()> {
    let mut shutdown = std::pin::pin!(shutdown());
    poll_fn(|cx| {
        for service in &mut services {
            if let Poll::Ready(result) = service.as_mut().poll(cx) {
                return Poll::Ready(result);
            }
        }
        if shutdown.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, future::pending, rc::Rc};

    use anyhow::anyhow;
    use selium_guest::{
        __reset_shutdown_for_tests, __signal_shutdown_for_tests, block_on, spawn, yield_now,
    };

    use super::{await_services_or_shutdown, spawn_service};

    #[test]
    fn await_services_or_shutdown_returns_ok_after_shutdown_signal() {
        __reset_shutdown_for_tests();
        let completed = Rc::new(Cell::new(false));
        let completed_ref = Rc::clone(&completed);
        let waiter = spawn(async move {
            let result = await_services_or_shutdown([spawn_service(
                "rpc server",
                pending::<anyhow::Result<()>>(),
            )])
            .await;
            completed_ref.set(true);
            result
        });

        let result = block_on(async {
            yield_now().await;
            assert!(
                !completed.get(),
                "service wait should still be pending before shutdown"
            );
            __signal_shutdown_for_tests();
            waiter.await
        });

        assert!(
            result.is_ok(),
            "shutdown should resolve cleanly: {result:#?}"
        );
        assert!(
            completed.get(),
            "service wait should complete after shutdown"
        );
        __reset_shutdown_for_tests();
    }

    #[test]
    fn await_services_or_shutdown_surfaces_service_failure() {
        __reset_shutdown_for_tests();

        let err = block_on(async {
            await_services_or_shutdown([spawn_service("rpc server", async {
                Err(anyhow!("boom"))
            })])
            .await
        })
        .expect_err("service failure should propagate");

        let message = format!("{err:#}");
        assert!(
            message.contains("rpc server failed"),
            "unexpected error: {message}"
        );
        assert!(message.contains("boom"), "unexpected error: {message}");

        __reset_shutdown_for_tests();
    }
}
