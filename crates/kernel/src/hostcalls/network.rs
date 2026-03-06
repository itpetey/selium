//! Protocol-neutral network hostcall drivers.

use std::{convert::TryFrom, sync::Arc};

use selium_abi::{
    NetworkAccept, NetworkAcceptResult, NetworkClose, NetworkConnect, NetworkListenerDescriptor,
    NetworkRpcAccept, NetworkRpcAcceptResult, NetworkRpcAwait, NetworkRpcBodyRead,
    NetworkRpcBodyReadResult, NetworkRpcBodyReaderDescriptor, NetworkRpcBodyWrite,
    NetworkRpcBodyWriterDescriptor, NetworkRpcClientExchangeDescriptor,
    NetworkRpcExchangeDescriptor, NetworkRpcInvoke, NetworkRpcInvokeResult, NetworkRpcRespond,
    NetworkRpcRespondResult, NetworkRpcResponseResult, NetworkSessionDescriptor, NetworkStatus,
    NetworkStreamAccept, NetworkStreamDescriptor, NetworkStreamOpen, NetworkStreamRecv,
    NetworkStreamRecvResult, NetworkStreamResult, NetworkStreamSend,
};

use crate::{
    guest_error::{GuestError, GuestResult},
    registry::ResourceType,
    spi::network::{
        ListenerHandle, NetworkCapability, NetworkProcessPolicy, RpcBodyReaderHandle,
        RpcBodyWriterHandle, SessionHandle, StreamHandle,
    },
};

use super::{Contract, HostcallContext, Operation};

type NetworkOps<C> = (
    Arc<Operation<NetworkListenDriver<C>>>,
    Arc<Operation<NetworkCloseDriver<C>>>,
    Arc<Operation<NetworkConnectDriver<C>>>,
    Arc<Operation<NetworkAcceptDriver<C>>>,
    Arc<Operation<NetworkStreamOpenDriver<C>>>,
    Arc<Operation<NetworkStreamAcceptDriver<C>>>,
    Arc<Operation<NetworkStreamSendDriver<C>>>,
    Arc<Operation<NetworkStreamRecvDriver<C>>>,
    Arc<Operation<NetworkRpcInvokeDriver<C>>>,
    Arc<Operation<NetworkRpcAwaitDriver<C>>>,
    Arc<Operation<NetworkRpcRequestBodyWriteDriver<C>>>,
    Arc<Operation<NetworkRpcResponseBodyReadDriver<C>>>,
    Arc<Operation<NetworkRpcAcceptDriver<C>>>,
    Arc<Operation<NetworkRpcRespondDriver<C>>>,
    Arc<Operation<NetworkRpcRequestBodyReadDriver<C>>>,
    Arc<Operation<NetworkRpcResponseBodyWriteDriver<C>>>,
);

/// Hostcall driver that opens listeners.
pub struct NetworkListenDriver<C>(C);
/// Hostcall driver that closes network resources.
pub struct NetworkCloseDriver<C>(C);
/// Hostcall driver that dials outbound sessions.
pub struct NetworkConnectDriver<C>(C);
/// Hostcall driver that accepts inbound sessions.
pub struct NetworkAcceptDriver<C>(C);
/// Hostcall driver that opens outbound streams.
pub struct NetworkStreamOpenDriver<C>(C);
/// Hostcall driver that accepts inbound streams.
pub struct NetworkStreamAcceptDriver<C>(C);
/// Hostcall driver that writes stream bytes.
pub struct NetworkStreamSendDriver<C>(C);
/// Hostcall driver that reads stream bytes.
pub struct NetworkStreamRecvDriver<C>(C);
/// Hostcall driver that performs outbound RPC calls.
pub struct NetworkRpcInvokeDriver<C>(C);
/// Hostcall driver that awaits outbound RPC responses.
pub struct NetworkRpcAwaitDriver<C>(C);
/// Hostcall driver that writes outbound request body bytes.
pub struct NetworkRpcRequestBodyWriteDriver<C>(C);
/// Hostcall driver that reads outbound response body bytes.
pub struct NetworkRpcResponseBodyReadDriver<C>(C);
/// Hostcall driver that accepts inbound RPC requests.
pub struct NetworkRpcAcceptDriver<C>(C);
/// Hostcall driver that responds to inbound RPC requests.
pub struct NetworkRpcRespondDriver<C>(C);
/// Hostcall driver that reads inbound request body bytes.
pub struct NetworkRpcRequestBodyReadDriver<C>(C);
/// Hostcall driver that writes inbound response body bytes.
pub struct NetworkRpcResponseBodyWriteDriver<C>(C);

impl<C> Contract for NetworkListenDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = selium_abi::NetworkListen;
    type Output = NetworkListenerDescriptor;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let policy = context
            .registry()
            .extension::<NetworkProcessPolicy>()
            .unwrap_or_else(|| Arc::new(NetworkProcessPolicy::default()));

        async move {
            let listener = inner.listen(policy, input).await.map_err(Into::into)?;
            let slot = registrar
                .insert(listener.clone(), None, ResourceType::NetworkListener)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkListenerDescriptor {
                resource_id,
                protocol: listener.protocol,
                interactions: listener.interactions,
            })
        }
    }
}

impl<C> Contract for NetworkConnectDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkConnect;
    type Output = NetworkSessionDescriptor;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let policy = context
            .registry()
            .extension::<NetworkProcessPolicy>()
            .unwrap_or_else(|| Arc::new(NetworkProcessPolicy::default()));

        async move {
            let session = inner.connect(policy, input).await.map_err(Into::into)?;
            let slot = registrar
                .insert(session.clone(), None, ResourceType::NetworkSession)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkSessionDescriptor {
                resource_id,
                protocol: session.protocol,
                interactions: session.interactions,
            })
        }
    }
}

impl<C> Contract for NetworkAcceptDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkAccept;
    type Output = NetworkAcceptResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let listener = context
            .registry()
            .with::<ListenerHandle<C::Listener>, _>(
                usize::try_from(input.listener_id).unwrap_or(usize::MAX),
                |listener| listener.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let listener = listener?;
            let accepted = inner
                .accept(&listener.inner, input.timeout_ms)
                .await
                .map_err(Into::into)?;
            let Some(session) = accepted.session else {
                return Ok(NetworkAcceptResult::from(accepted));
            };

            let slot = registrar
                .insert(session.clone(), None, ResourceType::NetworkSession)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkAcceptResult {
                code: accepted.code,
                session: Some(NetworkSessionDescriptor {
                    resource_id,
                    protocol: session.protocol,
                    interactions: session.interactions,
                }),
            })
        }
    }
}

impl<C> Contract for NetworkStreamOpenDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkStreamOpen;
    type Output = NetworkStreamResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let session = context
            .registry()
            .with::<SessionHandle<C::Session>, _>(
                usize::try_from(input.session_id).unwrap_or(usize::MAX),
                |session| session.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let session = session?;
            let opened = inner
                .stream_open(&session.inner)
                .await
                .map_err(Into::into)?;
            let Some(stream) = opened.stream else {
                return Ok(NetworkStreamResult::from(opened));
            };
            let slot = registrar
                .insert(stream.clone(), None, ResourceType::NetworkStream)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkStreamResult {
                code: opened.code,
                stream: Some(NetworkStreamDescriptor { resource_id }),
            })
        }
    }
}

impl<C> Contract for NetworkStreamAcceptDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkStreamAccept;
    type Output = NetworkStreamResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let session = context
            .registry()
            .with::<SessionHandle<C::Session>, _>(
                usize::try_from(input.session_id).unwrap_or(usize::MAX),
                |session| session.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let session = session?;
            let accepted = inner
                .stream_accept(&session.inner, input.timeout_ms)
                .await
                .map_err(Into::into)?;
            let Some(stream) = accepted.stream else {
                return Ok(NetworkStreamResult::from(accepted));
            };
            let slot = registrar
                .insert(stream.clone(), None, ResourceType::NetworkStream)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkStreamResult {
                code: accepted.code,
                stream: Some(NetworkStreamDescriptor { resource_id }),
            })
        }
    }
}

impl<C> Contract for NetworkStreamSendDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkStreamSend;
    type Output = NetworkStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let stream = context
            .registry()
            .with::<StreamHandle<C::Stream>, _>(
                usize::try_from(input.stream_id).unwrap_or(usize::MAX),
                |stream| stream.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let stream = stream?;
            inner
                .stream_send(&stream.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for NetworkStreamRecvDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkStreamRecv;
    type Output = NetworkStreamRecvResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let stream = context
            .registry()
            .with::<StreamHandle<C::Stream>, _>(
                usize::try_from(input.stream_id).unwrap_or(usize::MAX),
                |stream| stream.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let stream = stream?;
            inner
                .stream_recv(&stream.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for NetworkRpcInvokeDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcInvoke;
    type Output = NetworkRpcInvokeResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let session = context
            .registry()
            .with::<SessionHandle<C::Session>, _>(
                usize::try_from(input.session_id).unwrap_or(usize::MAX),
                |session| session.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let session = session?;
            let started = inner
                .rpc_invoke(&session.inner, input)
                .await
                .map_err(Into::into)?;
            let Some(exchange) = started.exchange else {
                return Ok(NetworkRpcInvokeResult::from(started));
            };
            let request_body =
                started
                    .request_body
                    .ok_or(GuestError::Kernel(crate::KernelError::Driver(
                        "rpc invoke missing request body".to_string(),
                    )))?;
            let exchange_slot = registrar
                .insert(exchange, None, ResourceType::NetworkRpcClientExchange)
                .map_err(GuestError::from)?;
            let exchange_id =
                u32::try_from(exchange_slot).map_err(|_| GuestError::InvalidArgument)?;
            let body_slot = registrar
                .insert(
                    request_body.clone(),
                    None,
                    ResourceType::NetworkRpcBodyWriter,
                )
                .map_err(GuestError::from)?;
            let body_id = u32::try_from(body_slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkRpcInvokeResult {
                code: started.code,
                exchange: Some(NetworkRpcClientExchangeDescriptor {
                    resource_id: exchange_id,
                }),
                request_body: Some(NetworkRpcBodyWriterDescriptor {
                    resource_id: body_id,
                }),
            })
        }
    }
}

impl<C> Contract for NetworkRpcAwaitDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcAwait;
    type Output = NetworkRpcResponseResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let exchange = context
            .registry_mut()
            .remove::<C::RpcClientExchange>(
                usize::try_from(input.exchange_id).unwrap_or(usize::MAX),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let exchange = exchange?;
            let awaited = inner.rpc_await(exchange, input).await.map_err(Into::into)?;
            let Some(response_body) = awaited.response_body else {
                return Ok(NetworkRpcResponseResult::from(awaited));
            };
            let body_slot = registrar
                .insert(
                    response_body.clone(),
                    None,
                    ResourceType::NetworkRpcBodyReader,
                )
                .map_err(GuestError::from)?;
            let body_id = u32::try_from(body_slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkRpcResponseResult {
                code: awaited.code,
                response: awaited.response,
                response_body: Some(NetworkRpcBodyReaderDescriptor {
                    resource_id: body_id,
                }),
            })
        }
    }
}

impl<C> Contract for NetworkRpcRequestBodyWriteDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcBodyWrite;
    type Output = NetworkStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let body = context
            .registry()
            .with::<RpcBodyWriterHandle<C::RpcBodyWriter>, _>(
                usize::try_from(input.body_id).unwrap_or(usize::MAX),
                |body| body.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let body = body?;
            inner
                .rpc_body_write(&body.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for NetworkRpcResponseBodyReadDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcBodyRead;
    type Output = NetworkRpcBodyReadResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let body = context
            .registry()
            .with::<RpcBodyReaderHandle<C::RpcBodyReader>, _>(
                usize::try_from(input.body_id).unwrap_or(usize::MAX),
                |body| body.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let body = body?;
            inner
                .rpc_body_read(&body.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for NetworkRpcAcceptDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcAccept;
    type Output = NetworkRpcAcceptResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let session = context
            .registry()
            .with::<SessionHandle<C::Session>, _>(
                usize::try_from(input.session_id).unwrap_or(usize::MAX),
                |session| session.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let session = session?;
            let accepted = inner
                .rpc_accept(&session.inner, input.timeout_ms)
                .await
                .map_err(Into::into)?;
            let Some(exchange) = accepted.exchange else {
                return Ok(NetworkRpcAcceptResult::from(accepted));
            };
            let request =
                accepted
                    .request
                    .ok_or(GuestError::Kernel(crate::KernelError::Driver(
                        "rpc accept missing request".to_string(),
                    )))?;
            let slot = registrar
                .insert(exchange, None, ResourceType::NetworkRpcExchange)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            let request_body =
                accepted
                    .request_body
                    .ok_or(GuestError::Kernel(crate::KernelError::Driver(
                        "rpc accept missing request body".to_string(),
                    )))?;
            let body_slot = registrar
                .insert(
                    request_body.clone(),
                    None,
                    ResourceType::NetworkRpcBodyReader,
                )
                .map_err(GuestError::from)?;
            let body_id = u32::try_from(body_slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkRpcAcceptResult {
                code: accepted.code,
                exchange: Some(NetworkRpcExchangeDescriptor { resource_id }),
                request: Some(request),
                request_body: Some(NetworkRpcBodyReaderDescriptor {
                    resource_id: body_id,
                }),
            })
        }
    }
}

impl<C> Contract for NetworkRpcRespondDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcRespond;
    type Output = NetworkRpcRespondResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let exchange = context
            .registry_mut()
            .remove::<C::RpcExchange>(usize::try_from(input.exchange_id).unwrap_or(usize::MAX))
            .ok_or(GuestError::NotFound);

        async move {
            let exchange = exchange?;
            let responded = inner
                .rpc_respond(exchange, input)
                .await
                .map_err(Into::into)?;
            let Some(response_body) = responded.response_body else {
                return Ok(NetworkRpcRespondResult::from(responded));
            };
            let body_slot = registrar
                .insert(
                    response_body.clone(),
                    None,
                    ResourceType::NetworkRpcBodyWriter,
                )
                .map_err(GuestError::from)?;
            let body_id = u32::try_from(body_slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(NetworkRpcRespondResult {
                code: responded.code,
                response_body: Some(NetworkRpcBodyWriterDescriptor {
                    resource_id: body_id,
                }),
            })
        }
    }
}

impl<C> Contract for NetworkRpcRequestBodyReadDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcBodyRead;
    type Output = NetworkRpcBodyReadResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let body = context
            .registry()
            .with::<RpcBodyReaderHandle<C::RpcBodyReader>, _>(
                usize::try_from(input.body_id).unwrap_or(usize::MAX),
                |body| body.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let body = body?;
            inner
                .rpc_body_read(&body.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for NetworkRpcResponseBodyWriteDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkRpcBodyWrite;
    type Output = NetworkStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let body = context
            .registry()
            .with::<RpcBodyWriterHandle<C::RpcBodyWriter>, _>(
                usize::try_from(input.body_id).unwrap_or(usize::MAX),
                |body| body.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let body = body?;
            inner
                .rpc_body_write(&body.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for NetworkCloseDriver<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    type Input = NetworkClose;
    type Output = NetworkStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let slot = usize::try_from(input.resource_id).map_err(|_| GuestError::InvalidArgument);
        let meta = slot
            .as_ref()
            .ok()
            .and_then(|slot| context.registry().entry(*slot))
            .and_then(|id| context.registry().registry().metadata(id))
            .ok_or(GuestError::NotFound);

        let future = match (slot, meta) {
            (Ok(slot), Ok(meta)) => match meta.kind {
                ResourceType::NetworkListener => {
                    let listener = context
                        .registry_mut()
                        .remove::<ListenerHandle<C::Listener>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::Listener(listener)
                }
                ResourceType::NetworkSession => {
                    let session = context
                        .registry_mut()
                        .remove::<SessionHandle<C::Session>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::Session(session)
                }
                ResourceType::NetworkStream => {
                    let stream = context
                        .registry_mut()
                        .remove::<StreamHandle<C::Stream>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::Stream(stream)
                }
                ResourceType::NetworkRpcExchange => {
                    let exchange = context
                        .registry_mut()
                        .remove::<C::RpcExchange>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::Exchange(exchange)
                }
                ResourceType::NetworkRpcClientExchange => {
                    let exchange = context
                        .registry_mut()
                        .remove::<C::RpcClientExchange>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::ClientExchange(exchange)
                }
                ResourceType::NetworkRpcBodyReader => {
                    let body = context
                        .registry_mut()
                        .remove::<RpcBodyReaderHandle<C::RpcBodyReader>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::BodyReader(body)
                }
                ResourceType::NetworkRpcBodyWriter => {
                    let body = context
                        .registry_mut()
                        .remove::<RpcBodyWriterHandle<C::RpcBodyWriter>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::BodyWriter(body)
                }
                _ => EitherClose::<C>::Error(Err(GuestError::InvalidArgument)),
            },
            (Err(err), _) => EitherClose::<C>::Error(Err(err)),
            (_, Err(err)) => EitherClose::<C>::Error(Err(err)),
        };

        async move {
            match future {
                EitherClose::Listener(listener) => {
                    inner.close_listener(listener?).await.map_err(Into::into)
                }
                EitherClose::Session(session) => {
                    inner.close_session(session?).await.map_err(Into::into)
                }
                EitherClose::Stream(stream) => {
                    inner.close_stream(stream?).await.map_err(Into::into)
                }
                EitherClose::Exchange(exchange) => inner
                    .close_rpc_exchange(exchange?)
                    .await
                    .map_err(Into::into),
                EitherClose::ClientExchange(exchange) => inner
                    .close_rpc_client_exchange(exchange?)
                    .await
                    .map_err(Into::into),
                EitherClose::BodyReader(body) => {
                    inner.close_rpc_body_reader(body?).await.map_err(Into::into)
                }
                EitherClose::BodyWriter(body) => {
                    inner.close_rpc_body_writer(body?).await.map_err(Into::into)
                }
                EitherClose::Error(result) => result,
            }
        }
    }
}

enum EitherClose<C>
where
    C: NetworkCapability,
{
    Listener(GuestResult<ListenerHandle<C::Listener>>),
    Session(GuestResult<SessionHandle<C::Session>>),
    Stream(GuestResult<StreamHandle<C::Stream>>),
    Exchange(GuestResult<C::RpcExchange>),
    ClientExchange(GuestResult<C::RpcClientExchange>),
    BodyReader(GuestResult<RpcBodyReaderHandle<C::RpcBodyReader>>),
    BodyWriter(GuestResult<RpcBodyWriterHandle<C::RpcBodyWriter>>),
    Error(GuestResult<NetworkStatus>),
}

/// Build hostcall operations for protocol-neutral network management.
pub fn operations<C>(cap: C) -> NetworkOps<C>
where
    C: NetworkCapability + Clone + Send + Sync + 'static,
{
    (
        Operation::from_hostcall(
            NetworkListenDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_LISTEN),
        ),
        Operation::from_hostcall(
            NetworkCloseDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_CLOSE),
        ),
        Operation::from_hostcall(
            NetworkConnectDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_CONNECT),
        ),
        Operation::from_hostcall(
            NetworkAcceptDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_ACCEPT),
        ),
        Operation::from_hostcall(
            NetworkStreamOpenDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_STREAM_OPEN),
        ),
        Operation::from_hostcall(
            NetworkStreamAcceptDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_STREAM_ACCEPT),
        ),
        Operation::from_hostcall(
            NetworkStreamSendDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_STREAM_SEND),
        ),
        Operation::from_hostcall(
            NetworkStreamRecvDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_STREAM_RECV),
        ),
        Operation::from_hostcall(
            NetworkRpcInvokeDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_INVOKE),
        ),
        Operation::from_hostcall(
            NetworkRpcAwaitDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_AWAIT),
        ),
        Operation::from_hostcall(
            NetworkRpcRequestBodyWriteDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_REQUEST_BODY_WRITE),
        ),
        Operation::from_hostcall(
            NetworkRpcResponseBodyReadDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_RESPONSE_BODY_READ),
        ),
        Operation::from_hostcall(
            NetworkRpcAcceptDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_ACCEPT),
        ),
        Operation::from_hostcall(
            NetworkRpcRespondDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_RESPOND),
        ),
        Operation::from_hostcall(
            NetworkRpcRequestBodyReadDriver(cap.clone()),
            selium_abi::hostcall_contract!(NETWORK_RPC_REQUEST_BODY_READ),
        ),
        Operation::from_hostcall(
            NetworkRpcResponseBodyWriteDriver(cap),
            selium_abi::hostcall_contract!(NETWORK_RPC_RESPONSE_BODY_WRITE),
        ),
    )
}
