//! Protocol-neutral network hostcall drivers.

use std::{convert::TryFrom, sync::Arc};

use selium_abi::{
    Capability, NetworkAccept, NetworkAcceptResult, NetworkClose, NetworkConnect,
    NetworkListenerDescriptor, NetworkRpcAccept, NetworkRpcAcceptResult, NetworkRpcAwait,
    NetworkRpcBodyRead, NetworkRpcBodyReadResult, NetworkRpcBodyReaderDescriptor,
    NetworkRpcBodyWrite, NetworkRpcBodyWriterDescriptor, NetworkRpcClientExchangeDescriptor,
    NetworkRpcExchangeDescriptor, NetworkRpcInvoke, NetworkRpcInvokeResult, NetworkRpcRespond,
    NetworkRpcRespondResult, NetworkRpcResponseResult, NetworkSessionDescriptor, NetworkStatus,
    NetworkStreamAccept, NetworkStreamDescriptor, NetworkStreamOpen, NetworkStreamRecv,
    NetworkStreamRecvResult, NetworkStreamResult, NetworkStreamSend,
};

use crate::{
    guest_error::{GuestError, GuestResult},
    registry::{InstanceRegistrar, ResourceType},
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
        let authorisation =
            super::ensure_capability_authorised(context.registry(), Capability::NetworkLifecycle);
        let policy = context
            .registry()
            .extension::<NetworkProcessPolicy>()
            .unwrap_or_else(|| Arc::new(NetworkProcessPolicy::default()));

        async move {
            authorisation?;
            let listener = inner.listen(policy, input).await.map_err(Into::into)?;
            let slot = registrar
                .insert(listener.clone(), None, ResourceType::NetworkListener)
                .map_err(GuestError::from)?;
            grant_scoped_network_resource(
                &registrar,
                slot,
                &[Capability::NetworkLifecycle, Capability::NetworkAccept],
            )?;
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
        let authorisation =
            super::ensure_capability_authorised(context.registry(), Capability::NetworkConnect);
        let policy = context
            .registry()
            .extension::<NetworkProcessPolicy>()
            .unwrap_or_else(|| Arc::new(NetworkProcessPolicy::default()));

        async move {
            authorisation?;
            let session = inner.connect(policy, input).await.map_err(Into::into)?;
            let slot = registrar
                .insert(session.clone(), None, ResourceType::NetworkSession)
                .map_err(GuestError::from)?;
            grant_scoped_network_resource(
                &registrar,
                slot,
                &[
                    Capability::NetworkConnect,
                    Capability::NetworkAccept,
                    Capability::NetworkStreamWrite,
                    Capability::NetworkRpcClient,
                    Capability::NetworkRpcServer,
                ],
            )?;
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
        let listener = (|| -> GuestResult<ListenerHandle<C::Listener>> {
            let slot =
                usize::try_from(input.listener_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkAccept, slot)?;
            context
                .registry()
                .with::<ListenerHandle<C::Listener>, _>(slot, |listener| listener.clone())
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(
                &registrar,
                slot,
                &[
                    Capability::NetworkConnect,
                    Capability::NetworkAccept,
                    Capability::NetworkStreamWrite,
                    Capability::NetworkRpcClient,
                    Capability::NetworkRpcServer,
                ],
            )?;
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
        let session = (|| -> GuestResult<SessionHandle<C::Session>> {
            let slot =
                usize::try_from(input.session_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::NetworkStreamWrite,
                slot,
            )?;
            context
                .registry()
                .with::<SessionHandle<C::Session>, _>(slot, |session| session.clone())
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(
                &registrar,
                slot,
                &[
                    Capability::NetworkAccept,
                    Capability::NetworkStreamRead,
                    Capability::NetworkStreamWrite,
                ],
            )?;
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
        let session = (|| -> GuestResult<SessionHandle<C::Session>> {
            let slot =
                usize::try_from(input.session_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkAccept, slot)?;
            context
                .registry()
                .with::<SessionHandle<C::Session>, _>(slot, |session| session.clone())
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(
                &registrar,
                slot,
                &[
                    Capability::NetworkAccept,
                    Capability::NetworkStreamRead,
                    Capability::NetworkStreamWrite,
                ],
            )?;
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
        let stream = (|| -> GuestResult<StreamHandle<C::Stream>> {
            let slot = usize::try_from(input.stream_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::NetworkStreamWrite,
                slot,
            )?;
            context
                .registry()
                .with::<StreamHandle<C::Stream>, _>(slot, |stream| stream.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let stream = (|| -> GuestResult<StreamHandle<C::Stream>> {
            let slot = usize::try_from(input.stream_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkStreamRead, slot)?;
            context
                .registry()
                .with::<StreamHandle<C::Stream>, _>(slot, |stream| stream.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let session = (|| -> GuestResult<SessionHandle<C::Session>> {
            let slot =
                usize::try_from(input.session_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcClient, slot)?;
            context
                .registry()
                .with::<SessionHandle<C::Session>, _>(slot, |session| session.clone())
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(
                &registrar,
                exchange_slot,
                &[Capability::NetworkRpcClient],
            )?;
            let exchange_id =
                u32::try_from(exchange_slot).map_err(|_| GuestError::InvalidArgument)?;
            let body_slot = registrar
                .insert(
                    request_body.clone(),
                    None,
                    ResourceType::NetworkRpcBodyWriter,
                )
                .map_err(GuestError::from)?;
            grant_scoped_network_resource(&registrar, body_slot, &[Capability::NetworkRpcClient])?;
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
        let exchange = (|| -> GuestResult<C::RpcClientExchange> {
            let slot =
                usize::try_from(input.exchange_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcClient, slot)?;
            context
                .registry_mut()
                .remove::<C::RpcClientExchange>(slot)
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(&registrar, body_slot, &[Capability::NetworkRpcClient])?;
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
        let body = (|| -> GuestResult<RpcBodyWriterHandle<C::RpcBodyWriter>> {
            let slot = usize::try_from(input.body_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcClient, slot)?;
            context
                .registry()
                .with::<RpcBodyWriterHandle<C::RpcBodyWriter>, _>(slot, |body| body.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let body = (|| -> GuestResult<RpcBodyReaderHandle<C::RpcBodyReader>> {
            let slot = usize::try_from(input.body_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcClient, slot)?;
            context
                .registry()
                .with::<RpcBodyReaderHandle<C::RpcBodyReader>, _>(slot, |body| body.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let session = (|| -> GuestResult<SessionHandle<C::Session>> {
            let slot =
                usize::try_from(input.session_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcServer, slot)?;
            context
                .registry()
                .with::<SessionHandle<C::Session>, _>(slot, |session| session.clone())
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(&registrar, slot, &[Capability::NetworkRpcServer])?;
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
            grant_scoped_network_resource(&registrar, body_slot, &[Capability::NetworkRpcServer])?;
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
        let exchange = (|| -> GuestResult<C::RpcExchange> {
            let slot =
                usize::try_from(input.exchange_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcServer, slot)?;
            context
                .registry_mut()
                .remove::<C::RpcExchange>(slot)
                .ok_or(GuestError::NotFound)
        })();

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
            grant_scoped_network_resource(&registrar, body_slot, &[Capability::NetworkRpcServer])?;
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
        let body = (|| -> GuestResult<RpcBodyReaderHandle<C::RpcBodyReader>> {
            let slot = usize::try_from(input.body_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcServer, slot)?;
            context
                .registry()
                .with::<RpcBodyReaderHandle<C::RpcBodyReader>, _>(slot, |body| body.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let body = (|| -> GuestResult<RpcBodyWriterHandle<C::RpcBodyWriter>> {
            let slot = usize::try_from(input.body_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::NetworkRpcServer, slot)?;
            context
                .registry()
                .with::<RpcBodyWriterHandle<C::RpcBodyWriter>, _>(slot, |body| body.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let future = match slot {
            Ok(slot) => {
                let meta = context
                    .registry()
                    .entry(slot)
                    .and_then(|id| context.registry().registry().metadata(id))
                    .ok_or(GuestError::NotFound);

                match meta {
                    Ok(meta) => {
                        if let Err(err) =
                            authorise_network_close(context.registry(), slot, meta.kind)
                        {
                            EitherClose::<C>::Error(Err(err))
                        } else {
                            match meta.kind {
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
                            }
                        }
                    }
                    Err(err) => EitherClose::<C>::Error(Err(err)),
                }
            }
            Err(err) => EitherClose::<C>::Error(Err(err)),
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

fn authorise_network_close(
    registry: &crate::registry::InstanceRegistry,
    slot: usize,
    kind: ResourceType,
) -> GuestResult<()> {
    let capabilities = match kind {
        ResourceType::NetworkListener => &[Capability::NetworkLifecycle][..],
        ResourceType::NetworkSession => &[
            Capability::NetworkConnect,
            Capability::NetworkAccept,
            Capability::NetworkStreamWrite,
            Capability::NetworkRpcClient,
            Capability::NetworkRpcServer,
        ],
        ResourceType::NetworkStream => &[
            Capability::NetworkAccept,
            Capability::NetworkStreamRead,
            Capability::NetworkStreamWrite,
        ],
        ResourceType::NetworkRpcExchange => &[Capability::NetworkRpcServer][..],
        ResourceType::NetworkRpcClientExchange => &[Capability::NetworkRpcClient][..],
        ResourceType::NetworkRpcBodyReader | ResourceType::NetworkRpcBodyWriter => {
            &[Capability::NetworkRpcClient, Capability::NetworkRpcServer]
        }
        _ => return Err(GuestError::InvalidArgument),
    };
    super::ensure_slot_authorised_any(registry, capabilities, slot).map(|_| ())
}

fn grant_scoped_network_resource(
    registrar: &InstanceRegistrar,
    slot: usize,
    capabilities: &[Capability],
) -> GuestResult<()> {
    let resource_id = registrar.entry(slot).ok_or(GuestError::NotFound)?;
    registrar
        .grant_root_session_resources(capabilities, resource_id)
        .map_err(GuestError::from)
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::HashMap, sync::Mutex};

    use selium_abi::{
        InteractionKind, NetworkConnect, NetworkListen, NetworkProtocol, NetworkRpcBodyWrite,
        NetworkRpcInvoke, NetworkRpcRequestHead, NetworkStatusCode, NetworkStreamOpen,
        NetworkStreamSend, PrincipalKind, PrincipalRef,
    };

    use crate::{
        registry::Registry,
        services::session_service::{ResourceScope, RootSession, Session, SessionAuthnMethod},
        spi::network::{
            AcceptedRpc, AcceptedSession, AcceptedStream, AwaitedRpc, ListenerHandle,
            NetworkFuture, RespondedRpc, RpcBodyReaderHandle, RpcBodyWriterHandle, SessionHandle,
            StartedRpc, StreamHandle,
        },
    };

    struct TestNetworkCapability {
        sent_stream_chunks: Arc<Mutex<Vec<Vec<u8>>>>,
        written_rpc_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl Default for TestNetworkCapability {
        fn default() -> Self {
            Self {
                sent_stream_chunks: Arc::new(Mutex::new(Vec::new())),
                written_rpc_bodies: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Clone for TestNetworkCapability {
        fn clone(&self) -> Self {
            Self {
                sent_stream_chunks: Arc::clone(&self.sent_stream_chunks),
                written_rpc_bodies: Arc::clone(&self.written_rpc_bodies),
            }
        }
    }

    impl NetworkCapability for TestNetworkCapability {
        type Error = GuestError;
        type Listener = &'static str;
        type Session = &'static str;
        type Stream = &'static str;
        type RpcClientExchange = &'static str;
        type RpcExchange = &'static str;
        type RpcBodyReader = &'static str;
        type RpcBodyWriter = &'static str;

        fn listen(
            &self,
            _policy: Arc<NetworkProcessPolicy>,
            _input: NetworkListen,
        ) -> NetworkFuture<ListenerHandle<Self::Listener>, Self::Error> {
            Box::pin(async move {
                Ok(ListenerHandle {
                    protocol: NetworkProtocol::Quic,
                    interactions: vec![InteractionKind::Stream, InteractionKind::Rpc],
                    inner: "listener",
                })
            })
        }

        fn connect(
            &self,
            _policy: Arc<NetworkProcessPolicy>,
            _input: NetworkConnect,
        ) -> NetworkFuture<SessionHandle<Self::Session>, Self::Error> {
            Box::pin(async move {
                Ok(SessionHandle {
                    protocol: NetworkProtocol::Quic,
                    interactions: vec![InteractionKind::Stream, InteractionKind::Rpc],
                    inner: "session",
                })
            })
        }

        fn accept(
            &self,
            _listener: &Self::Listener,
            _timeout_ms: u32,
        ) -> NetworkFuture<AcceptedSession<Self::Session>, Self::Error> {
            Box::pin(async move {
                Ok(AcceptedSession {
                    code: NetworkStatusCode::Ok,
                    session: Some(SessionHandle {
                        protocol: NetworkProtocol::Quic,
                        interactions: vec![InteractionKind::Stream],
                        inner: "accepted-session",
                    }),
                })
            })
        }

        fn stream_open(
            &self,
            _session: &Self::Session,
        ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error> {
            Box::pin(async move {
                Ok(AcceptedStream {
                    code: NetworkStatusCode::Ok,
                    stream: Some(StreamHandle { inner: "stream" }),
                })
            })
        }

        fn stream_accept(
            &self,
            _session: &Self::Session,
            _timeout_ms: u32,
        ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error> {
            Box::pin(async move {
                Ok(AcceptedStream {
                    code: NetworkStatusCode::Ok,
                    stream: Some(StreamHandle {
                        inner: "accepted-stream",
                    }),
                })
            })
        }

        fn stream_send(
            &self,
            _stream: &Self::Stream,
            input: NetworkStreamSend,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            let bytes = input.bytes;
            self.sent_stream_chunks
                .lock()
                .expect("stream lock")
                .push(bytes);
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn stream_recv(
            &self,
            _stream: &Self::Stream,
            _input: selium_abi::NetworkStreamRecv,
        ) -> NetworkFuture<selium_abi::NetworkStreamRecvResult, Self::Error> {
            panic!("stream_recv not used in test")
        }

        fn rpc_invoke(
            &self,
            _session: &Self::Session,
            _input: NetworkRpcInvoke,
        ) -> NetworkFuture<StartedRpc<Self::RpcClientExchange, Self::RpcBodyWriter>, Self::Error>
        {
            Box::pin(async move {
                Ok(StartedRpc {
                    code: NetworkStatusCode::Ok,
                    exchange: Some("client-exchange"),
                    request_body: Some(RpcBodyWriterHandle {
                        inner: "request-body",
                    }),
                })
            })
        }

        fn rpc_await(
            &self,
            _exchange: Self::RpcClientExchange,
            _input: selium_abi::NetworkRpcAwait,
        ) -> NetworkFuture<AwaitedRpc<Self::RpcBodyReader>, Self::Error> {
            panic!("rpc_await not used in test")
        }

        fn rpc_accept(
            &self,
            _session: &Self::Session,
            _timeout_ms: u32,
        ) -> NetworkFuture<AcceptedRpc<Self::RpcExchange, Self::RpcBodyReader>, Self::Error>
        {
            panic!("rpc_accept not used in test")
        }

        fn rpc_respond(
            &self,
            _exchange: Self::RpcExchange,
            _input: selium_abi::NetworkRpcRespond,
        ) -> NetworkFuture<RespondedRpc<Self::RpcBodyWriter>, Self::Error> {
            panic!("rpc_respond not used in test")
        }

        fn rpc_body_read(
            &self,
            _body: &Self::RpcBodyReader,
            _input: selium_abi::NetworkRpcBodyRead,
        ) -> NetworkFuture<selium_abi::NetworkRpcBodyReadResult, Self::Error> {
            panic!("rpc_body_read not used in test")
        }

        fn rpc_body_write(
            &self,
            _body: &Self::RpcBodyWriter,
            input: NetworkRpcBodyWrite,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            let bytes = input.bytes;
            self.written_rpc_bodies
                .lock()
                .expect("rpc body lock")
                .push(bytes);
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_listener(
            &self,
            _listener: ListenerHandle<Self::Listener>,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_session(
            &self,
            _session: SessionHandle<Self::Session>,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_stream(
            &self,
            _stream: StreamHandle<Self::Stream>,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_rpc_exchange(
            &self,
            _exchange: Self::RpcExchange,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_rpc_client_exchange(
            &self,
            _exchange: Self::RpcClientExchange,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_rpc_body_reader(
            &self,
            _body: RpcBodyReaderHandle<Self::RpcBodyReader>,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }

        fn close_rpc_body_writer(
            &self,
            _body: RpcBodyWriterHandle<Self::RpcBodyWriter>,
        ) -> NetworkFuture<NetworkStatus, Self::Error> {
            Box::pin(async move {
                Ok(NetworkStatus {
                    code: NetworkStatusCode::Ok,
                })
            })
        }
    }

    struct TestContext {
        registry: crate::registry::InstanceRegistry,
    }

    impl HostcallContext for TestContext {
        fn registry(&self) -> &crate::registry::InstanceRegistry {
            &self.registry
        }

        fn registry_mut(&mut self) -> &mut crate::registry::InstanceRegistry {
            &mut self.registry
        }

        fn mailbox_base(&mut self) -> Option<usize> {
            None
        }
    }

    fn registry_with_capabilities(
        capabilities: Vec<Capability>,
    ) -> crate::registry::InstanceRegistry {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap(capabilities, [0; 32])))
            .expect("root session");
        instance
    }

    fn registry_with_scoped_entitlements(
        entitlements: HashMap<Capability, ResourceScope>,
    ) -> crate::registry::InstanceRegistry {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap_with_scoped_principal(
                entitlements,
                [0; 32],
                PrincipalRef::new(PrincipalKind::Internal, "runtime"),
                SessionAuthnMethod::Delegated,
            )))
            .expect("root session");
        instance
    }

    #[test]
    fn network_close_allows_stream_write_holders_to_close_streams() {
        let instance = registry_with_capabilities(vec![Capability::NetworkStreamWrite]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::NetworkStream)
            .expect("insert stream");

        authorise_network_close(&instance, slot, ResourceType::NetworkStream)
            .expect("stream close should be authorised");
    }

    #[test]
    fn network_close_still_requires_lifecycle_for_listeners() {
        let instance = registry_with_capabilities(vec![Capability::NetworkStreamWrite]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::NetworkListener)
            .expect("insert listener");

        let err = authorise_network_close(&instance, slot, ResourceType::NetworkListener)
            .expect_err("listener close should require lifecycle");
        assert!(matches!(err, GuestError::PermissionDenied));
    }

    #[tokio::test]
    async fn network_listen_grants_scoped_listener_accept_access() {
        let capability = TestNetworkCapability::default();
        let mut ctx = TestContext {
            registry: registry_with_scoped_entitlements(HashMap::from([
                (Capability::NetworkLifecycle, ResourceScope::Any),
                (Capability::NetworkAccept, ResourceScope::None),
            ])),
        };
        let listen = NetworkListenDriver(capability.clone());
        let accept = NetworkAcceptDriver(capability);

        let listener = listen
            .to_future(
                &mut ctx,
                NetworkListen {
                    binding_name: "ingress".to_string(),
                },
            )
            .await
            .expect("listen should succeed");
        let accepted = accept
            .to_future(
                &mut ctx,
                NetworkAccept {
                    listener_id: listener.resource_id,
                    timeout_ms: 0,
                },
            )
            .await
            .expect("accept should succeed");

        assert_eq!(accepted.code, NetworkStatusCode::Ok);
        assert!(accepted.session.is_some());
    }

    #[tokio::test]
    async fn network_stream_flow_grants_scoped_handles() {
        let capability = TestNetworkCapability::default();
        let mut ctx = TestContext {
            registry: registry_with_scoped_entitlements(HashMap::from([
                (Capability::NetworkConnect, ResourceScope::Any),
                (Capability::NetworkStreamWrite, ResourceScope::None),
            ])),
        };
        let connect = NetworkConnectDriver(capability.clone());
        let open = NetworkStreamOpenDriver(capability.clone());
        let send = NetworkStreamSendDriver(capability.clone());

        let session = connect
            .to_future(
                &mut ctx,
                NetworkConnect {
                    protocol: NetworkProtocol::Quic,
                    profile_name: "egress".to_string(),
                    authority: "example.com:443".to_string(),
                },
            )
            .await
            .expect("connect should succeed");
        let stream = open
            .to_future(
                &mut ctx,
                NetworkStreamOpen {
                    session_id: session.resource_id,
                },
            )
            .await
            .expect("stream open should succeed")
            .stream
            .expect("stream");
        let status = send
            .to_future(
                &mut ctx,
                NetworkStreamSend {
                    stream_id: stream.resource_id,
                    bytes: b"hello".to_vec(),
                    finish: false,
                    timeout_ms: 0,
                },
            )
            .await
            .expect("stream send should succeed");

        assert_eq!(status.code, NetworkStatusCode::Ok);
        assert_eq!(
            capability
                .sent_stream_chunks
                .lock()
                .expect("chunks lock")
                .as_slice(),
            &[b"hello".to_vec()]
        );
    }

    #[tokio::test]
    async fn network_rpc_flow_grants_scoped_exchange_and_body_handles() {
        let capability = TestNetworkCapability::default();
        let mut ctx = TestContext {
            registry: registry_with_scoped_entitlements(HashMap::from([
                (Capability::NetworkConnect, ResourceScope::Any),
                (Capability::NetworkRpcClient, ResourceScope::None),
            ])),
        };
        let connect = NetworkConnectDriver(capability.clone());
        let invoke = NetworkRpcInvokeDriver(capability.clone());
        let write = NetworkRpcRequestBodyWriteDriver(capability.clone());

        let session = connect
            .to_future(
                &mut ctx,
                NetworkConnect {
                    protocol: NetworkProtocol::Quic,
                    profile_name: "egress".to_string(),
                    authority: "example.com:443".to_string(),
                },
            )
            .await
            .expect("connect should succeed");
        let invoke_result = invoke
            .to_future(
                &mut ctx,
                NetworkRpcInvoke {
                    session_id: session.resource_id,
                    request: NetworkRpcRequestHead {
                        method: "POST".to_string(),
                        path: "/rpc".to_string(),
                        metadata: Default::default(),
                    },
                    timeout_ms: 0,
                },
            )
            .await
            .expect("rpc invoke should succeed");
        let body = invoke_result.request_body.expect("request body");
        let status = write
            .to_future(
                &mut ctx,
                NetworkRpcBodyWrite {
                    body_id: body.resource_id,
                    bytes: b"payload".to_vec(),
                    finish: true,
                    timeout_ms: 0,
                },
            )
            .await
            .expect("rpc body write should succeed");

        assert_eq!(status.code, NetworkStatusCode::Ok);
        assert_eq!(
            capability
                .written_rpc_bodies
                .lock()
                .expect("bodies lock")
                .as_slice(),
            &[b"payload".to_vec()]
        );
    }
}
