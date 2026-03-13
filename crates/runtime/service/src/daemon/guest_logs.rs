use super::*;

pub(super) async fn query_runtime_usage(
    state: &DaemonState,
    payload: &RuntimeUsageApiRequest,
) -> Result<RuntimeUsageApiResponse> {
    let collector = state
        .kernel
        .get::<crate::usage::RuntimeUsageCollector>()
        .cloned()
        .ok_or_else(|| anyhow!("runtime usage collector unavailable"))?;
    let result = collector.replay_usage(&payload.query).await?;
    let high_watermark = result.high_watermark;
    let next_sequence =
        next_runtime_usage_sequence(&payload.query, &result.records, high_watermark);
    if let Some(name) = payload.query.save_checkpoint.as_deref() {
        let checkpoint_sequence = next_sequence.unwrap_or(collector.next_sequence().await);
        collector.checkpoint(name, checkpoint_sequence).await?;
    }
    Ok(RuntimeUsageApiResponse {
        next_sequence,
        records: result.records,
        high_watermark,
    })
}

pub(super) fn next_runtime_usage_sequence(
    query: &selium_abi::RuntimeUsageQuery,
    records: &[selium_abi::RuntimeUsageRecord],
    high_watermark: Option<u64>,
) -> Option<u64> {
    if query.limit == 0 {
        return None;
    }
    if let Some(record) = records.last() {
        return Some(record.sequence.saturating_add(1));
    }
    let advanced = high_watermark.map(|sequence| sequence.saturating_add(1));
    match query.start {
        selium_abi::RuntimeUsageReplayStart::Sequence(sequence) => {
            Some(advanced.map_or(sequence, |next| next.max(sequence)))
        }
        selium_abi::RuntimeUsageReplayStart::Checkpoint(_) => advanced,
        _ => advanced,
    }
}

pub(super) async fn handle_guest_log_subscription_stream(
    state: Rc<DaemonState>,
    request_context: AuthenticatedRequestContext,
    send: &mut quinn::SendStream,
    envelope: Envelope,
) -> Result<()> {
    let payload: SubscribeGuestLogsRequest = match decode_payload(&envelope) {
        Ok(payload) => payload,
        Err(err) => {
            let response = rpc_decode_error(envelope.method, envelope.request_id, err)?;
            write_framed(send, &response)
                .await
                .context("write subscription decode error")?;
            let _ = send.finish();
            return Ok(());
        }
    };

    let resolved = match resolve_guest_log_subscription(&state, &request_context, &payload).await {
        Ok(resolved) => resolved,
        Err(err) => {
            let response = encode_error_response(
                envelope.method,
                envelope.request_id,
                classify_guest_log_resolution_error(&err),
                err.to_string(),
                false,
            )?;
            write_framed(send, &response)
                .await
                .context("write subscription resolution error")?;
            let _ = send.finish();
            return Ok(());
        }
    };

    let subscription = match activate_guest_log_subscription(&state, &resolved).await {
        Ok(subscription) => subscription,
        Err(err) => {
            let response = rpc_server_error(envelope.method, envelope.request_id, err)?;
            write_framed(send, &response)
                .await
                .context("write subscription setup error")?;
            let _ = send.finish();
            return Ok(());
        }
    };

    let response = encode_response(
        envelope.method,
        envelope.request_id,
        &SubscribeGuestLogsResponse {
            status: "ok".to_string(),
            target_node: resolved.target.node.clone(),
            target_instance_id: resolved.target.replica_key.clone(),
            streams: resolved.streams.clone(),
        },
    )?;

    let stream_result = async {
        write_framed(send, &response)
            .await
            .context("write subscription response")?;
        stream_guest_log_events(&state, &subscription.instance_id, &resolved.streams, send).await
    }
    .await;
    let cleanup_result = deactivate_guest_log_subscription(&state, &resolved, &subscription).await;
    let _ = send.finish();

    cleanup_result?;
    match stream_result {
        Ok(()) => Ok(()),
        Err(err) if is_closed_stream_error(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

pub(super) async fn resolve_guest_log_subscription(
    state: &Rc<DaemonState>,
    request_context: &AuthenticatedRequestContext,
    payload: &SubscribeGuestLogsRequest,
) -> Result<ResolvedGuestLogSubscription> {
    resolve_guest_log_subscription_with(
        &state.node_id,
        payload,
        request_context.discovery_scope_for(Method::SubscribeGuestLogs),
        |query| state.control_plane.query_value(query, true),
    )
    .await
}

pub(super) async fn resolve_guest_log_subscription_with<F, Fut>(
    local_node_id: &str,
    payload: &SubscribeGuestLogsRequest,
    discovery_scope: DiscoveryCapabilityScope,
    query_value: F,
) -> Result<ResolvedGuestLogSubscription>
where
    F: Fn(Query) -> Fut,
    Fut: Future<Output = Result<DataValue>>,
{
    let target = decode_query_bytes::<DiscoveryResolution>(
        query_value(Query::ResolveDiscovery {
            request: DiscoveryRequest {
                operation: DiscoveryOperation::Discover,
                target: DiscoveryTarget::RunningProcess(payload.target.clone()),
                scope: discovery_scope.clone(),
            },
        })
        .await?,
        "running process discovery",
    )?;
    let DiscoveryResolution::RunningProcess(target) = target else {
        bail!("control-plane returned non-process discovery for guest log target");
    };

    let control_plane = decode_query_bytes::<ControlPlaneState>(
        query_value(Query::ControlPlaneState).await?,
        "control-plane state",
    )?;
    let source_node = control_plane
        .nodes
        .get(&target.node)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "unknown node `{}` for replica `{}`",
                target.node,
                target.replica_key
            )
        })?;
    let local_node = control_plane
        .nodes
        .get(local_node_id)
        .cloned()
        .ok_or_else(|| {
            anyhow!("current daemon node `{local_node_id}` is missing from control-plane state")
        })?;

    let mut seen = BTreeSet::new();
    let mut streams = Vec::new();
    for stream_name in &payload.stream_names {
        let stream_name = stream_name.trim();
        if stream_name.is_empty() {
            bail!("log stream names must not be empty");
        }
        if !seen.insert(stream_name.to_string()) {
            continue;
        }
        let endpoint = PublicEndpointRef {
            workload: target.workload.clone(),
            kind: ContractKind::Event,
            name: stream_name.to_string(),
        };
        let resolution = decode_query_bytes::<DiscoveryResolution>(
            query_value(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(endpoint),
                    scope: discovery_scope.clone(),
                },
            })
            .await?,
            "guest log endpoint discovery",
        )?;
        let DiscoveryResolution::Endpoint(resolved) = resolution else {
            bail!("control-plane returned non-endpoint discovery for guest log stream");
        };
        if resolved.endpoint.kind != ContractKind::Event {
            bail!(
                "guest log stream `{}` resolved to non-event endpoint kind {:?}",
                resolved.endpoint.name,
                resolved.endpoint.kind
            );
        }
        streams.push(resolved.endpoint);
    }
    if streams.is_empty() {
        bail!("at least one guest log stream is required");
    }

    Ok(ResolvedGuestLogSubscription {
        target,
        source_node,
        local_node,
        streams,
    })
}

pub(super) fn classify_guest_log_resolution_error(err: &anyhow::Error) -> u16 {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<selium_control_plane_api::ApiError>()
            .is_some_and(|api_err| {
                matches!(
                    api_err,
                    selium_control_plane_api::ApiError::Unauthorised { .. }
                )
            })
            || cause
                .downcast_ref::<selium_control_plane_runtime::RuntimeError>()
                .is_some_and(|runtime_err| {
                    matches!(
                        runtime_err,
                        selium_control_plane_runtime::RuntimeError::Api(
                            selium_control_plane_api::ApiError::Unauthorised { .. }
                        )
                    )
                })
            || cause
                .to_string()
                .starts_with("control-plane guest reported 403:")
            || cause.to_string().contains("unauthorised to ")
    }) {
        403
    } else {
        400
    }
}

pub(super) fn decode_query_bytes<T>(value: DataValue, subject: &str) -> Result<T>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let bytes = match value {
        DataValue::Bytes(bytes) => bytes,
        other => bail!("invalid {subject} payload (expected bytes), got {other:?}"),
    };
    decode_rkyv(&bytes).with_context(|| format!("decode {subject}"))
}

pub(super) async fn activate_guest_log_subscription(
    state: &Rc<DaemonState>,
    resolved: &ResolvedGuestLogSubscription,
) -> Result<ActiveGuestLogSubscription> {
    let subscription_id = state.host_subscription_id.fetch_add(1, Ordering::Relaxed);
    let instance_id = format!("host-log-subscription-{subscription_id}");
    let bindings = resolved
        .streams
        .iter()
        .map(|endpoint| ManagedEndpointBinding {
            endpoint_name: endpoint.name.clone(),
            endpoint_kind: ContractKind::Event,
            role: ManagedEndpointRole::Ingress,
            binding_type: ManagedEndpointBindingType::OneWay,
        })
        .collect::<Vec<_>>();
    ensure_managed_endpoint_bindings(state, &instance_id, &bindings).await?;

    let mut bridge_ids = Vec::new();
    for endpoint in &resolved.streams {
        let bridge_id = format!("{instance_id}:{}", endpoint.name);
        let request = ActivateEndpointBridgeRequest {
            node_id: resolved.target.node.clone(),
            bridge_id: bridge_id.clone(),
            source_instance_id: resolved.target.replica_key.clone(),
            source_endpoint: endpoint.clone(),
            target_instance_id: instance_id.clone(),
            target_node: state.node_id.clone(),
            target_daemon_addr: resolved.local_node.daemon_addr.clone(),
            target_daemon_server_name: resolved.local_node.daemon_server_name.clone(),
            target_endpoint: endpoint.clone(),
            semantics: EndpointBridgeSemantics::Event(
                selium_control_plane_protocol::EventBridgeSemantics {
                    delivery: selium_control_plane_protocol::EventDeliveryMode::Frame,
                },
            ),
        };
        let activation = if resolved.target.node == state.node_id {
            activate_endpoint_bridge(state, &request).await
        } else {
            activate_endpoint_bridge_remote(&state.tls_paths, &resolved.source_node, &request).await
        };
        if let Err(err) = activation {
            let partial = ActiveGuestLogSubscription {
                instance_id: instance_id.clone(),
                bridge_ids,
            };
            let _ = deactivate_guest_log_subscription(state, resolved, &partial).await;
            return Err(err);
        }
        bridge_ids.push(bridge_id);
    }

    Ok(ActiveGuestLogSubscription {
        instance_id,
        bridge_ids,
    })
}

pub(super) async fn deactivate_guest_log_subscription(
    state: &Rc<DaemonState>,
    resolved: &ResolvedGuestLogSubscription,
    subscription: &ActiveGuestLogSubscription,
) -> Result<()> {
    let mut first_error: Option<anyhow::Error> = None;
    for bridge_id in &subscription.bridge_ids {
        let request = DeactivateEndpointBridgeRequest {
            node_id: resolved.target.node.clone(),
            bridge_id: bridge_id.clone(),
        };
        let outcome = if resolved.target.node == state.node_id {
            deactivate_endpoint_bridge(state, &request)
                .await
                .map(|_| ())
        } else {
            deactivate_endpoint_bridge_remote(&state.tls_paths, &resolved.source_node, &request)
                .await
                .map(|_| ())
        };
        if let Err(err) = outcome
            && first_error.is_none()
        {
            first_error = Some(err);
        }
    }
    state
        .target_bindings
        .lock()
        .await
        .retain(|(instance_id, _, _), _| instance_id != &subscription.instance_id);

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

pub(super) async fn stream_guest_log_events<W>(
    state: &DaemonState,
    subscription_instance_id: &str,
    streams: &[PublicEndpointRef],
    writer: &mut W,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut readers = Vec::new();
    for endpoint in streams {
        let queue = {
            let bindings = state.target_bindings.lock().await;
            bindings
                .get(&endpoint_key(subscription_instance_id, endpoint))
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "subscription queue missing for `{}` on `{subscription_instance_id}`",
                        endpoint.name
                    )
                })?
        };
        let reader = QueueService
            .attach(
                &queue.queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Reader,
                },
            )
            .with_context(|| format!("attach guest log reader for `{}`", endpoint.name))?;
        readers.push((endpoint.clone(), reader));
    }

    loop {
        for (endpoint, reader) in &readers {
            let waited = QueueService
                .wait(reader, MANAGED_EVENT_RETRY_DELAY.as_millis() as u32)
                .await
                .with_context(|| format!("wait for guest log `{}`", endpoint.name))?;
            match waited.code {
                QueueStatusCode::Timeout => continue,
                QueueStatusCode::Ok => {
                    let Some(frame) = waited.frame else {
                        continue;
                    };
                    let payload = read_managed_event_frame(
                        state,
                        frame.shm_shared_id,
                        frame.offset,
                        frame.len,
                    )
                    .await?;
                    let event = GuestLogEvent {
                        endpoint: endpoint.clone(),
                        payload,
                    };
                    let frame_bytes = encode_rkyv(&event).context("encode guest log event")?;
                    write_framed(writer, &frame_bytes)
                        .await
                        .with_context(|| format!("write guest log `{}`", endpoint.name))?;
                    QueueService
                        .ack(
                            reader,
                            QueueAck {
                                endpoint_id: 0,
                                seq: frame.seq,
                            },
                        )
                        .with_context(|| format!("ack guest log `{}`", endpoint.name))?;
                }
                other => {
                    bail!(
                        "guest log queue wait failed for `{}` with {other:?}",
                        endpoint.name
                    )
                }
            }
        }
    }
}

pub(super) fn is_closed_stream_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::UnexpectedEof
            )
        })
    })
}
