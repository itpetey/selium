//! ABI contract and codec tests.

use super::*;

/// `Namespace` round-trips through the rkyv codec in both variants; the
/// selector vocabulary that uses it (and the delegation fence) shares the
/// same ABI codec.
#[test]
fn namespace_round_trips_through_rkyv() {
    for namespace in [Namespace::Root, Namespace::Tenant("acme".to_string())] {
        let encoded = encode_rkyv(&namespace).expect("encode namespace");
        let decoded: Namespace = decode_rkyv(&encoded).expect("decode namespace");
        assert_eq!(decoded, namespace);
    }
}

// scenario: selium-abi/regionprot-enum/regionprot-serialisation
#[test]
fn region_prot_serialises_to_stable_discriminants() {
    // The wire form is the discriminant: `RegionProt` travels inside
    // `AllocRegion`/`AttachRegion` envelopes, so a reordering would
    // silently invert a read-only request into a writable one.
    assert_eq!(RegionProt::ReadOnly as u8, 0);
    assert_eq!(RegionProt::ReadWrite as u8, 1);

    for prot in [RegionProt::ReadOnly, RegionProt::ReadWrite] {
        let encoded = encode_rkyv(&prot).expect("encode region protection");
        let decoded: RegionProt = decode_rkyv(&encoded).expect("decode region protection");
        assert_eq!(decoded, prot);
    }
}

/// `ResourceSelector::Namespace` matches hierarchically: a tenant selector
/// admits only its own tenant, and the root selector admits only a root
/// (tenant-less) scope context.
// scenario: capability-enforcement/namespace-selector-hierarchy/tenant-selector-admits-only-its-tenant
// scenario: capability-enforcement/namespace-selector-hierarchy/root-selector-admits-the-root-namespace
#[test]
fn namespace_selector_matches_exactly_and_root() {
    let acme = ResourceSelector::Namespace(Namespace::Tenant("acme".to_string()));
    let root = ResourceSelector::Namespace(Namespace::Root);

    let acme_context = ScopeContext {
        tenant: Some("acme".to_string()),
        ..ScopeContext::default()
    };
    let root_context = ScopeContext {
        tenant: None,
        ..ScopeContext::default()
    };
    let beta_context = ScopeContext {
        tenant: Some("beta".to_string()),
        ..ScopeContext::default()
    };

    // Exact-tenant admission.
    assert!(acme.matches(&acme_context));
    assert!(!acme.matches(&beta_context));
    assert!(
        !acme.matches(&root_context),
        "tenant selector must not admit root"
    );

    // Root admission.
    assert!(root.matches(&root_context));
    assert!(
        !root.matches(&acme_context),
        "root selector must not admit a tenant"
    );
}

#[test]
fn scope_grants_use_intersection_semantics() {
    let grant = CapabilityGrant::new(
        Capability::ProcessLifecycle,
        vec![
            ResourceSelector::Tenant("acme".to_string()),
            ResourceSelector::UriPrefix("sel://acme/payments/".to_string()),
        ],
    );
    let allowed = ScopeContext {
        tenant: Some("acme".to_string()),
        uri: Some("sel://acme/payments/worker".to_string()),
        ..ScopeContext::default()
    };
    let denied = ScopeContext {
        tenant: Some("acme".to_string()),
        uri: Some("sel://acme/other/worker".to_string()),
        ..ScopeContext::default()
    };

    assert!(grant.allows(&allowed));
    assert!(!grant.allows(&denied));
}

// scenario: selium-abi/host-guest-boundary-only/hostcall-contract-is-unchanged
#[test]
fn encode_and_decode_round_trip() {
    let request = HostcallEnvelope {
        request: HostcallRequest::AllocRegion {
            pages: 16,
            prot: RegionProt::ReadWrite,
            purpose: ResourceKind::SharedMemory,
            serving_tenant: None,
        },
        task_id: Some(42),
    };

    let encoded = encode_rkyv(&request).expect("encode request");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode request");
    assert_eq!(decoded, request);
}

#[test]
fn explicit_error_codes_round_trip() {
    let error = AbiError::new(AbiErrorCode::DetachedResource, "mapping detached");
    let encoded = encode_rkyv(&error).expect("encode error");
    let decoded: AbiError = decode_rkyv(&encoded).expect("decode error");

    assert_eq!(decoded.code, AbiErrorCode::DetachedResource);
    assert_eq!(decoded.message, "mapping detached");
}

#[test]
fn quota_exceeded_error_code_round_trip() {
    // A quota denial is a distinct code: guests must be able to tell
    // "you lack the capability" apart from "your tenant hit its ceiling".
    let error = AbiError::new(
        AbiErrorCode::QuotaExceeded,
        "quota exceeded for tenant acme on SharedRegion",
    );
    let encoded = encode_rkyv(&error).expect("encode error");
    let decoded: AbiError = decode_rkyv(&encoded).expect("decode error");

    assert_eq!(decoded.code, AbiErrorCode::QuotaExceeded);
    assert!(decoded.message.contains("acme"));
}

#[test]
fn frame_and_deframe_bytes_round_trip() {
    let framed = frame_bytes(b"hello").expect("frame bytes");
    let deframed = deframe_bytes(&framed).expect("deframe bytes");
    assert_eq!(deframed, b"hello");
}

// scenario: selium-abi/hostqueuecreate-serving-tenant/serving-tenant-round-trips
#[test]
fn host_queue_create_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::HostQueueCreate {
            serving_tenant: Some("acme".to_string()),
        },
        task_id: Some(1),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

// scenario: selium-abi/host-and-guest-negotiate-abi-compatibility/current-versions-match
#[test]
fn hostcall_preamble_accepts_the_matching_abi_version() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::SelfInfo,
        task_id: Some(5),
    };
    let encoded = encode_hostcall_request(&envelope).expect("encode with preamble");
    assert_eq!(
        encoded.get(..4),
        Some(HOSTCALL_MAGIC.to_le_bytes().as_slice())
    );
    assert_eq!(
        encoded.get(4..HOSTCALL_PREAMBLE_SIZE),
        Some(SELIUM_ABI_VERSION.to_le_bytes().as_slice())
    );
    let payload = hostcall_request_payload(&encoded).expect("matching version");
    let decoded: HostcallEnvelope = decode_rkyv(payload).expect("decode payload");
    assert_eq!(decoded, envelope);
}

#[test]
fn hostcall_preamble_rejects_missing_or_truncated_headers() {
    assert_eq!(
        hostcall_request_payload(&[]),
        Err(HostcallPreambleError::Missing)
    );
    assert_eq!(
        hostcall_request_payload(&HOSTCALL_MAGIC.to_le_bytes()),
        Err(HostcallPreambleError::Missing),
        "a magic without a version is still missing the preamble"
    );
}

#[test]
fn hostcall_preamble_rejects_bad_magic() {
    let mut bytes = HOSTCALL_MAGIC.to_le_bytes().to_vec();
    bytes.extend_from_slice(&SELIUM_ABI_VERSION.to_le_bytes());
    bytes[0] ^= 0xff;
    assert_eq!(
        hostcall_request_payload(&bytes),
        Err(HostcallPreambleError::BadMagic {
            found: u32::from_le_bytes(
                bytes
                    .get(..4)
                    .and_then(|slice| slice.try_into().ok())
                    .expect("four bytes")
            )
        })
    );
}

// scenario: selium-abi/host-and-guest-negotiate-abi-compatibility/guest-abi-is-incompatible
#[test]
fn hostcall_preamble_reports_both_versions_on_mismatch() {
    for guest in [0, SELIUM_ABI_VERSION + 1, u32::MAX] {
        let mut bytes = HOSTCALL_MAGIC.to_le_bytes().to_vec();
        bytes.extend_from_slice(&guest.to_le_bytes());
        let error = hostcall_request_payload(&bytes).expect_err("version must be rejected");
        assert_eq!(
            error,
            HostcallPreambleError::VersionMismatch {
                host: SELIUM_ABI_VERSION,
                guest
            }
        );
        let message = error.to_string();
        assert!(
            message.contains(&SELIUM_ABI_VERSION.to_string())
                && message.contains(&guest.to_string()),
            "the error must name both versions: {message}"
        );
    }
}

#[test]
fn hostcall_preamble_rejection_is_a_structured_abi_error() {
    let error = HostcallPreambleError::VersionMismatch {
        host: SELIUM_ABI_VERSION,
        guest: SELIUM_ABI_VERSION + 7,
    };
    let abi = error.to_abi_error();
    assert_eq!(abi.code, AbiErrorCode::MalformedPayload);
    assert!(
        abi.message.contains(&SELIUM_ABI_VERSION.to_string())
            && abi.message.contains(&(SELIUM_ABI_VERSION + 7).to_string()),
        "the guest-visible error names both versions: {}",
        abi.message
    );
    assert_eq!(
        HostcallPreambleError::Missing.to_abi_error().code,
        AbiErrorCode::MalformedPayload
    );
}

#[test]
fn host_queue_attach_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::HostQueueAttach { shared_id: 42 },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn host_queue_send_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::HostQueueSend {
            local_id: 7,
            value: 99,
            metadata: Vec::new(),
        },
        task_id: Some(3),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn host_queue_send_metadata_round_trip() {
    let metadata: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04];
    let envelope = HostcallEnvelope {
        request: HostcallRequest::HostQueueSend {
            local_id: 7,
            value: 99,
            metadata: metadata.clone(),
        },
        task_id: Some(3),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    let HostcallRequest::HostQueueSend {
        metadata: decoded_metadata,
        ..
    } = decoded.request
    else {
        panic!("expected HostQueueSend");
    };
    assert_eq!(decoded_metadata, metadata);
}

#[test]
fn capability_delegate_grants_round_trip() {
    for capability in [
        Capability::DelegateGrants,
        Capability::HostQueue,
        Capability::MintCertificate,
        Capability::ProcessProbe,
    ] {
        let encoded = encode_rkyv(&capability).expect("encode");
        let decoded: Capability = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, capability);
    }
}

#[test]
fn capability_mint_certificate_round_trip() {
    // `MintCertificate` is rkyv-encodable like every other variant, so it
    // carries the same ABI stability guarantee as the rest of the enum.
    let capability = Capability::MintCertificate;
    let encoded = encode_rkyv(&capability).expect("encode");
    let decoded: Capability = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, Capability::MintCertificate);
}

#[test]
fn capability_quota_write_round_trip() {
    // `QuotaWrite` is rkyv-encodable like every other variant: the quota
    // hostcalls it gates are gated by an ordinary capability grant that
    // travels over the same ABI as the rest of the capability set.
    let capability = Capability::QuotaWrite;
    let encoded = encode_rkyv(&capability).expect("encode");
    let decoded: Capability = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, Capability::QuotaWrite);
}

#[test]
fn host_queue_recv_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::HostQueueRecv { local_id: 7 },
        task_id: Some(4),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn connection_info_output_round_trip() {
    let output = HostcallOutput::ConnectionInfo {
        client_process_id: 123,
        value: 456,
        metadata: Vec::new(),
    };
    let encoded = encode_rkyv(&output).expect("encode");
    let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, output);
}

#[test]
fn connection_info_metadata_round_trip() {
    let output = HostcallOutput::ConnectionInfo {
        client_process_id: 123,
        value: 456,
        metadata: b"tenant=acme,fp=abcd".to_vec(),
    };
    let encoded = encode_rkyv(&output).expect("encode");
    let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, output);
}

#[test]
fn volume_hostcalls_round_trip() {
    let requests = vec![
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: Some("acme".to_string()),
        },
        HostcallRequest::VolumeAppend {
            local_id: 1,
            bytes: vec![1, 2, 3],
        },
        HostcallRequest::VolumeReadAt {
            local_id: 1,
            offset: 4,
            len: 8,
        },
        HostcallRequest::VolumeLen { local_id: 1 },
        HostcallRequest::VolumeList {
            serving_tenant: None,
        },
        HostcallRequest::VolumeRemove {
            name: "orders".to_string(),
            serving_tenant: Some("acme".to_string()),
        },
        HostcallRequest::VolumeSync { local_id: 1 },
        HostcallRequest::VolumeClose { local_id: 1 },
        HostcallRequest::VolumeUsage {
            tenant: "acme".to_string(),
        },
    ];
    for request in requests {
        let envelope = HostcallEnvelope {
            request,
            task_id: Some(9),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    for output in [
        HostcallOutput::Volume(VolumeDescriptor {
            local_id: 3,
            len: 64,
        }),
        HostcallOutput::VolumeNames(vec!["a".to_string(), "b".to_string()]),
    ] {
        let encoded = encode_rkyv(&output).expect("encode");
        let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, output);
    }
}

// scenario: selium-abi/volume-resource-class/log-and-blob-classes-are-absent
#[test]
fn storage_classes_leave_the_abi() {
    assert_eq!(ResourceClass::Volume.uri_segment(), "volume");
    assert_eq!(
        ResourceClass::from_uri_segment("volume"),
        Some(ResourceClass::Volume)
    );
    assert_eq!(ResourceClass::from_uri_segment("log"), None);
    assert_eq!(ResourceClass::from_uri_segment("blob"), None);
}
// scenario: selium-abi/process-metering-observation/observation-fields-round-trip
// scenario: selium-abi/process-metering-observation/cpu-microseconds-absent
// scenario: selium-abi/process-metering-observation/storage-is-not-a-per-process-field

#[test]
fn metering_observation_round_trip() {
    // The observation carries the cumulative instruction count and the
    // memory/bandwidth gauges. Neither the former microsecond CPU field
    // nor a per-process storage field exists on the type: tenant storage
    // is read through the volume-usage hostcall.
    let output = HostcallOutput::Metering(MeteringObservation {
        cpu_instructions: 123_456,
        memory_bytes: 4_096,
        bandwidth_bytes: 8_192,
    });
    let encoded = encode_rkyv(&output).expect("encode");
    let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, output);

    let HostcallOutput::Metering(observation) = decoded else {
        panic!("expected metering observation");
    };
    assert_eq!(observation.cpu_instructions, 123_456);
    assert_eq!(observation.memory_bytes, 4_096);
    assert_eq!(observation.bandwidth_bytes, 8_192);
}

#[test]
fn self_info_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::SelfInfo,
        task_id: Some(3),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);

    let output = HostcallOutput::SelfInfo {
        process_id: 42,
        tenant: Some("acme".to_string()),
    };
    let encoded = encode_rkyv(&output).expect("encode");
    let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, output);
}

// scenario: selium-abi/certificate-signing-hostcall-variants/signtenantca-round-trips
#[test]
fn sign_tenant_ca_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::SignTenantCa {
            tenant: "acme".to_string(),
        },
        task_id: Some(1),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn tenant_ca_transaction_round_trips() {
    for request in [
        HostcallRequest::CommitTenantCa {
            tenant: "acme".to_string(),
            ca_cert_der: vec![0x30, 0x01],
        },
        HostcallRequest::RollbackTenantCa {
            tenant: "acme".to_string(),
            ca_cert_der: vec![0x30, 0x02],
        },
    ] {
        let envelope = HostcallEnvelope {
            request,
            task_id: None,
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }
}

// scenario: selium-abi/certificate-signing-hostcall-variants/signusercert-round-trips
#[test]
fn sign_user_cert_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::SignUserCert {
            tenant: "acme".to_string(),
            spki_der: vec![0x30, 0x82, 0x01, 0x02],
        },
        task_id: Some(2),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn revoke_ca_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::RevokeCa {
            tenant: "acme".to_string(),
        },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn certificate_output_round_trip() {
    let output = HostcallOutput::Certificate(vec![0x30, 0x03, 0x02, 0x01]);
    let encoded = encode_rkyv(&output).expect("encode");
    let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, output);
}

#[test]
fn authorise_attach_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::AuthoriseAttach {
            resource_class: ResourceClass::SharedRegion,
            resource_id: 7,
            client_process_id: 42,
        },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn declare_attachable_by_resolution_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::DeclareAttachableByResolution {
            resource_class: ResourceClass::HostQueue,
            resource_id: 9,
            scope: AttachDeclarationScope::Tenant("acme".to_string()),
        },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn record_resolved_resource_for_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::RecordResolvedResourceFor {
            resource_class: ResourceClass::SharedRegion,
            resource_id: 7,
            client_process_id: 42,
        },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn a_durable_slice_module_source_round_trips() {
    // The slice variant carries the whole module reference (volume, offset,
    // length) across the envelope; a truncated or reordered field would
    // spawn from the wrong bytes.
    let envelope = HostcallEnvelope {
        request: HostcallRequest::ProcessStart {
            source: ModuleSource::Slice {
                volume: "selium.control-plane.modules".to_string(),
                offset: 4096,
                len: 65_536,
            },
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
        },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn attach_declaration_scope_round_trip() {
    for scope in [
        AttachDeclarationScope::Any,
        AttachDeclarationScope::Tenant("acme".to_string()),
    ] {
        let encoded = encode_rkyv(&scope).expect("encode");
        let decoded: AttachDeclarationScope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, scope);
    }
}

#[test]
fn metadata_max_bytes_is_bounded() {
    // The metadata cap must stay a small, bounded budget: it bounds
    // kernel queue memory per pending handoff entry.
    const _: () = {
        assert!(METADATA_MAX_BYTES <= 64 * 1024);
        assert!(METADATA_MAX_BYTES >= client_identity::FINGERPRINT_LEN + 16);
    };
}

#[test]
fn tcp_bind_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::TcpBind {
            address: "127.0.0.1:8080".to_string(),
        },
        task_id: Some(1),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn tcp_connect_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::TcpConnect {
            address: "127.0.0.1:443".to_string(),
        },
        task_id: Some(2),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn capability_vocabulary_covers_every_variant() {
    // One list, one exhaustive name match: a new variant breaks the match
    // at compile time, and this test then fails until `ALL` lists it.
    let mut seen = std::collections::BTreeSet::new();
    for capability in Capability::ALL {
        assert!(
            seen.insert(capability.name()),
            "duplicate capability name {}",
            capability.name()
        );
        assert!(!capability.name().is_empty());
    }
    assert_eq!(
        seen.len(),
        Capability::ALL.len(),
        "every capability appears exactly once in ALL"
    );

    // The bootstrap-provisioned set is the non-delegable set; keep it in
    // declaration order so a reviewer can diff it.
    let provisioned: Vec<&str> = Capability::ALL
        .iter()
        .filter(|capability| capability.is_bootstrap_provisioned())
        .map(Capability::name)
        .collect();
    assert_eq!(
        provisioned,
        [
            "DelegateGrants",
            "RouteClaim",
            "MintCertificate",
            "QuotaWrite",
            "ProcessProbe",
            "DomainWrite",
        ]
    );
}

// scenario: selium-abi/cpu-resource-class/cpu-variant-joins-the-resource-vocabulary
#[test]
fn resource_class_uri_segments_cover_every_variant() {
    // Task 1.3: the segment vocabulary covers the entire closed set, and
    // the mapping round-trips. Documented names are asserted exactly.
    let every = [
        (ResourceClass::SharedRegion, "region"),
        (ResourceClass::SharedMapping, "mapping"),
        (ResourceClass::Signal, "signal"),
        (ResourceClass::TcpListener, "listener"),
        (ResourceClass::TcpStream, "stream"),
        (ResourceClass::UdpSocket, "socket"),
        (ResourceClass::Volume, "volume"),
        (ResourceClass::Process, "proc"),
        (ResourceClass::ActivityLog, "activity"),
        (ResourceClass::MeteringStream, "metering"),
        (ResourceClass::GuestLog, "guest-log"),
        (ResourceClass::HostQueue, "queue"),
        (ResourceClass::Cpu, "cpu"),
    ];
    for (class, expected) in every {
        assert_eq!(class.uri_segment(), expected, "segment for {class:?}");
        assert_eq!(
            ResourceClass::from_uri_segment(expected),
            Some(class),
            "inverse for {expected}"
        );
        assert!(expected.chars().all(|c| c.is_ascii_lowercase() || c == '-'));
        let _ = class;
    }
    assert_eq!(ResourceClass::from_uri_segment("unknown"), None);
}

#[test]
fn process_tenant_hostcall_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::ProcessTenant { process_id: 42 },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);

    let output = HostcallOutput::Tenant(Some("acme".to_string()));
    let encoded = encode_rkyv(&output).expect("encode");
    let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, output);
}

#[test]
fn resource_kind_round_trip() {
    for kind in [
        ResourceKind::LogChannel,
        ResourceKind::LiveTable,
        ResourceKind::RpcRing,
        ResourceKind::PubSubTopic,
        ResourceKind::NetworkBuffer,
        ResourceKind::SharedMemory,
    ] {
        let encoded = encode_rkyv(&kind).expect("encode");
        let decoded: ResourceKind = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, kind);
    }
}

#[test]
fn alloc_region_with_purpose_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::AllocRegion {
            pages: 8,
            prot: RegionProt::ReadWrite,
            purpose: ResourceKind::LogChannel,
            serving_tenant: Some("acme".to_string()),
        },
        task_id: Some(5),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn guest_log_register_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::GuestLogRegister { shared_id: 42 },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

// --- Network endpoint URI tests ---

#[test]
fn parse_tcp_endpoint() {
    let ep = NetworkEndpoint::parse("tcp://93.184.216.34:443").expect("parse");
    assert_eq!(ep.scheme, "tcp");
    assert_eq!(ep.host, "93.184.216.34");
    assert_eq!(ep.port, "443");
}

#[test]
fn parse_udp_endpoint() {
    let ep = NetworkEndpoint::parse("udp://10.0.0.5:53").expect("parse");
    assert_eq!(ep.scheme, "udp");
    assert_eq!(ep.host, "10.0.0.5");
    assert_eq!(ep.port, "53");
}

#[test]
fn parse_ipv6_bracketed() {
    let ep = NetworkEndpoint::parse("tcp://[2001:db8::1]:443").expect("parse");
    assert_eq!(ep.scheme, "tcp");
    assert_eq!(ep.host, "[2001:db8::1]");
    assert_eq!(ep.port, "443");
}

#[test]
fn parse_lowercases_host() {
    let ep = NetworkEndpoint::parse("tcp://EXAMPLE.COM:8080").expect("parse");
    assert_eq!(ep.host, "example.com");
}

#[test]
fn parse_strips_trailing_dot() {
    let ep = NetworkEndpoint::parse("tcp://example.com.:8080").expect("parse");
    assert_eq!(ep.host, "example.com");
}

#[test]
fn parse_rejects_non_network_scheme() {
    assert!(NetworkEndpoint::parse("sel://acme/payments/").is_none());
}

#[test]
fn parse_rejects_missing_port() {
    assert!(NetworkEndpoint::parse("tcp://127.0.0.1").is_none());
}

#[test]
fn socket_addr_format_ipv4() {
    let addr: std::net::SocketAddr = "93.184.216.34:443".parse().unwrap();
    let uri = format!("tcp://{addr}");
    assert_eq!(uri, "tcp://93.184.216.34:443");
}

#[test]
fn socket_addr_format_ipv6() {
    let addr: std::net::SocketAddr = "[2001:db8::1]:53".parse().unwrap();
    let uri = format!("tcp://{addr}");
    assert_eq!(uri, "tcp://[2001:db8::1]:53");
}

// --- Component-aware matching tests ---

#[test]
fn uri_prefix_exact_match_tcp() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://93.184.216.34:443".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx = ScopeContext {
        uri: Some("tcp://93.184.216.34:443".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&ctx));
}

// scenario: capability-enforcement/component-aware-uri-prefix-matching/port-wildcard
#[test]
fn uri_prefix_wildcard_port() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://127.0.0.1:*".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx = ScopeContext {
        uri: Some("tcp://127.0.0.1:8080".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&ctx));
}

#[test]
fn uri_prefix_wildcard_port_only_matches_127_0_0_1() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://127.0.0.1:*".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    // Different host — should NOT match.
    let ctx = ScopeContext {
        uri: Some("tcp://10.0.0.5:8080".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(!grant.allows(&ctx));
}

#[test]
fn uri_prefix_wildcard_label_boundary() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://*.example.com:443".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx = ScopeContext {
        uri: Some("tcp://foo.example.com:443".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&ctx));
}

#[test]
fn uri_prefix_wildcard_multi_label() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://*.example.com:443".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx = ScopeContext {
        uri: Some("tcp://bar.foo.example.com:443".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&ctx));
}

// scenario: capability-enforcement/component-aware-uri-prefix-matching/label-suffix-attack-rejected
#[test]
fn uri_prefix_rejects_label_suffix_attack() {
    // `tcp://example.com:443` must NOT match `tcp://example.com.evil.com:443`.
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://example.com:443".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx = ScopeContext {
        uri: Some("tcp://example.com.evil.com:443".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(!grant.allows(&ctx));
}

#[test]
fn uri_prefix_rejects_wildcard_partial_label_prefix() {
    // `*.example.com` must NOT match `badexample.com` (no dot before suffix).
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://*.example.com:443".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx = ScopeContext {
        uri: Some("tcp://badexample.com:443".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(!grant.allows(&ctx));
}

#[test]
fn uri_prefix_port_list() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("tcp://127.0.0.1:80,443".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx_80 = ScopeContext {
        uri: Some("tcp://127.0.0.1:80".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    let ctx_443 = ScopeContext {
        uri: Some("tcp://127.0.0.1:443".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    let ctx_8080 = ScopeContext {
        uri: Some("tcp://127.0.0.1:8080".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&ctx_80));
    assert!(grant.allows(&ctx_443));
    assert!(!grant.allows(&ctx_8080));
}

#[test]
fn uri_prefix_non_network_keeps_string_prefix() {
    // A non-`sel` URI keeps plain starts_with semantics.
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("selium.control".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ],
    );
    let ctx_match = ScopeContext {
        uri: Some("selium.control-plane".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    let ctx_no_match = ScopeContext {
        uri: Some("other.control-plane".to_string()),
        resource_class: Some(ResourceClass::TcpStream),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&ctx_match));
    assert!(!grant.allows(&ctx_no_match));
}

// scenario: capability-enforcement/component-aware-uri-prefix-matching/registry-prefix-respects-segment-boundaries
#[test]
fn registry_prefix_respects_segment_boundaries() {
    let grant = CapabilityGrant::new(
        Capability::Network,
        vec![
            ResourceSelector::UriPrefix("sel:///control".to_string()),
            ResourceSelector::ResourceClass(ResourceClass::HostQueue),
        ],
    );
    let same = ScopeContext {
        uri: Some("sel:///control".to_string()),
        resource_class: Some(ResourceClass::HostQueue),
        ..ScopeContext::default()
    };
    let nested = ScopeContext {
        uri: Some("sel:///control/status".to_string()),
        resource_class: Some(ResourceClass::HostQueue),
        ..ScopeContext::default()
    };
    let lookalike = ScopeContext {
        uri: Some("sel:///control-plane".to_string()),
        resource_class: Some(ResourceClass::HostQueue),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&same));
    assert!(grant.allows(&nested));
    assert!(
        !grant.allows(&lookalike),
        "a registry prefix must not match at a non-segment boundary"
    );
}

// scenario: capability-enforcement/exact-uri-selector/exact-uri-matches
// scenario: capability-enforcement/exact-uri-selector/longer-name-does-not-match
#[test]
fn exact_uri_selector_matches_only_the_canonical_name() {
    let selector = ResourceSelector::Uri("sel:///control".to_string());
    let exact = ScopeContext {
        uri: Some("sel:///control".to_string()),
        ..ScopeContext::default()
    };
    let longer = ScopeContext {
        uri: Some("sel:///control-plane".to_string()),
        ..ScopeContext::default()
    };
    let nested = ScopeContext {
        uri: Some("sel:///control/status".to_string()),
        ..ScopeContext::default()
    };
    assert!(selector.matches(&exact));
    assert!(!selector.matches(&longer));
    assert!(!selector.matches(&nested), "Uri is exact, never a prefix");
}

// scenario: capability-enforcement/exact-uri-selector/wire-name-is-normalised
#[test]
fn exact_uri_selector_normalises_wire_names_and_trailing_separators() {
    let selector = ResourceSelector::Uri("sel:///control".to_string());
    let wire = ScopeContext {
        uri: Some("control".to_string()),
        ..ScopeContext::default()
    };
    let trailing = ScopeContext {
        uri: Some("sel:///control/".to_string()),
        ..ScopeContext::default()
    };
    assert!(selector.matches(&wire));
    assert!(selector.matches(&trailing));
}

// scenario: selium-abi/route-claim-abi-variants/uri-selector-is-part-of-the-selector-vocabulary
#[test]
fn uri_selector_is_evaluatable() {
    let selector = ResourceSelector::Uri("sel:///control".to_string());
    assert!(selector.is_evaluatable(std::slice::from_ref(&selector)));
}

// scenario: selium-abi/route-claim-abi-variants/route-claim-variants-round-trip
#[test]
fn route_claim_grant_round_trips() {
    let grant = CapabilityGrant {
        capability: Capability::RouteClaim,
        selectors: vec![ResourceSelector::Uri("sel:///control".to_string())],
    };
    let encoded = encode_rkyv(&grant).expect("encode");
    let decoded: CapabilityGrant = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, grant);
}

// --- is_evaluatable tests ---

#[test]
fn uri_prefix_evaluatable_with_tcp_stream_class() {
    let selectors = vec![
        ResourceSelector::ResourceClass(ResourceClass::TcpStream),
        ResourceSelector::UriPrefix("tcp://127.0.0.1:443".to_string()),
    ];
    assert!(selectors[1].is_evaluatable(&selectors));
}

#[test]
fn uri_prefix_evaluatable_with_tcp_listener_class() {
    let selectors = vec![
        ResourceSelector::ResourceClass(ResourceClass::TcpListener),
        ResourceSelector::UriPrefix("tcp://0.0.0.0:8080".to_string()),
    ];
    assert!(selectors[1].is_evaluatable(&selectors));
}

#[test]
fn uri_prefix_evaluatable_with_udp_socket_class() {
    let selectors = vec![
        ResourceSelector::ResourceClass(ResourceClass::UdpSocket),
        ResourceSelector::UriPrefix("udp://10.0.0.5:53".to_string()),
    ];
    assert!(selectors[1].is_evaluatable(&selectors));
}

#[test]
fn uri_prefix_evaluatable_with_volume_class() {
    let selectors = vec![
        ResourceSelector::ResourceClass(ResourceClass::Volume),
        ResourceSelector::UriPrefix("selium.control-plane".to_string()),
    ];
    assert!(selectors[1].is_evaluatable(&selectors));

    // The prefix scopes which volume names the grant admits: plain
    // `starts_with` matching on the volume name.
    let grant = CapabilityGrant::new(Capability::Storage, selectors);
    let admitted = ScopeContext {
        resource_class: Some(ResourceClass::Volume),
        uri: Some("selium.control-plane.modules".to_string()),
        ..ScopeContext::default()
    };
    let refused = ScopeContext {
        resource_class: Some(ResourceClass::Volume),
        uri: Some("acme.private".to_string()),
        ..ScopeContext::default()
    };
    assert!(grant.allows(&admitted));
    assert!(!grant.allows(&refused));
}

#[test]
fn uri_prefix_not_evaluatable_without_an_applicable_class() {
    let selectors = vec![
        ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
        ResourceSelector::UriPrefix("tcp://10.0.0.5:443".to_string()),
    ];
    assert!(!selectors[1].is_evaluatable(&selectors));
}

#[test]
fn uri_prefix_not_evaluatable_with_empty_selectors() {
    let selectors = vec![ResourceSelector::UriPrefix(
        "tcp://10.0.0.5:443".to_string(),
    )];
    assert!(!selectors[0].is_evaluatable(&selectors));
}

#[test]
fn other_selectors_always_evaluatable() {
    let empty: &[ResourceSelector] = &[];
    assert!(ResourceSelector::Tenant("acme".to_string()).is_evaluatable(empty));
    assert!(ResourceSelector::Locality(LocalityScope::Cluster).is_evaluatable(empty));
    assert!(ResourceSelector::ResourceClass(ResourceClass::SharedRegion).is_evaluatable(empty));
    assert!(ResourceSelector::ExplicitResource(ResourceIdentity::Shared(1)).is_evaluatable(empty));
    assert!(ResourceSelector::Children.is_evaluatable(empty));
}

// scenario: selium-abi/waitregister-hostcall/round-trip-encoding
#[test]
fn wait_register_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::WaitRegister {
            region_id: 42,
            generation: 7,
        },
        task_id: Some(3),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

// scenario: selium-abi/quota-hostcall-variants/quotaset-round-trips
#[test]
fn quota_set_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::QuotaSet {
            tenant: "acme".to_string(),
            class: ResourceClass::SharedRegion,
            limit: 1_073_741_824,
        },
        task_id: None,
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

#[test]
fn quota_clear_round_trip() {
    let envelope = HostcallEnvelope {
        request: HostcallRequest::QuotaClear {
            tenant: "acme".to_string(),
            class: ResourceClass::Volume,
        },
        task_id: Some(7),
    };
    let encoded = encode_rkyv(&envelope).expect("encode");
    let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
    assert_eq!(decoded, envelope);
}

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `selium-abi` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `addressing` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.
