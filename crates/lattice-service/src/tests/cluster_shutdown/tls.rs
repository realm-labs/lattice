use lattice_remoting::{endpoint::EndpointSecurity, handshake::NodeIdentity};
use rcgen::{CertificateParams, KeyPair, SanType};
use std::sync::Arc;
use tokio_rustls::rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};

fn certificate(node: &NodeIdentity) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let mut params = CertificateParams::new(vec!["lattice.test".to_owned()]).unwrap();
    params.subject_alt_names.push(SanType::URI(
        format!(
            "spiffe://{}/node/{}/{:032x}",
            node.cluster_id.as_str(),
            node.node_id,
            node.incarnation.get()
        )
        .try_into()
        .unwrap(),
    ));
    let key = KeyPair::generate().unwrap();
    let certificate = params.self_signed(&key).unwrap();
    (
        certificate.der().clone(),
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
    )
}

fn security(
    provider: Arc<CryptoProvider>,
    trusted: CertificateDer<'static>,
    own: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> EndpointSecurity {
    let mut roots = RootCertStore::empty();
    roots.add(trusted).unwrap();
    let roots = Arc::new(roots);
    let client = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots.clone())
        .with_client_auth_cert(vec![own.clone()], key.clone_key())
        .unwrap();
    let verifier = WebPkiClientVerifier::builder_with_provider(roots, provider.clone())
        .build()
        .unwrap();
    let server = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![own], key)
        .unwrap();
    EndpointSecurity {
        client: Arc::new(client),
        server: Arc::new(server),
        server_name: "lattice.test".to_owned(),
    }
}

pub(crate) fn pair(
    first: &NodeIdentity,
    second: &NodeIdentity,
) -> (EndpointSecurity, EndpointSecurity) {
    #[cfg(feature = "rustls-ring")]
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    #[cfg(all(not(feature = "rustls-ring"), feature = "rustls-aws-lc"))]
    let provider = Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());
    let (a, ak) = certificate(first);
    let (b, bk) = certificate(second);
    (
        security(provider.clone(), b.clone(), a.clone(), ak),
        security(provider, a, b, bk),
    )
}
