//! Connection-time address policy for remotely requested crawls.
use spider::client::dns::{Addrs, Name, Resolve, Resolving};
use std::{net::IpAddr, sync::Arc};
use url::Url;

pub fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_broadcast()
                && !ip.is_multicast()
                && !ip.is_documentation()
                && a != 0
                && a < 224
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 198 && (18..=19).contains(&b))
                && !(a == 192 && b == 0 && c == 0)
                && !(a == 192 && b == 88 && c == 99)
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return public_address(IpAddr::V4(mapped));
            }
            let s = ip.segments();
            s[0] & 0xe000 == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x0200 || s[1] == 0x0db8))
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

pub fn validate_public_url(url: &Url) -> anyhow::Result<()> {
    super::validate_url(url)?;
    if let Some(host) = url.host() {
        match host {
            url::Host::Ipv4(ip) => anyhow::ensure!(
                public_address(ip.into()),
                "non-public crawl address blocked"
            ),
            url::Host::Ipv6(ip) => anyhow::ensure!(
                public_address(ip.into()),
                "non-public crawl address blocked"
            ),
            url::Host::Domain(name) => anyhow::ensure!(
                name != "localhost" && !name.ends_with(".localhost"),
                "non-public crawl host blocked"
            ),
        }
    }
    Ok(())
}

pub(super) struct PublicResolver;
impl Resolve for PublicResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses: Vec<_> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if addresses.is_empty() || addresses.iter().any(|a| !public_address(a.ip())) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "non-public DNS answer blocked",
                )
                .into());
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}
pub(super) fn resolver() -> Arc<PublicResolver> {
    Arc::new(PublicResolver)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn url_and_address_boundaries_include_alternate_ip_spellings() {
        for raw in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.1.1",
            "192.168.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "198.18.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "192.0.2.1",
            "::1",
            "::ffff:127.0.0.1",
            "fe80::1",
            "fc00::1",
            "2001:db8::1",
            "3fff::1",
        ] {
            assert!(!public_address(raw.parse().unwrap()), "{raw}");
        }
        for raw in [
            "8.8.8.8",
            "1.1.1.1",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(public_address(raw.parse().unwrap()), "{raw}");
        }
        for raw in [
            "http://2130706433/",
            "http://0x7f000001/",
            "http://127.1/",
            "http://localhost/",
            "http://a.localhost/",
            "http://[::ffff:127.0.0.1]/",
            "file:///tmp/x",
            "https://user:pass@example.com/",
        ] {
            assert!(
                validate_public_url(&Url::parse(raw).unwrap()).is_err(),
                "{raw}"
            );
        }
    }
    #[tokio::test]
    async fn resolver_blocks_private_dns_answers_at_connection_time() {
        assert!(
            PublicResolver
                .resolve("localhost".parse().unwrap())
                .await
                .is_err()
        );
    }
}
