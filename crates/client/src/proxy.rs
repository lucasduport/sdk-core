use base64::prelude::*;
use hyper_util::{
    client::legacy::connect::{Connected, Connection, proxy::Tunnel},
    rt::TokioIo,
};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tonic::transport::{Channel, Endpoint};
use tower::Service;

#[cfg(unix)]
use tokio::net::UnixStream;

/// Options for HTTP CONNECT proxy.
#[derive(Clone, Debug)]
pub struct HttpConnectProxyOptions {
    /// The host:port to proxy through for TCP, or unix:/path/to/unix.sock for
    /// Unix socket (which means it must start with "unix:/").
    pub target_addr: String,
    /// Optional HTTP basic auth for the proxy as user/pass tuple.
    pub basic_auth: Option<(String, String)>,
}

impl HttpConnectProxyOptions {
    /// Create a channel from the given endpoint that uses the HTTP CONNECT proxy.
    pub async fn connect_endpoint(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Channel, tonic::transport::Error> {
        // Build the proxy URI. OverrideAddrConnector ignores this URI and always
        // connects to self.target_addr, but Tunnel needs a valid URI to pass to
        // the inner connector's call().
        let proxy_uri: tonic::transport::Uri = if self.target_addr.starts_with("unix:/") {
            // Unix socket — use a placeholder URI since OverrideAddrConnector
            // ignores the URI anyway.
            "http://localhost".parse().unwrap()
        } else {
            format!("http://{}", self.target_addr)
                .parse()
                .unwrap_or_else(|e| {
                    warn!(
                        target_addr = %self.target_addr,
                        error = %e,
                        "Failed to parse proxy target_addr as URI, falling back to localhost"
                    );
                    "http://localhost".parse().unwrap()
                })
        };

        let connector = OverrideAddrConnector(self.target_addr.clone());
        let mut tunnel = Tunnel::new(proxy_uri, connector);

        if let Some((user, pass)) = &self.basic_auth {
            let creds = BASE64_STANDARD.encode(format!("{user}:{pass}"));
            let auth = http::header::HeaderValue::from_str(&format!("Basic {creds}"))
                .expect("valid base64 produces valid header value");
            tunnel = tunnel.with_auth(auth);
        }

        endpoint.connect_with_connector(tunnel).await
    }
}

#[derive(Clone)]
struct OverrideAddrConnector(String);

impl Service<tonic::transport::Uri> for OverrideAddrConnector {
    type Response = TokioIo<ProxyStream>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _ctx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: tonic::transport::Uri) -> Self::Future {
        let target_addr = self.0.clone();
        let fut = async move {
            Ok(TokioIo::new(
                ProxyStream::connect(target_addr.as_str()).await?,
            ))
        };
        Box::pin(fut)
    }
}

/// Visible only for tests
#[doc(hidden)]
pub enum ProxyStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl ProxyStream {
    async fn connect(target_addr: &str) -> anyhow::Result<Self> {
        if target_addr.starts_with("unix:/") {
            #[cfg(unix)]
            {
                Ok(ProxyStream::Unix(
                    UnixStream::connect(&target_addr[5..]).await?,
                ))
            }
            #[cfg(not(unix))]
            {
                Err(anyhow::anyhow!(
                    "Unix sockets are not supported on this platform"
                ))
            }
        } else {
            Ok(ProxyStream::Tcp(TcpStream::connect(target_addr).await?))
        }
    }
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            ProxyStream::Tcp(s) => s.is_write_vectored(),
            #[cfg(unix)]
            ProxyStream::Unix(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Connection for ProxyStream {
    fn connected(&self) -> Connected {
        match self {
            ProxyStream::Tcp(s) => s.connected(),
            // There is no special connected metadata for Unix sockets
            #[cfg(unix)]
            ProxyStream::Unix(_) => Connected::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    struct CapturedConnect {
        request_line: String,
        headers: Vec<String>,
    }

    // Starts a mock TCP proxy that accepts one connection, captures the
    // CONNECT request, and replies 200. Returns the proxy address and a
    // handle to retrieve the captured request.
    async fn mock_proxy() -> (String, tokio::task::JoinHandle<CapturedConnect>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).await.unwrap();
            let mut headers = Vec::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                headers.push(line.trim_end().to_string());
            }
            reader
                .into_inner()
                .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
                .await
                .unwrap();
            CapturedConnect {
                request_line,
                headers,
            }
        });
        (addr, handle)
    }

    fn make_tunnel(proxy_addr: &str) -> Tunnel<OverrideAddrConnector> {
        let proxy_uri: tonic::transport::Uri = format!("http://{proxy_addr}").parse().unwrap();
        Tunnel::new(proxy_uri, OverrideAddrConnector(proxy_addr.to_string()))
    }

    #[tokio::test]
    async fn connect_includes_port_for_https() {
        let (proxy_addr, handle) = mock_proxy().await;
        let tunnel = make_tunnel(&proxy_addr);
        let uri: tonic::transport::Uri = "https://example.com/some/path".parse().unwrap();
        let _ = tower::ServiceExt::oneshot(tunnel, uri).await.unwrap();

        let captured = handle.await.unwrap();
        assert_eq!(
            captured.request_line.trim(),
            "CONNECT example.com:443 HTTP/1.1"
        );
    }

    // Tunnel defaults to port 443 when no port is present in the URI, regardless
    // of scheme, because CONNECT is almost exclusively used for TLS tunnelling.
    #[tokio::test]
    async fn connect_defaults_to_443_without_port() {
        let (proxy_addr, handle) = mock_proxy().await;
        let tunnel = make_tunnel(&proxy_addr);
        let uri: tonic::transport::Uri = "http://example.com".parse().unwrap();
        let _ = tower::ServiceExt::oneshot(tunnel, uri).await.unwrap();

        let captured = handle.await.unwrap();
        assert_eq!(
            captured.request_line.trim(),
            "CONNECT example.com:443 HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn connect_preserves_explicit_port() {
        let (proxy_addr, handle) = mock_proxy().await;
        let tunnel = make_tunnel(&proxy_addr);
        let uri: tonic::transport::Uri = "https://example.com:7233".parse().unwrap();
        let _ = tower::ServiceExt::oneshot(tunnel, uri).await.unwrap();

        let captured = handle.await.unwrap();
        assert_eq!(
            captured.request_line.trim(),
            "CONNECT example.com:7233 HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn connect_includes_basic_auth() {
        let (proxy_addr, handle) = mock_proxy().await;
        let creds = BASE64_STANDARD.encode("user:pass");
        let auth = http::header::HeaderValue::from_str(&format!("Basic {creds}")).unwrap();
        let tunnel = make_tunnel(&proxy_addr).with_auth(auth);
        let uri: tonic::transport::Uri = "https://example.com:7233".parse().unwrap();
        let _ = tower::ServiceExt::oneshot(tunnel, uri).await.unwrap();

        let captured = handle.await.unwrap();
        let auth_header = captured
            .headers
            .iter()
            .find(|h| h.to_lowercase().starts_with("proxy-authorization:"))
            .expect("missing proxy-authorization header");
        assert_eq!(
            auth_header.trim(),
            format!("Proxy-Authorization: Basic {creds}")
        );
    }

    #[tokio::test]
    async fn connect_host_header_matches_target() {
        let (proxy_addr, handle) = mock_proxy().await;
        let tunnel = make_tunnel(&proxy_addr);
        let uri: tonic::transport::Uri = "https://example.com:7233".parse().unwrap();
        let _ = tower::ServiceExt::oneshot(tunnel, uri).await.unwrap();

        let captured = handle.await.unwrap();
        let host = captured
            .headers
            .iter()
            .find(|h| h.to_lowercase().starts_with("host:"))
            .expect("missing host header");
        assert_eq!(host.trim(), "Host: example.com:7233");
    }
}
