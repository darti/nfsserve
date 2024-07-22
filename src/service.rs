use std::{io, net::SocketAddr, sync::Arc};

use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::mpsc,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info};

use crate::{context::RPCContext, tcp::process_socket, vfs::NFSFileSystem};

#[derive(Clone)]
pub struct NFSService<T>
where
    T: NFSFileSystem + Send + Sync + 'static,
{
    fs: Arc<T>,
    mount_signal: Option<mpsc::Sender<bool>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSService<T> {
    pub fn new(fs: T) -> Self {
        Self {
            fs: Arc::new(fs),
            mount_signal: None,
            cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn stop(&self) -> TaskTracker {
        self.cancellation_token.cancelled().await;
        self.task_tracker.clone()
    }

    pub async fn handle<A>(&self, addr: A) -> io::Result<()>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;

        self.task_tracker.close();

        loop {
            let (socket, _) = tokio::select! {
                res = listener.accept() => res?,
                _ = self.cancellation_token.cancelled() => break,
            };

            let port = match listener.local_addr().unwrap() {
                SocketAddr::V4(s) => s.port(),
                SocketAddr::V6(s) => s.port(),
            };

            let context = RPCContext {
                local_port: port,
                client_addr: socket.peer_addr().unwrap().to_string(),
                auth: crate::rpc::auth_unix::default(),
                vfs: self.fs.clone(),
                mount_signal: self.mount_signal.clone(),
                cancellation_token: self.cancellation_token.clone(),
            };

            debug!("Accepting socket {:?} {:?}", socket, context);

            self.task_tracker.spawn(async move {
                let _ = process_socket(socket, context).await;
                info!("Stopped socked processing");
            });
        }

        info!("Service stopped");

        Ok(())
    }
}
