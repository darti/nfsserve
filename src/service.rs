use std::{io, net::SocketAddr, sync::Arc};

use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::mpsc,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info};

use crate::{context::RPCContext, tcp::process_socket, vfs::NFSFileSystem};

#[derive(Clone)]
pub struct NFSService<T>
where
    T: NFSFileSystem + Send + Sync + 'static,
{
    fs: Arc<T>,
    listener: Arc<TcpListener>,
    mount_signal: Option<mpsc::Sender<bool>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSService<T> {
    pub async fn new<A>(fs: T, addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self {
            fs: Arc::new(fs),
            listener: Arc::new(listener),
            mount_signal: None,
            cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    pub async fn stop(&self) -> TaskTracker {
        self.cancellation_token.cancelled().await;
        self.task_tracker.clone()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub async fn handle(&self) -> io::Result<()> {
        self.task_tracker.close();

        loop {
            let (socket, _) = tokio::select! {
                res = self.listener.accept() => res?,
                _ = self.cancellation_token.cancelled() => break,
            };

            let context = RPCContext {
                local_port: self.local_addr().port(),
                client_addr: socket.peer_addr().unwrap().to_string(),
                auth: crate::rpc::auth_unix::default(),
                vfs: self.fs.clone(),
                mount_signal: self.mount_signal.clone(),
                cancellation_token: self.cancellation_token.clone(),
            };

            debug!("Accepting socket {:?} {:?}", socket, context);

            self.task_tracker.spawn(async move {
                match process_socket(socket, context).await {
                    Ok(_) => info!("exiting"),
                    Err(e) => error!("Socket processing error: {}", e),
                };
                info!("Stopped socked processing");
            });
        }

        info!("Service stopped");

        Ok(())
    }
}
