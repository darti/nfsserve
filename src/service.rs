use std::{io, net::SocketAddr, sync::Arc};

use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::mpsc,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info};

use crate::{context::RPCContext, tcp::process_socket, vfs::NFSFileSystem};

#[derive(Clone, Debug)]
pub struct NFSService<T>
where
    T: NFSFileSystem + Send + Sync + 'static,
{
    fs: Arc<T>,
    listener: Arc<TcpListener>,
    mount_signal: Option<mpsc::Sender<bool>>,
    pub cancellation_token: CancellationToken,
    pub task_tracker: TaskTracker,
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSService<T> {
    pub async fn new<A>(
        fs: T,
        addr: A,
        cancellation_token: Option<CancellationToken>,
        task_tracker: Option<TaskTracker>,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self {
            fs: Arc::new(fs),
            listener: Arc::new(listener),
            mount_signal: None,
            cancellation_token: cancellation_token.unwrap_or_else(|| CancellationToken::new()),
            task_tracker: task_tracker.unwrap_or_else(|| TaskTracker::new()),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub async fn handle(&self) -> io::Result<()> {
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => break,
                res = self.listener.accept() => {
                   let (socket, _) =  res?;
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
                           Ok(_) => (),
                           Err(e) => error!("Socket processing error: {}", e),
                       };
                   });
                },
            }
        }

        info!("Service stopped");

        Ok(())
    }
}
