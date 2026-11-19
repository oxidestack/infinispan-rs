//! The slice of `org.infinispan.remoting.transport.Transport` the topology managers use, plus the
//! local-invocation hook `TopologyManagementHelper` calls for "execute on self".

use std::time::Duration;

use super::cluster_cache_status::TopologyError;
use super::commands::TopologyCommand;
use super::messages::{Response, ResponseValue};
use super::support::BoxFuture;
use crate::ch::address::Address;

/// How a remote invocation failed, in the categories the managers branch on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteError {
    /// `SuspectException`: the target left, or answered `CacheNotFoundResponse`.
    Suspect(String),
    /// `TimeoutException`.
    Timeout,
    /// Any other failure (an `ExceptionResponse`, a marshalling error...).
    Other(String),
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteError::Suspect(m) => write!(f, "SuspectException: {m}"),
            RemoteError::Timeout => write!(f, "TimeoutException"),
            RemoteError::Other(m) => f.write_str(m),
        }
    }
}

impl From<RemoteError> for TopologyError {
    fn from(e: RemoteError) -> Self {
        TopologyError(e.to_string())
    }
}

/// What the managers need from the cluster transport. Commands travel as [`TopologyCommand`]s;
/// implementations marshal them with [`TopologyCommand::to_bytes`] and answers with
/// `ObjectOutput::write_response`.
pub trait TopologyTransport: Send + Sync {
    /// The local node's address.
    fn address(&self) -> Address;
    /// The id of the installed view.
    fn view_id(&self) -> i32;
    /// The members of the installed view, coordinator first.
    fn members(&self) -> Vec<Address>;
    /// `Transport.getCoordinator`: the first member of the view.
    fn coordinator(&self) -> Address {
        self.members().first().cloned().expect("a view has members")
    }
    /// Whether the local node is the coordinator.
    fn is_coordinator(&self) -> bool {
        self.coordinator() == self.address()
    }
    /// `Transport.withView`: completes once the installed view id is at least `view_id`.
    fn with_view(&self, view_id: i32) -> BoxFuture<'static, ()>;
    /// `Transport.invokeCommand(target, ...)`.
    fn invoke_command(&self, target: &Address, command: TopologyCommand, timeout: Duration) -> BoxFuture<'static, Result<Response, RemoteError>>;
    /// `Transport.invokeCommandOnAll(command, ...)` / `invokeCommandOnAll(requiredTargets, ...)`:
    /// the command goes to every member, and the answers of `targets` (default: every member except
    /// the local node, which is always excluded, `JGroupsTransport.java:1095`) are collected in
    /// member order. A required target that is not in the view yields [`RemoteError::Suspect`].
    fn invoke_command_on_all(
        &self,
        targets: Option<Vec<Address>>,
        command: TopologyCommand,
        timeout: Duration,
    ) -> BoxFuture<'static, Vec<(Address, Result<Response, RemoteError>)>>;
    /// `Transport.sendToAll`: fire and forget, every member except the local node.
    fn send_to_all(&self, command: TopologyCommand);
    /// `Transport.sendTo`: fire and forget.
    fn send_to(&self, target: &Address, command: TopologyCommand);
}

/// `GlobalRpcCommand.invokeAsync(gcr)`: run a command on the local node.
pub trait LocalInvoker: Send + Sync {
    /// Runs `command` from `origin` on the local node and returns its answer.
    fn invoke(&self, origin: Address, command: TopologyCommand) -> BoxFuture<'static, Result<Option<ResponseValue>, TopologyError>>;
}
