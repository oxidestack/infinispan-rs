//! What upstream learns about a listener by reflection, as data: the `@Listener` attributes of the class,
//! and per public method its annotations, parameters and return type (`notifications/Listener.java`,
//! `notifications/impl/AbstractListenerImpl.java:310-380`).
//!
//! Rust has no annotations, so a listener is a [`ListenerDefinition`] built by hand; the checks
//! `AbstractListenerImpl` runs while registering one are ported on that data:
//! [`test_listener_class_validity`] (the class must carry `@Listener`),
//! [`test_listener_method_validity`] (exactly one parameter, assignable from the event class, return type
//! void or a `CompletionStage`), the skipping of non-public methods, and which annotations a listener may
//! use (`CacheNotifierImpl.getAllowedMethodAnnotations`, `:154-171`, `:277-283`).
//!
//! A cluster listener (`@Listener(clustered = true)`) is limited to `@CacheEntryCreated`,
//! `@CacheEntryModified`, `@CacheEntryRemoved` and `@CacheEntryExpired` (`clusterAllowedListeners`).
//! Methods carrying any other annotation (MockServer's `@CacheEntriesEvicted` handler) are **not even
//! looked at**: they are not validated and not registered, silently (`AbstractListenerImpl.java:216-222`
//! iterates `getAllowedMethodAnnotations(l)` only). That silence is the behaviour MockServer lives with.

use std::fmt;
use std::sync::Arc;

use super::event::{Event, EventClass, EventKind};

/// `Listener.Observation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// Only before the change.
    Pre,
    /// Only after the change.
    Post,
    /// Before and after the change.
    Both,
}

impl Observation {
    /// `Observation.shouldInvoke(pre)`.
    pub fn should_invoke(self, pre: bool) -> bool {
        match self {
            Observation::Pre => pre,
            Observation::Post => !pre,
            Observation::Both => true,
        }
    }
}

/// The attributes of `@Listener`, with upstream's defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerAttrs {
    /// Whether the listener runs on the thread making the change (default true).
    pub sync: bool,
    /// Whether only the primary owner of a key notifies (default false).
    pub primary_only: bool,
    /// Whether the listener hears changes made on every node (default false).
    pub clustered: bool,
    /// Whether the listener receives the existing entries as creations when registered (default false).
    pub include_current_state: bool,
    /// Before, after or both (default both).
    pub observation: Observation,
}

impl Default for ListenerAttrs {
    fn default() -> Self {
        ListenerAttrs { sync: true, primary_only: false, clustered: false, include_current_state: false, observation: Observation::Both }
    }
}

impl ListenerAttrs {
    /// `@Listener(clustered = true)`.
    pub fn clustered() -> Self {
        ListenerAttrs { clustered: true, ..Default::default() }
    }
}

/// A listener method's return type: only void and `CompletionStage` are valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnType {
    /// `void`.
    Void,
    /// `CompletionStage`.
    CompletionStage,
    /// Any other type, by name: an invalid listener.
    Other(String),
}

/// The body of a listener method. An `Err` is the exception the method threw.
pub type ListenerHandler = Arc<dyn Fn(&Event) -> Result<(), String> + Send + Sync>;

/// One method of a listener class.
#[derive(Clone)]
pub struct ListenerMethod {
    /// The method's name.
    pub name: String,
    /// `Class.getMethods()` returns public methods only
    pub public: bool,
    /// the event annotations on the method (`@CacheEntryVisited @CacheEntryRemoved` is two)
    pub annotations: Vec<EventKind>,
    /// The method's parameter types.
    pub params: Vec<EventClass>,
    /// The method's return type.
    pub returns: ReturnType,
    /// The method's body.
    pub handler: ListenerHandler,
}

impl ListenerMethod {
    /// A public `void name(Event e)` method annotated with `annotations`: the shape of a valid listener.
    pub fn new(name: &str, annotations: &[EventKind], handler: impl Fn(&Event) + Send + Sync + 'static) -> Self {
        ListenerMethod {
            name: name.to_string(),
            public: true,
            annotations: annotations.to_vec(),
            params: vec![EventClass::Event],
            returns: ReturnType::Void,
            handler: Arc::new(move |e| {
                handler(e);
                Ok(())
            }),
        }
    }

    /// A method whose body may throw.
    pub fn fallible(name: &str, annotations: &[EventKind], handler: impl Fn(&Event) -> Result<(), String> + Send + Sync + 'static) -> Self {
        ListenerMethod { handler: Arc::new(handler), ..ListenerMethod::new(name, annotations, |_| {}) }
    }

    /// Sets the parameter types.
    pub fn with_params(mut self, params: Vec<EventClass>) -> Self {
        self.params = params;
        self
    }

    /// Sets the return type.
    pub fn with_returns(mut self, returns: ReturnType) -> Self {
        self.returns = returns;
        self
    }

    /// Makes the method non-public, so registration ignores it.
    pub fn non_public(mut self) -> Self {
        self.public = false;
        self
    }
}

impl fmt::Debug for ListenerMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({:?}) -> {:?} @{:?}", self.name, self.params, self.returns, self.annotations)
    }
}

/// A listener object: its class name, its `@Listener` annotation (`None` when the class has none) and its
/// methods. Identity (`Arc::ptr_eq`) is `listener.equals(li.getTarget())`.
pub struct ListenerDefinition {
    /// The listener's class name.
    pub class_name: String,
    /// The `@Listener` annotation, or `None` when the class has none.
    pub listener: Option<ListenerAttrs>,
    /// The listener's methods.
    pub methods: Vec<ListenerMethod>,
}

impl ListenerDefinition {
    /// A class annotated `@Listener` with the given attributes.
    pub fn new(class_name: &str, attrs: ListenerAttrs, methods: Vec<ListenerMethod>) -> Arc<Self> {
        Arc::new(ListenerDefinition { class_name: class_name.to_string(), listener: Some(attrs), methods })
    }

    /// A class without `@Listener`.
    pub fn unannotated(class_name: &str, methods: Vec<ListenerMethod>) -> Arc<Self> {
        Arc::new(ListenerDefinition { class_name: class_name.to_string(), listener: None, methods })
    }
}

impl fmt::Debug for ListenerDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.class_name)
    }
}

/// What registration, removal and invocation can fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotifierError {
    /// `IncorrectListenerException`
    IncorrectListener(String),
    /// a `CacheException`
    Cache(String),
    /// `UnsupportedOperationException`
    UnsupportedOperation(String),
    /// A feature of the notifier this port does not carry (listed in the module docs of `cache_notifier`).
    NotPorted(String),
    /// `CacheListenerException`: a synchronous listener method threw (`Log.exceptionInvokingListener`, ISPN000280)
    Listener(String),
}

impl fmt::Display for NotifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifierError::IncorrectListener(m) | NotifierError::Cache(m) | NotifierError::UnsupportedOperation(m) | NotifierError::NotPorted(m) | NotifierError::Listener(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for NotifierError {}

/// `AbstractListenerImpl.testListenerClassValidity`.
pub fn test_listener_class_validity(def: &ListenerDefinition) -> Result<ListenerAttrs, NotifierError> {
    def.listener.ok_or_else(|| NotifierError::IncorrectListener(format!("Cache listener class {} must be annotated with org.infinispan.notifications.Listener", def.class_name)))
}

/// `AbstractListenerImpl.testListenerMethodValidity` (`:360-369`).
pub fn test_listener_method_validity(m: &ListenerMethod, allowed_parameter: EventKind, annotation: EventKind) -> Result<(), NotifierError> {
    let name = annotation.annotation_name();
    if m.params.len() != 1 || !m.params[0].is_assignable_from(allowed_parameter) {
        return Err(NotifierError::IncorrectListener(format!(
            "Methods annotated with {name} must accept exactly one parameter, of assignable from type {}",
            allowed_parameter.event_class_name()
        )));
    }
    if !matches!(m.returns, ReturnType::Void | ReturnType::CompletionStage) {
        return Err(NotifierError::IncorrectListener(format!("Methods annotated with {name} should have a return type of void or CompletionStage.")));
    }
    Ok(())
}

/// `CacheNotifierImpl.clusterAllowedListeners` (`:166-169`).
pub const CLUSTER_ALLOWED: [EventKind; 4] = [EventKind::CacheEntryCreated, EventKind::CacheEntryModified, EventKind::CacheEntryRemoved, EventKind::CacheEntryExpired];

/// `CacheNotifierImpl.getAllowedMethodAnnotations(l)`: a cluster listener gets the subset.
pub fn allowed_annotations(attrs: &ListenerAttrs) -> &'static [EventKind] {
    if attrs.clustered { &CLUSTER_ALLOWED } else { &EventKind::ALL }
}

/// The annotation/method pairs a listener registers: for every public method, for every allowed annotation
/// it carries, the method is validated and kept (`validateAndAddListenerInvocations`, `:195-239`).
/// A failed validation aborts the whole registration with nothing registered, as the invocations of that
/// call are built before any is added here (upstream adds one by one, so an earlier valid method of the
/// same class stays registered when a later one throws; this port registers atomically, which no upstream
/// test distinguishes).
pub fn collect_invocations<'a>(def: &'a ListenerDefinition, attrs: &ListenerAttrs) -> Result<Vec<(EventKind, &'a ListenerMethod)>, NotifierError> {
    let allowed = allowed_annotations(attrs);
    let mut found = Vec::new();
    for m in def.methods.iter().filter(|m| m.public) {
        for &kind in allowed {
            if m.annotations.contains(&kind) {
                test_listener_method_validity(m, kind, kind)?;
                found.push((kind, m));
            }
        }
    }
    Ok(found)
}
