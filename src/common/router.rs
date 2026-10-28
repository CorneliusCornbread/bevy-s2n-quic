//! Routes driver events to the components they concern.
//!
//! Every connection and stream has a route, keyed by the driver's IDs. While
//! its component is on an entity, the route holds that entity and events are
//! applied to the component directly. Otherwise (before it's spawned, or while
//! it's held by an attempt or a server) events wait in the route and are
//! applied when the component is inserted. Component hooks keep the entity up
//! to date, so the per-frame cost depends on the number of events, not on the
//! number of connections and streams.
//!
//! Objects tell the router they've been dropped with a [`Notice`], and the
//! route is removed once nothing more can arrive for it.

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    mem,
    sync::PoisonError,
    time::Instant,
};

use bevy::ecs::{
    change_detection::DetectChangesMut,
    component::{Component, Mutable},
    entity::Entity,
    lifecycle::HookContext,
    resource::Resource,
    system::{Query, Res, ResMut, SystemParam},
    world::DeferredWorld,
};
use bytes::Bytes;

use crate::{
    common::{
        QuicParentId,
        connection::{
            QuicConnection, QuicConnectionAttempt, id::ConnectionId,
            stats::ConnectionStats,
        },
        error::{AppErrorCode, ConnectionError},
        runtime::QuicRuntime,
        stream::{
            QuicBidirectionalStreamAttempt, QuicPeerStream, QuicSendStreamAttempt,
            id::StreamId, receive::QuicReceiveStream, send::QuicSendStream,
        },
    },
    driver::{
        Command, ConnId, EndpointId, Event, StreamId as DriverStreamId, StreamInfo,
    },
    server::QuicServer,
};

/// Tells the router about objects created or dropped outside the poll system.
pub(crate) enum Notice {
    /// A server endpoint was opened.
    Endpoint {
        endpoint: EndpointId,
        parent: QuicParentId,
    },
    /// A server was dropped.
    EndpointReleased {
        endpoint: EndpointId,
    },
    /// A client started connecting.
    Conn {
        conn: ConnId,
        id: ConnectionId,
        driver: usize,
    },
    ConnReleased {
        conn: ConnId,
    },
    HalfReleased {
        conn: ConnId,
        stream: DriverStreamId,
        half: Half,
    },
    /// A stream open attempt was dropped before it resolved.
    OpenCancelled {
        conn: ConnId,
        request: u64,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Half {
    Send,
    Recv,
}

pub(crate) enum ConnDelivery {
    Connected {
        min_datagram_size: Option<usize>,
    },
    Lost(ConnectionError),
    Accepted(Box<QuicPeerStream>),
    Stats {
        stats: ConnectionStats,
        max_datagram_size: Option<usize>,
    },
    Datagrams(Vec<Bytes>, Instant),
}

pub(crate) enum SendDelivery {
    /// The peer acknowledged everything up to the finish.
    Finished,
    Stopped(AppErrorCode),
    Lost(ConnectionError),
}

pub(crate) enum RecvDelivery {
    Data(Vec<Bytes>, Instant),
    Finished,
    Reset(AppErrorCode),
    Lost(ConnectionError),
}

/// A component that receives deliveries from a route.
pub(crate) trait Routed: Component<Mutability = Mutable> + Sized {
    type Key: Copy;
    type Delivery;

    fn key(&self) -> Self::Key;

    fn slot(
        router: &mut DriverRouter,
        key: Self::Key,
    ) -> Option<&mut Slot<Self::Delivery>>;

    fn deliver(&mut self, delivery: Self::Delivery);
}

/// Where a route's deliveries go: the entity holding its component, or a
/// queue until there is one.
pub(crate) struct Slot<D> {
    entity: Option<Entity>,
    pending: Vec<D>,
}

impl<D> Default for Slot<D> {
    fn default() -> Self {
        Self {
            entity: None,
            pending: Vec::new(),
        }
    }
}

impl<D> Slot<D> {
    fn deliver<C: Routed<Delivery = D>>(
        &mut self,
        query: &mut Query<&mut C>,
        delivery: D,
    ) {
        if let Some(entity) = self.entity
            && let Ok(mut component) = query.get_mut(entity)
        {
            component.deliver(delivery);
        } else {
            self.pending.push(delivery);
        }
    }
}

/// Binds a route to the entity its component was inserted on, and applies
/// the deliveries that arrived while it had none.
pub(crate) fn bind<C: Routed>(mut world: DeferredWorld, context: HookContext) {
    let Some(key) = world.get::<C>(context.entity).map(C::key) else {
        return;
    };
    let pending = {
        let Some(mut router) = world.get_resource_mut::<DriverRouter>() else {
            return;
        };
        let Some(slot) = C::slot(router.bypass_change_detection(), key) else {
            return;
        };
        slot.entity = Some(context.entity);
        mem::take(&mut slot.pending)
    };

    if !pending.is_empty()
        && let Some(mut component) = world.get_mut::<C>(context.entity)
    {
        for delivery in pending {
            component.deliver(delivery);
        }
    }
}

/// Unbinds a route when its component is removed from an entity or replaced.
pub(crate) fn unbind<C: Routed>(mut world: DeferredWorld, context: HookContext) {
    let Some(key) = world.get::<C>(context.entity).map(C::key) else {
        return;
    };
    if let Some(mut router) = world.get_resource_mut::<DriverRouter>()
        && let Some(slot) = C::slot(router.bypass_change_detection(), key)
        && slot.entity == Some(context.entity)
    {
        slot.entity = None;
    }
}

/// The routes for every live connection and stream.
#[derive(Resource, Default)]
pub(crate) struct DriverRouter {
    endpoints: HashMap<EndpointId, EndpointRoute>,
    conns: HashMap<ConnId, ConnRoute>,
}

struct EndpointRoute {
    parent: QuicParentId,
    /// Connections that finished their handshake, for the server to take.
    accepted: Vec<QuicConnection>,
    closed: bool,
}

struct ConnRoute {
    id: ConnectionId,
    driver: usize,
    slot: Slot<ConnDelivery>,
    /// A server connection that's still handshaking. The router holds it
    /// until it can be handed to its server.
    handshaking: Option<(EndpointId, QuicConnection)>,
    lost: Option<ConnectionError>,
    /// The connection object was dropped.
    released: bool,
    streams: HashMap<DriverStreamId, StreamRoute>,
    /// Opened streams waiting for their attempts, by request.
    opened: HashMap<u64, Box<StreamObjects>>,
    /// Requests whose attempts were dropped before the stream opened.
    cancelled: HashSet<u64>,
}

impl ConnRoute {
    fn new(id: ConnectionId, driver: usize) -> Self {
        Self {
            id,
            driver,
            slot: Slot::default(),
            handshaking: None,
            lost: None,
            released: false,
            streams: HashMap::new(),
            opened: HashMap::new(),
            cancelled: HashSet::new(),
        }
    }

    /// Nothing more can arrive for the route, and nothing is left to
    /// deliver to.
    fn is_done(&self) -> bool {
        self.released
            && self.lost.is_some()
            && self.handshaking.is_none()
            && self.streams.is_empty()
    }

    fn deliver(
        &mut self,
        query: &mut Query<&mut QuicConnection>,
        delivery: ConnDelivery,
    ) {
        // Nothing is left to deliver to, and the slot would only grow.
        if self.released {
            return;
        }
        match &mut self.handshaking {
            Some((_, conn)) => conn.deliver(delivery),
            None => self.slot.deliver(query, delivery),
        }
    }
}

/// A stream's halves. A half is `None` if the stream doesn't have it or its
/// object was dropped.
#[derive(Default)]
struct StreamRoute {
    send: Option<Slot<SendDelivery>>,
    recv: Option<Slot<RecvDelivery>>,
}

/// The objects for a newly opened or accepted stream.
pub(crate) struct StreamObjects {
    pub send: Option<QuicSendStream>,
    pub recv: Option<QuicReceiveStream>,
}

/// How an open attempt resolved.
pub(crate) enum OpenResult {
    Opened(Box<StreamObjects>),
    Failed(ConnectionError),
}

/// The components the poll system delivers to.
#[derive(SystemParam)]
pub(crate) struct RouteTargets<'w, 's> {
    conns: Query<'w, 's, &'static mut QuicConnection>,
    sends: Query<'w, 's, &'static mut QuicSendStream>,
    recvs: Query<'w, 's, &'static mut QuicReceiveStream>,
}

/// Servers and attempts, which the poll system checks every frame. There are
/// few servers, and attempts last a frame or two.
#[derive(SystemParam)]
pub(crate) struct Waiting<'w, 's> {
    servers: Query<'w, 's, &'static mut QuicServer>,
    connects: Query<'w, 's, &'static mut QuicConnectionAttempt>,
    bidirectional: Query<'w, 's, &'static mut QuicBidirectionalStreamAttempt>,
    send: Query<'w, 's, &'static mut QuicSendStreamAttempt>,
}

/// Applies everything the drivers reported since the last frame.
pub(crate) fn poll_driver_events(
    runtime: Res<QuicRuntime>,
    mut router: ResMut<DriverRouter>,
    mut targets: RouteTargets,
    mut waiting: Waiting,
) {
    let router = &mut *router;
    let now = Instant::now();

    router.drain_notices(&runtime);
    for (driver, events) in runtime.event_receivers().enumerate() {
        let events = events.lock().unwrap_or_else(PoisonError::into_inner);
        while let Ok(event) = events.try_recv() {
            router.handle_event(event, driver, now, &runtime, &mut targets);
        }
    }
    // Objects dropped while handling events.
    router.drain_notices(&runtime);

    // Components are only borrowed mutably when something changes, so
    // change detection isn't triggered every frame.
    for mut server in &mut waiting.servers {
        if let Some((accepted, closed)) = router.take_accepted(&server) {
            server.receive(accepted, closed);
        }
    }
    for mut attempt in &mut waiting.connects {
        if let Some(conn) = attempt.connecting()
            && let Some(deliveries) = router.take_connect_deliveries(conn)
        {
            attempt.deliver(deliveries);
        }
    }
    for mut attempt in &mut waiting.bidirectional {
        if let Some(key) = attempt.opening()
            && let Some(result) = router.take_open_result(key)
        {
            attempt.resolve_open(result, |objects| match objects {
                StreamObjects {
                    send: Some(send),
                    recv: Some(recv),
                } => Some((recv, send)),
                _ => None,
            });
        }
    }
    for mut attempt in &mut waiting.send {
        if let Some(key) = attempt.opening()
            && let Some(result) = router.take_open_result(key)
        {
            attempt.resolve_open(result, |objects| objects.send);
        }
    }
}

/// Wakes each driver that has commands waiting. Runs once per frame, so a
/// driver is woken at most once per frame however many commands it gets.
pub(crate) fn flush_driver_commands(runtime: Res<QuicRuntime>) {
    runtime.flush();
}

impl DriverRouter {
    fn drain_notices(&mut self, runtime: &QuicRuntime) {
        let notices = runtime
            .notice_receiver()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while let Ok(notice) = notices.try_recv() {
            self.handle_notice(notice);
        }
    }

    fn handle_notice(&mut self, notice: Notice) {
        match notice {
            Notice::Endpoint { endpoint, parent } => {
                self.endpoints.insert(
                    endpoint,
                    EndpointRoute {
                        parent,
                        accepted: Vec::new(),
                        closed: false,
                    },
                );
            }
            // Dropping its accepted connections closes them.
            Notice::EndpointReleased { endpoint } => {
                drop(self.endpoints.remove(&endpoint))
            }
            Notice::Conn { conn, id, driver } => {
                self.conns.insert(conn, ConnRoute::new(id, driver));
            }
            Notice::ConnReleased { conn } => {
                if let Some(route) = self.conns.get_mut(&conn) {
                    route.released = true;
                }
                self.remove_if_done(conn);
            }
            Notice::HalfReleased { conn, stream, half } => {
                let Some(route) = self.conns.get_mut(&conn) else {
                    return;
                };
                if let Entry::Occupied(mut entry) = route.streams.entry(stream) {
                    let halves = entry.get_mut();
                    match half {
                        Half::Send => halves.send = None,
                        Half::Recv => halves.recv = None,
                    }
                    if halves.send.is_none() && halves.recv.is_none() {
                        entry.remove();
                    }
                }
                self.remove_if_done(conn);
            }
            Notice::OpenCancelled { conn, request } => {
                let Some(route) = self.conns.get_mut(&conn) else {
                    return;
                };
                // Dropping the objects releases the stream.
                if route.opened.remove(&request).is_none() && route.lost.is_none() {
                    route.cancelled.insert(request);
                }
            }
        }
    }

    fn remove_if_done(&mut self, conn: ConnId) {
        if self.conns.get(&conn).is_some_and(ConnRoute::is_done) {
            self.conns.remove(&conn);
        }
    }

    fn handle_event(
        &mut self,
        event: Event,
        driver: usize,
        now: Instant,
        runtime: &QuicRuntime,
        targets: &mut RouteTargets,
    ) {
        match event {
            Event::EndpointClosed { endpoint } => {
                if let Some(route) = self.endpoints.get_mut(&endpoint) {
                    route.closed = true;
                }
            }
            Event::ConnectionAccepted {
                endpoint,
                conn,
                remote,
            } => {
                if !self.endpoints.contains_key(&endpoint) {
                    // The server may have been bound since notices were read.
                    self.drain_notices(runtime);
                }
                let link = runtime.link(driver);
                let Some(server) = self.endpoints.get(&endpoint) else {
                    // The server was dropped.
                    link.send(Command::Close {
                        conn,
                        code: AppErrorCode::UNKNOWN,
                        reason: Bytes::new(),
                    });
                    return;
                };

                let id = ConnectionId::new(conn.0, server.parent);
                let mut route = ConnRoute::new(id, driver);
                let datagram_buffer = runtime.endpoint_options().datagram_receive_buffer;
                route.handshaking = Some((
                    endpoint,
                    QuicConnection::new(link, conn, id, remote, datagram_buffer),
                ));
                self.conns.insert(conn, route);
            }
            Event::Connected {
                conn,
                min_datagram_size,
            } => {
                let Some(route) = self.conn_route(conn, runtime) else {
                    return;
                };
                let delivery = ConnDelivery::Connected { min_datagram_size };
                let Some((endpoint, mut connection)) = route.handshaking.take() else {
                    route.deliver(&mut targets.conns, delivery);
                    return;
                };
                connection.deliver(delivery);
                // If the server is gone, dropping the connection closes it.
                if let Some(server) = self.endpoints.get_mut(&endpoint) {
                    server.accepted.push(connection);
                }
            }
            Event::ConnectionLost { conn, reason } => {
                let Some(route) = self.conn_route(conn, runtime) else {
                    return;
                };
                route.lost = Some(reason.clone());
                route.cancelled.clear();
                route.deliver(&mut targets.conns, ConnDelivery::Lost(reason.clone()));
                // A connection that never finished its handshake isn't
                // handed to the server.
                route.handshaking = None;

                for stream in route.streams.values_mut() {
                    if let Some(slot) = &mut stream.send {
                        slot.deliver(
                            &mut targets.sends,
                            SendDelivery::Lost(reason.clone()),
                        );
                    }
                    if let Some(slot) = &mut stream.recv {
                        slot.deliver(
                            &mut targets.recvs,
                            RecvDelivery::Lost(reason.clone()),
                        );
                    }
                }
                self.remove_if_done(conn);
            }
            Event::StreamOpened {
                conn,
                request,
                stream,
            } => {
                let Some(route) = self.conn_route(conn, runtime) else {
                    return;
                };
                let objects = add_stream(route, conn, stream, runtime);
                // A cancelled request's objects are dropped, which releases
                // the stream.
                if !route.cancelled.remove(&request) {
                    route.opened.insert(request, Box::new(objects));
                }
            }
            Event::StreamAccepted { conn, stream } => {
                let Some(route) = self.conn_route(conn, runtime) else {
                    return;
                };
                let peer = match add_stream(route, conn, stream, runtime) {
                    StreamObjects {
                        send: Some(send),
                        recv: Some(recv),
                    } => QuicPeerStream::Bidirectional(recv, send),
                    StreamObjects {
                        recv: Some(recv), ..
                    } => QuicPeerStream::Receive(recv),
                    _ => return,
                };
                route.deliver(&mut targets.conns, ConnDelivery::Accepted(Box::new(peer)));
            }
            Event::StreamData {
                conn,
                stream,
                chunks,
            } => {
                self.deliver_recv(conn, stream, targets, RecvDelivery::Data(chunks, now))
            }
            Event::StreamFinished { conn, stream } => {
                self.deliver_recv(conn, stream, targets, RecvDelivery::Finished)
            }
            Event::StreamReset { conn, stream, code } => {
                self.deliver_recv(conn, stream, targets, RecvDelivery::Reset(code))
            }
            Event::StreamStopped { conn, stream, code } => {
                self.deliver_send(conn, stream, targets, SendDelivery::Stopped(code))
            }
            Event::SendFinished { conn, stream } => {
                self.deliver_send(conn, stream, targets, SendDelivery::Finished)
            }
            Event::Datagrams { conn, data } => {
                if let Some(route) = self.conns.get_mut(&conn) {
                    route.deliver(&mut targets.conns, ConnDelivery::Datagrams(data, now));
                }
            }
            Event::Stats {
                conn,
                stats,
                max_datagram_size,
            } => {
                if let Some(route) = self.conns.get_mut(&conn) {
                    let delivery = ConnDelivery::Stats {
                        stats,
                        max_datagram_size,
                    };
                    route.deliver(&mut targets.conns, delivery);
                }
            }
        }
    }

    /// Finds a connection's route. A client registers its connection before
    /// sending the connect command, so if the route is missing, reading the
    /// latest notices finds it unless the connection is gone.
    fn conn_route(
        &mut self,
        conn: ConnId,
        runtime: &QuicRuntime,
    ) -> Option<&mut ConnRoute> {
        if !self.conns.contains_key(&conn) {
            self.drain_notices(runtime);
        }
        self.conns.get_mut(&conn)
    }

    fn deliver_send(
        &mut self,
        conn: ConnId,
        stream: DriverStreamId,
        targets: &mut RouteTargets,
        delivery: SendDelivery,
    ) {
        if let Some(slot) = self
            .conns
            .get_mut(&conn)
            .and_then(|route| route.streams.get_mut(&stream))
            .and_then(|halves| halves.send.as_mut())
        {
            slot.deliver(&mut targets.sends, delivery);
        }
    }

    fn deliver_recv(
        &mut self,
        conn: ConnId,
        stream: DriverStreamId,
        targets: &mut RouteTargets,
        delivery: RecvDelivery,
    ) {
        if let Some(slot) = self
            .conns
            .get_mut(&conn)
            .and_then(|route| route.streams.get_mut(&stream))
            .and_then(|halves| halves.recv.as_mut())
        {
            slot.deliver(&mut targets.recvs, delivery);
        }
    }

    /// Takes the connections that finished their handshake for a server,
    /// and whether its endpoint has closed, if either changed.
    fn take_accepted(
        &mut self,
        server: &QuicServer,
    ) -> Option<(Vec<QuicConnection>, bool)> {
        let route = self.endpoints.get_mut(&server.endpoint())?;
        let closed = route.closed && !server.is_closed();
        (!route.accepted.is_empty() || closed)
            .then(|| (mem::take(&mut route.accepted), route.closed))
    }

    /// Takes what arrived for a connection whose attempt holds it.
    fn take_connect_deliveries(&mut self, conn: ConnId) -> Option<Vec<ConnDelivery>> {
        match self.conns.get_mut(&conn) {
            Some(route) if route.slot.pending.is_empty() => None,
            Some(route) => Some(mem::take(&mut route.slot.pending)),
            None => Some(vec![ConnDelivery::Lost(ConnectionError::LocallyClosed)]),
        }
    }

    /// Takes the result of an open request, if it has one.
    fn take_open_result(&mut self, (conn, request): (ConnId, u64)) -> Option<OpenResult> {
        let Some(route) = self.conns.get_mut(&conn) else {
            return Some(OpenResult::Failed(ConnectionError::LocallyClosed));
        };
        if let Some(objects) = route.opened.remove(&request) {
            return Some(OpenResult::Opened(objects));
        }
        route.lost.clone().map(OpenResult::Failed)
    }
}

/// Creates the objects and route for a newly opened or accepted stream.
fn add_stream(
    route: &mut ConnRoute,
    conn: ConnId,
    info: StreamInfo,
    runtime: &QuicRuntime,
) -> StreamObjects {
    let link = runtime.link(route.driver);
    let id = StreamId::new(route.id, info.id.0);

    let send = info
        .send
        .map(|budget| QuicSendStream::new(link.clone(), conn, info.id, id, budget));
    let recv = info
        .recv
        .map(|budget| QuicReceiveStream::new(link, conn, info.id, id, budget));

    route.streams.insert(
        info.id,
        StreamRoute {
            send: send.is_some().then(Slot::default),
            recv: recv.is_some().then(Slot::default),
        },
    );
    StreamObjects { send, recv }
}

impl Routed for QuicConnection {
    type Key = ConnId;
    type Delivery = ConnDelivery;

    fn key(&self) -> ConnId {
        self.conn()
    }

    fn slot(router: &mut DriverRouter, key: ConnId) -> Option<&mut Slot<ConnDelivery>> {
        router.conns.get_mut(&key).map(|route| &mut route.slot)
    }

    fn deliver(&mut self, delivery: ConnDelivery) {
        QuicConnection::deliver(self, delivery);
    }
}

impl Routed for QuicSendStream {
    type Key = (ConnId, DriverStreamId);
    type Delivery = SendDelivery;

    fn key(&self) -> Self::Key {
        self.key()
    }

    fn slot(
        router: &mut DriverRouter,
        (conn, stream): Self::Key,
    ) -> Option<&mut Slot<SendDelivery>> {
        router
            .conns
            .get_mut(&conn)?
            .streams
            .get_mut(&stream)?
            .send
            .as_mut()
    }

    fn deliver(&mut self, delivery: SendDelivery) {
        QuicSendStream::deliver(self, delivery);
    }
}

impl Routed for QuicReceiveStream {
    type Key = (ConnId, DriverStreamId);
    type Delivery = RecvDelivery;

    fn key(&self) -> Self::Key {
        self.key()
    }

    fn slot(
        router: &mut DriverRouter,
        (conn, stream): Self::Key,
    ) -> Option<&mut Slot<RecvDelivery>> {
        router
            .conns
            .get_mut(&conn)?
            .streams
            .get_mut(&stream)?
            .recv
            .as_mut()
    }

    fn deliver(&mut self, delivery: RecvDelivery) {
        QuicReceiveStream::deliver(self, delivery);
    }
}
