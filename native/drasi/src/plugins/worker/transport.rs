use super::*;
use std::io::{Read, Write};

const MAGIC: [u8; 4] = *b"TDW1";
pub(super) const MAX_FRAME: usize = 8 << 20;
pub(super) const MAX_PAYLOAD: usize = 1 << 20;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub nonce: String,
    pub identity: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Frame {
    version: u16,
    pub binding: Binding,
    pub command: Command,
}

impl Frame {
    pub fn new(binding: &Binding, command: Command) -> Self {
        Self {
            version: 1,
            binding: binding.clone(),
            command,
        }
    }
    pub fn validate(&self, binding: &Binding) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.version == 1 && &self.binding == binding,
            "foreign source IPC binding failed"
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub(super) enum Command {
    ConstructionPhase(ConstructionPhase),
    Request {
        id: u64,
        request: Request,
    },
    Response {
        id: u64,
        response: Response,
    },
    Resolve {
        id: u64,
        artifact: String,
        request: Vec<u8>,
    },
    Resolved {
        id: u64,
        value: Option<String>,
    },
    Event {
        subscription: u64,
        event: Event,
    },
    Ack {
        query: String,
        sequence: u64,
    },
    Unsubscribe {
        subscription: u64,
    },
    Unsubscribed {
        subscription: u64,
    },
    Stop,
    Stopped,
    Failure,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub(super) enum ConstructionPhase {
    #[default]
    Spawned,
    SourceLibrary,
    BootstrapLibrary,
    ResolverInjection,
    SourceFactory,
    SourceContract,
    BootstrapFactory,
    AttachBootstrap,
    StateDirectory,
    StateStore,
    StateBridge,
    Initialize,
    Ready,
}

#[derive(Serialize, Deserialize)]
pub(super) enum Request {
    Probe(TrustedArtifact),
    Construct(Box<WorkerSpec>),
    Start,
    Status,
    Deprovision,
    RemovePin {
        query: String,
    },
    Subscribe {
        subscription: u64,
        settings: Settings,
    },
}

#[derive(Serialize, Deserialize)]
pub(super) enum Response {
    Descriptor(Box<WorkerDescriptor>),
    ProbeFailed(String),
    Constructed,
    Subscribed {
        bootstrap: bool,
        position_handle: bool,
    },
    Ok,
    Status(ComponentStatus),
    Failed,
}

#[derive(Serialize, Deserialize)]
pub(super) enum Event {
    Live(Vec<u8>),
    Bootstrap(Vec<u8>),
    LiveEnd,
    BootstrapEnd,
    BootstrapResult {
        count: usize,
        position: Option<Vec<u8>>,
    },
    BootstrapFailed,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Settings {
    pub source_id: String,
    pub query_id: String,
    pub bootstrap: bool,
    pub nodes: Vec<String>,
    pub relations: Vec<String>,
    pub position: Option<Vec<u8>>,
    pub sequence: Option<u64>,
    pub position_handle: bool,
}

impl From<&SourceSubscriptionSettings> for Settings {
    fn from(value: &SourceSubscriptionSettings) -> Self {
        Self {
            source_id: value.source_id.clone(),
            query_id: value.query_id.clone(),
            bootstrap: value.enable_bootstrap,
            nodes: value.nodes.iter().cloned().collect(),
            relations: value.relations.iter().cloned().collect(),
            position: value.resume_from.as_ref().map(|value| value.to_vec()),
            sequence: value.resume_sequence,
            position_handle: value.request_position_handle,
        }
    }
}

pub(super) fn write(writer: &mut impl Write, frame: &Frame) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(frame)
        .map_err(|_| failed("foreign source frame could not be encoded"))?;
    anyhow::ensure!(
        bytes.len() <= MAX_FRAME,
        "foreign source frame exceeds capacity"
    );
    let result = writer
        .write_all(&MAGIC)
        .and_then(|_| writer.write_all(&(bytes.len() as u32).to_be_bytes()))
        .and_then(|_| writer.write_all(&bytes))
        .and_then(|_| writer.flush());
    // Resolver replies may contain credentials. Do not retain serialized copies.
    bytes.fill(0);
    result.map_err(|_| failed("foreign source IPC write failed"))
}

pub(super) fn read(reader: &mut impl Read) -> anyhow::Result<Frame> {
    let mut header = [0; 8];
    reader
        .read_exact(&mut header)
        .map_err(|_| failed("foreign source IPC ended"))?;
    anyhow::ensure!(header[..4] == MAGIC, "foreign source IPC framing failed");
    let size = u32::from_be_bytes(header[4..].try_into().expect("frame length")) as usize;
    anyhow::ensure!(
        size > 0 && size <= MAX_FRAME,
        "foreign source IPC frame exceeds capacity"
    );
    let mut bytes = vec![0; size];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| failed("foreign source IPC payload ended"))?;
    let frame =
        serde_json::from_slice(&bytes).map_err(|_| failed("foreign source IPC payload is invalid"));
    bytes.fill(0);
    frame
}

pub(super) fn parent_writer(
    mut stdin: std::process::ChildStdin,
    commands: std::sync::mpsc::Receiver<Frame>,
    shared: Arc<Shared>,
) {
    loop {
        match commands.recv_timeout(Duration::from_millis(20)) {
            Ok(frame) => {
                if write(&mut stdin, &frame).is_err() {
                    if !shared.closing.load(Ordering::Acquire) {
                        shared.fail_at("input pipe write");
                    }
                    return;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if shared.reaped.load(Ordering::Acquire) {
                    return;
                }
            }
        }
        if shared.closing.load(Ordering::Acquire) {
            continue;
        }
        let acks = match acknowledgements(&shared) {
            Ok(acks) => acks,
            Err(_) => {
                shared.fail();
                return;
            }
        };
        for (query, sequence) in acks {
            if write(
                &mut stdin,
                &Frame::new(&shared.binding, Command::Ack { query, sequence }),
            )
            .is_err()
            {
                shared.fail();
                return;
            }
        }
    }
}

pub(super) fn acknowledgements(shared: &Shared) -> anyhow::Result<Vec<(String, u64)>> {
    let mut pins = shared
        .pins
        .lock()
        .map_err(|_| failed("foreign source pin lock failed"))?;
    let mut acks = Vec::new();
    let mut frozen = false;
    for (query, pin) in pins.iter().filter(|(_, pin)| !pin.removing) {
        let sequence = pin.position.load(Ordering::Acquire);
        if !pin.attached || sequence == u64::MAX {
            frozen = true;
            continue;
        }
        anyhow::ensure!(
            sequence <= pin.delivered && (pin.sent == u64::MAX || sequence >= pin.sent),
            "foreign source committed position is invalid"
        );
        if sequence != pin.sent {
            acks.push((query.clone(), sequence));
        }
    }
    // MAX is ignored by the SDK's minimum-watermark calculation. Freeze peers
    // before acquiring a new bootstrap cut, until that query commits real input.
    if frozen {
        return Ok(Vec::new());
    }
    for (query, sequence) in &acks {
        pins.get_mut(query).expect("sampled pin").sent = *sequence;
    }
    Ok(acks)
}

pub(super) fn parent_reader(mut stdout: std::process::ChildStdout, shared: Arc<Shared>) {
    loop {
        let frame = match read(&mut stdout) {
            Ok(frame) => frame,
            Err(_) => {
                if !shared.closing.load(Ordering::Acquire)
                    || !shared.stopped.load(Ordering::Acquire)
                {
                    shared.fail_at("output pipe read");
                }
                return;
            }
        };
        if frame.validate(&shared.binding).is_err() {
            shared.fail_at("output binding");
            return;
        }
        let result = match frame.command {
            Command::ConstructionPhase(phase) => {
                shared
                    .diagnostics
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .phase = phase;
                Ok(())
            }
            Command::Response { id, response } => {
                let sender = shared
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
                match sender {
                    Some(sender) => {
                        let response = if matches!(response, Response::Failed) {
                            Err(failed("foreign source operation failed"))
                        } else {
                            Ok(response)
                        };
                        if sender.send(response).is_err() {
                            shared.fail();
                        }
                        Ok(())
                    }
                    None => Err(failed("foreign source RPC identity is invalid")),
                }
            }
            Command::Resolve {
                id,
                artifact,
                request,
            } => {
                if request.len() > 512 {
                    Err(failed("foreign source resolver request exceeds capacity"))
                } else {
                    let value = (shared._resolver)(&artifact, &request)
                        .ok()
                        .filter(|value| value.len() <= 16 << 10 && !value.as_bytes().contains(&0));
                    shared
                        .commands
                        .try_send(Frame::new(&shared.binding, Command::Resolved { id, value }))
                        .map_err(|_| failed("foreign source resolver response capacity exceeded"))
                }
            }
            Command::Event { .. } if shared.closing.load(Ordering::Acquire) => Ok(()),
            Command::Event {
                subscription,
                event,
            } => deliver(&shared, subscription, event),
            Command::Unsubscribed { subscription } => {
                shared
                    .lanes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&subscription);
                Ok(())
            }
            Command::Stopped if shared.closing.load(Ordering::Acquire) => {
                shared.stopped.store(true, Ordering::Release);
                Ok(())
            }
            _ => Err(failed("foreign source IPC command is invalid")),
        };
        if result.is_err() {
            shared.fail_at("output dispatch");
            return;
        }
    }
}

fn deliver(shared: &Shared, subscription: u64, event: Event) -> anyhow::Result<()> {
    let mut lanes = shared
        .lanes
        .lock()
        .map_err(|_| failed("foreign source lane lock failed"))?;
    let lane = lanes
        .get_mut(&subscription)
        .ok_or_else(|| failed("foreign source subscription is unknown"))?;
    if lane.closing {
        return Ok(());
    }
    match event {
        Event::Live(bytes) => {
            anyhow::ensure!(
                bytes.len() <= MAX_FRAME,
                "foreign source input exceeds capacity"
            );
            let budget = Budget::reserve(&shared.bytes, bytes.len())?;
            let value: drasi_plugin_sdk::ffi::payload::SourceEventPayload =
                serde_json::from_slice(&bytes)
                    .map_err(|_| failed("foreign source event encoding is invalid"))?;
            position(value.source_position.as_deref())?;
            anyhow::ensure!(
                value.source_id == lane.source && value.sequence > lane.delivered,
                "foreign source input sequence is invalid"
            );
            lane.delivered = value.sequence;
            if let Some(pin) = shared
                .pins
                .lock()
                .map_err(|_| failed("foreign source pin lock failed"))?
                .get_mut(&lane.query)
            {
                pin.delivered = pin.delivered.max(value.sequence);
            }
            lane.live
                .try_send(Queued {
                    value: value.into_wrapper(),
                    _budget: budget,
                })
                .map_err(|_| failed("foreign source input capacity exceeded"))?;
        }
        Event::Bootstrap(bytes) => {
            if lane.bootstrap_failed {
                return Ok(());
            }
            anyhow::ensure!(
                bytes.len() <= MAX_FRAME,
                "foreign source seed exceeds capacity"
            );
            let value: drasi_plugin_sdk::ffi::payload::BootstrapEventPayload =
                serde_json::from_slice(&bytes)
                    .map_err(|_| failed("foreign source seed encoding is invalid"))?;
            anyhow::ensure!(
                value.source_id == lane.source,
                "foreign source seed identity is invalid"
            );
            let mut budget = Budget::reserve(&shared.bytes, bytes.len())?;
            lane.bootstrap_budget.size += budget.size;
            budget.size = 0;
            let sender = lane
                .bootstrap
                .as_ref()
                .ok_or_else(|| failed("foreign source seed arrived after EOF"))?;
            sender
                .try_send(value.into_event())
                .map_err(|_| failed("foreign source seed capacity exceeded"))?;
            lane.bootstrap_count = lane
                .bootstrap_count
                .checked_add(1)
                .ok_or_else(|| failed("foreign source seed count overflow"))?;
        }
        Event::BootstrapEnd => {
            anyhow::ensure!(!lane.bootstrap_ended, "foreign source repeated seed EOF");
            lane.bootstrap_ended = true;
            lane.bootstrap.take();
        }
        Event::BootstrapResult {
            count,
            position: cut,
        } => {
            position(cut.as_deref())?;
            anyhow::ensure!(
                lane.bootstrap_result.is_none() && lane.result.is_some(),
                "foreign source repeated seed result"
            );
            lane.bootstrap_result = Some((count, cut));
        }
        Event::BootstrapFailed => {
            anyhow::ensure!(
                !lane.bootstrap_failed && lane.result.is_some(),
                "foreign source repeated seed failure"
            );
            lane.bootstrap_failed = true;
            lane.bootstrap_result = None;
            lane.bootstrap.take();
            if let Some(sender) = lane.result.take() {
                let _ = sender.send(Err(failed("foreign source bootstrap failed")));
            }
        }
        Event::LiveEnd => return Err(failed("foreign source live input ended")),
    }
    if lane.bootstrap_ended {
        if let Some((count, cut)) = lane.bootstrap_result.take() {
            anyhow::ensure!(
                count == lane.bootstrap_count,
                "foreign source seed count does not match successful result"
            );
            let sender = lane
                .result
                .take()
                .ok_or_else(|| failed("foreign source seed result is missing"))?;
            sender
                .send(Ok(BootstrapResult {
                    event_count: count,
                    source_position: cut.map(bytes::Bytes::from),
                }))
                .map_err(|_| failed("foreign source seed result was abandoned"))?;
        }
    }
    Ok(())
}
