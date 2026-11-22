use super::*;

/// One commit in a hub's ring: its encoded event and little else, so a ring's share of
/// `--feed-ring-bytes` holds as many commits as it can (#1397's 16 hubs × 16, where each
/// hub's share is a sixteenth). The decoded commit isn't kept: a client with a row prefix
/// filters the encoded event's changes instead ([`Entry::for_prefix`]).
pub(crate) struct Entry {
    /// The commit's seqno.
    pub(super) seqno: u64,
    /// The hub feed's token right after this commit: a slice of `encoded` when there is
    /// one (its first field), so it's held once.
    pub(super) token: Bytes,
    /// The hub feed's position just before it (`Feed::position`): every commit of the spec
    /// at or below it came before this one.
    pub(super) prev: u64,
    /// Its share of `--feed-ring-bytes`: `encoded` (the token included) or the token, and a
    /// fixed overhead.
    pub(super) bytes: usize,
    /// The commit as a client with no prefix gets it, encoded once for every such client on
    /// the ring: `None` if it has no change to send. Encoded by the hub, before the entry is
    /// pushed, so no client waits on another's encode (perf287's P2 on #1541).
    pub(super) encoded: Option<Bytes>,
}

/// An entry's size beside its bytes: the struct (88), the `Arc`'s counts (16) and the ring's
/// slot (8) (perf287 on #1655).
pub(super) const ENTRY_OVERHEAD: usize = 112;

impl Entry {
    /// An entry for `commit`, encoded for clients with no prefix and sized with that
    /// encoding (simp724's R1, perf287's P1 on #1541); `prev` is set when it's pushed.
    pub(super) fn new(commit: &Commit, token: Bytes) -> Self {
        let encoded: Option<Bytes> = filtered(commit, b"").map(|c| {
            let xid = commit.xid().unwrap_or_default();
            prost::Message::encode_to_vec(&commit_event(token.clone(), c, xid)).into()
        });
        // The token is the event's field 1, which prost writes first: its tag, its length,
        // then its bytes. Kept as that slice, it isn't held twice.
        let token = match &encoded {
            Some(e) => {
                let at = 1 + prost::encoding::encoded_len_varint(token.len() as u64);
                match e.get(at..at + token.len()) {
                    Some(b) if !token.is_empty() && b == &token[..] => {
                        e.slice(at..at + token.len())
                    }
                    _ => token,
                }
            }
            None => token,
        };
        let bytes = ENTRY_OVERHEAD + encoded.as_ref().map_or(token.len(), Bytes::len);
        Entry {
            seqno: commit.seqno(),
            token,
            prev: 0,
            bytes,
            encoded,
        }
    }

    /// The event a client with `prefix` sends for this entry: the commit's changes to rows
    /// starting with it, or `None` if there are none. Decoded from `encoded`, in the order a
    /// filter of the commit gives them.
    pub(super) fn for_prefix(&self, prefix: &[u8]) -> Option<proto::FeedEvent> {
        let bytes = self.encoded.as_ref()?;
        // From the `Bytes` itself: protogen's `bytes(["."])` makes the row, qualifier, value
        // and token fields slices of it, not copies (perf287 on #1655).
        let mut event = <proto::FeedEvent as prost::Message>::decode(bytes.clone()).ok()?;
        let Some(proto::feed_event::Event::Commit(c)) = &mut event.event else {
            return None;
        };
        c.changes.retain(|ch| ch.row.starts_with(prefix));
        (!c.changes.is_empty()).then_some(event)
    }

    /// The `FeedEvent` a client with no prefix sends for this commit, encoded.
    pub(super) fn encoded(&self) -> Option<&Bytes> {
        self.encoded.as_ref()
    }
}

/// How an entry's changes stand against a client's row prefix ([`rows`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Rows {
    /// Every change is to a matching row: the client's event is the shared one.
    All,
    /// No change is: nothing to send.
    None,
    /// Some are, or the encoding isn't one the walk reads: decode and filter it.
    Mixed,
}

/// Where an encoded `FeedEvent`'s changes' rows stand against `prefix` (#1780), read off
/// the encoding with no allocation: the commit's changes' `row` fields only, every other
/// field stepped over by its wire type. A client whose prefix matches every row (perf287's
/// g90: `w0` over writers `w000`-`w015`) then sends the shared encoding, as a client with
/// no prefix does, instead of decoding and encoding each entry again for itself: 13 times
/// the shared path's cost per event (cli692's micro-benchmark), on every client of a hub.
/// Anything the walk doesn't expect (a truncated encoding, a group, a second commit field,
/// another event) is [`Rows::Mixed`], so the decode decides, never a skip.
pub(super) fn rows(event: &[u8], prefix: &[u8]) -> Rows {
    rows_of(event, prefix).unwrap_or(Rows::Mixed)
}

/// `FeedEvent.commit`, `FeedCommit.changes` and `Change.row`.
const FEED_COMMIT: u64 = 2;
const COMMIT_CHANGES: u64 = 3;
const CHANGE_ROW: u64 = 4;

fn rows_of(mut event: &[u8], prefix: &[u8]) -> Option<Rows> {
    let mut commit = None;
    while !event.is_empty() {
        match field(&mut event)? {
            (FEED_COMMIT, Some(c)) if commit.is_none() => commit = Some(c),
            // Another commit field (prost would merge them), or another of the oneof's events
            // (the planned catalog event, progress, partition state, D331's move barrier and
            // raw commit): the oneof's last field wins, so the decode decides.
            (FEED_COMMIT | 3 | 4 | 6 | 8 | 9, _) => return None,
            _ => {}
        }
    }
    let mut commit = commit?;
    let (mut all, mut any) = (true, false);
    while !commit.is_empty() {
        match field(&mut commit)? {
            (COMMIT_CHANGES, Some(change)) => {
                let matches = row_of(change)?.starts_with(prefix);
                all &= matches;
                any |= matches;
            }
            (COMMIT_CHANGES, None) => return None,
            _ => {}
        }
    }
    Some(match (any, all) {
        // No change at all is none matching, as the decode's filter leaves it.
        (false, _) => Rows::None,
        (true, true) => Rows::All,
        (true, false) => Rows::Mixed,
    })
}

/// An encoded `Change`'s row: its last `row` field, as prost decodes it, or empty without
/// one (proto3's default, which only an empty prefix matches).
fn row_of(mut change: &[u8]) -> Option<&[u8]> {
    let mut row: &[u8] = &[];
    while !change.is_empty() {
        match field(&mut change)? {
            (CHANGE_ROW, Some(r)) => row = r,
            (CHANGE_ROW, None) => return None,
            _ => {}
        }
    }
    Some(row)
}

/// The next field of an encoded message: its number, and its bytes if it's length-delimited
/// (`None` for a varint or fixed-width one, stepped over). `None` for anything else: a
/// group, field 0, a key past `u32::MAX`, or a truncated encoding.
fn field<'a>(buf: &mut &'a [u8]) -> Option<(u64, Option<&'a [u8]>)> {
    let key = prost::encoding::decode_varint(buf).ok()?;
    let number = key >> 3;
    // As prost's decode refuses them (feedback from feed1248, N1 on #1783).
    if number == 0 || key > u64::from(u32::MAX) {
        return None;
    }
    let value = match key & 7 {
        0 => {
            prost::encoding::decode_varint(buf).ok()?;
            None
        }
        1 => {
            *buf = buf.get(8..)?;
            None
        }
        5 => {
            *buf = buf.get(4..)?;
            None
        }
        2 => {
            let len = usize::try_from(prost::encoding::decode_varint(buf).ok()?).ok()?;
            let value = buf.get(..len)?;
            *buf = &buf[len..];
            Some(value)
        }
        _ => return None,
    };
    Some((number, value))
}

/// What a client with a (non-empty) `prefix` sends for entry `e`, if anything: the shared
/// encoding when every change matches, nothing when none does, and the matching changes
/// decoded off it otherwise (#1780). The same bytes the decode alone would send, in every
/// case (`a_prefix_client_sends_what_the_decode_would_whatever_the_rows`).
pub(super) fn prefix_out(e: &Arc<Entry>, prefix: &[u8]) -> Option<Out> {
    match rows(e.encoded()?, prefix) {
        Rows::All => Some(Out::Shared(Arc::clone(e))),
        Rows::None => None,
        Rows::Mixed => e.for_prefix(prefix).map(Out::event),
    }
}

/// A commit event: `c` with the token right after it, and its spanning transaction's id
/// (`xid`, D329 2b; empty for any other commit).
pub(super) fn commit_event(token: Bytes, c: proto::FeedCommit, xid: &[u8]) -> proto::FeedEvent {
    proto::FeedEvent {
        token,
        event: Some(proto::feed_event::Event::Commit(c)),
        partition: String::new(),
        xid: Bytes::copy_from_slice(xid),
    }
}

/// `commit` as the client sees it: its changes to rows under `prefix`, or `None` when it has
/// none (the engine's prefix feed passes such a commit over).
pub(super) fn filtered(commit: &Commit, prefix: &[u8]) -> Option<proto::FeedCommit> {
    let changes: Vec<proto::Change> = commit
        .changes()
        .iter()
        .filter(|ch| ch.row().starts_with(prefix))
        .map(|ch| proto::Change {
            table: ch.table().to_owned(),
            family: ch.family().to_owned(),
            kind: change_kind(ch.kind()) as i32,
            row: ch.row().to_vec().into(),
            qualifier: ch.qualifier().to_vec().into(),
            timestamp: ch.timestamp(),
            value: ch.value().map(convert::value_out),
        })
        .collect();
    (!changes.is_empty()).then(|| proto::FeedCommit {
        seqno: commit.seqno(),
        timestamp: commit.timestamp(),
        changes,
    })
}

pub(super) fn change_kind(k: pigeonhole::ChangeKind) -> proto::ChangeKind {
    use pigeonhole::ChangeKind as K;
    match k {
        K::Put => proto::ChangeKind::Put,
        K::Merge => proto::ChangeKind::Merge,
        K::DeleteCell => proto::ChangeKind::DeleteCell,
        K::DeleteColumn => proto::ChangeKind::DeleteColumn,
        K::DeleteFamily => proto::ChangeKind::DeleteFamily,
        _ => proto::ChangeKind::Unspecified,
    }
}
