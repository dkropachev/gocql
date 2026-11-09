//! The scylla-rust-driver client of the driver comparison.
//!
//! It mirrors the Go clients (main.go, old_cases.go, cases.go): the same
//! flags, dataset, key sequences, row validation, closed- and open-loop
//! phases, and JSON measurement fields. Cases are named R<family><n> after the
//! master case they match (RA1 matches MA1 and A1, and so on).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use hdrhistogram::Histogram;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::policies::timestamp_generator::MonotonicTimestampGenerator;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::value::{CqlValue, Row};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const KEYS: u64 = 100_000;
const PARTITIONS: u64 = 1024;
const ROWS_PER_PARTITION: i32 = 128;
const PAGE_ROWS: i32 = 32;
const LWT_KEYS: usize = 4096;
const LWT_KEYS_PER_WORKER: usize = 64;

const POINT_READ: &str = "SELECT k,r,p FROM bench_compare.point WHERE k=?";
const PAGE_READ: &str = "SELECT k,r,p FROM bench_compare.paged WHERE k=?";
const POINT_WRITE: &str = "INSERT INTO bench_compare.point (k,r,p) VALUES (?,?,?)";
const PAGE_WRITE: &str = "INSERT INTO bench_compare.paged (k,r,p) VALUES (?,?,?)";
const LWT_UPDATE: &str = "UPDATE bench_compare.lwt SET v=? WHERE k=? IF v=?";
const LWT_READ: &str = "SELECT v FROM bench_compare.lwt WHERE k=?";

type Error = String;

fn err<E: std::fmt::Display>(e: E) -> Error {
    e.to_string()
}

// ---------------------------------------------------------------- dataset

struct Dataset {
    payloads: [Vec<Vec<u8>>; 2],
    checksums: [Vec<[u8; 32]>; 2],
}

impl Dataset {
    fn new() -> Self {
        let mut payloads: [Vec<Vec<u8>>; 2] = [Vec::new(), Vec::new()];
        let mut checksums: [Vec<[u8; 32]>; 2] = [Vec::new(), Vec::new()];
        for size in 0..2 {
            for id in 0..256u64 {
                let mut b = vec![0u8; 1024 * (1 + 3 * size)];
                let mut x = id + 1;
                for v in b.iter_mut() {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *v = x as u8;
                }
                checksums[size].push(Sha256::digest(&b).into());
                payloads[size].push(b);
            }
        }
        Dataset { payloads, checksums }
    }
    fn payload(&self, paged: bool, k: i32, r: i32) -> &[u8] {
        &self.payloads[paged as usize][variant(k, r)]
    }
}

fn variant(k: i32, r: i32) -> usize {
    ((k as u32).wrapping_mul(131).wrapping_add((r as u32).wrapping_mul(17)) % 256) as usize
}
fn point_key(seq: u64) -> i32 {
    (seq.wrapping_mul(7919) % KEYS) as i32
}
fn part_key(seq: u64) -> i32 {
    (seq.wrapping_mul(7919) % PARTITIONS) as i32
}

/// Validates rows of one key, from row `first` on, as main.go's rowCheck.
#[derive(Default)]
struct RowCheck {
    paged: bool,
    k: i32,
    next: i32,
    seen: i32,
}

impl RowCheck {
    fn reset(&mut self, paged: bool, k: i32, first: i32) {
        *self = RowCheck { paged, k, next: first, seen: 0 };
    }
    fn row(&mut self, d: &Dataset, k: i32, r: i32, p: &[u8]) -> Result<(), Error> {
        let s = self.paged as usize;
        let ok = k == self.k
            && r == self.next
            && p.len() == 1024 * (1 + 3 * s)
            && <[u8; 32]>::from(Sha256::digest(p)) == d.checksums[s][variant(self.k, self.next)];
        if !ok {
            return Err(format!(
                "invalid owned row key={} row={} got={}/{} len={}",
                self.k,
                self.next,
                k,
                r,
                p.len()
            ));
        }
        self.next += 1;
        self.seen += 1;
        Ok(())
    }
    fn done(&self, want: i32) -> Result<(), Error> {
        if self.seen != want {
            return Err(format!("key={} rows={} expected={}", self.k, self.seen, want));
        }
        Ok(())
    }
}

/// Application-driven paging over the paged table, as main.go's pageCursor.
struct PageCursor {
    state: Vec<u8>,
    k: i32,
    next: i32,
    part: u64,
}

impl PageCursor {
    fn expect(&self) -> i32 {
        PAGE_ROWS.min(ROWS_PER_PARTITION - self.next)
    }
    fn advance(&mut self, n: i32, state: Option<&[u8]>) -> Result<(), Error> {
        self.next += n;
        match state {
            None => {
                if self.next != ROWS_PER_PARTITION {
                    return Err(format!("partition {} ended after {} rows", self.k, self.next));
                }
                self.part += 1;
                self.k = part_key(self.part);
                self.next = 0;
                self.state.clear();
            }
            Some(s) => {
                self.state.clear();
                self.state.extend_from_slice(s);
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- driver

struct Statements {
    point_read: PreparedStatement,
    page_read: PreparedStatement,
    point_write: PreparedStatement,
    page_write: PreparedStatement,
    lwt_update: PreparedStatement,
    lwt_read: PreparedStatement,
}

async fn connect(hosts: &[String]) -> Result<Session, Error> {
    let policy = DefaultPolicy::builder()
        .prefer_datacenter("datacenter1".to_string())
        .token_aware(true)
        .permit_dc_failover(false)
        .build();
    let profile = ExecutionProfile::builder()
        .consistency(Consistency::LocalQuorum)
        .serial_consistency(None)
        .request_timeout(Some(Duration::from_secs(10)))
        .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
        .load_balancing_policy(policy)
        .build();
    SessionBuilder::new()
        .known_nodes(hosts)
        .default_execution_profile_handle(profile.into_handle())
        .timestamp_generator(Arc::new(MonotonicTimestampGenerator::new()))
        .cluster_metadata_refresh_interval(Duration::from_secs(3600))
        .connection_timeout(Duration::from_secs(10))
        .build()
        .await
        .map_err(err)
}

async fn prepare(s: &Session) -> Result<Statements, Error> {
    let mut point_read = s.prepare(POINT_READ).await.map_err(err)?;
    point_read.set_page_size(PAGE_ROWS);
    let mut page_read = s.prepare(PAGE_READ).await.map_err(err)?;
    page_read.set_page_size(PAGE_ROWS);
    let point_write = s.prepare(POINT_WRITE).await.map_err(err)?;
    let page_write = s.prepare(PAGE_WRITE).await.map_err(err)?;
    let mut lwt_update = s.prepare(LWT_UPDATE).await.map_err(err)?;
    lwt_update.set_serial_consistency(Some(SerialConsistency::LocalSerial));
    let lwt_read = s.prepare(LWT_READ).await.map_err(err)?;
    if !point_read.is_token_aware() || !page_read.is_token_aware() {
        return Err("prepared reads are not token aware".into());
    }
    Ok(Statements { point_read, page_read, point_write, page_write, lwt_update, lwt_read })
}

// ---------------------------------------------------------------- cases

enum Kind {
    /// RA1: point read of one row.
    PointRead,
    /// RB1: partition read of all 128 rows, page by page.
    PartitionRead,
    /// RC1: one 32-row page from a saved state.
    AppPaging,
    /// RD1: point write.
    PointWrite,
    /// RE1: unlogged batch of 8 rows of one partition.
    UnloggedBatch,
    /// RE2: logged batch of 4 point rows.
    LoggedBatch,
    /// RF1: LWT compare-and-set that must apply.
    Lwt,
    /// RG1: even seq RA1, odd seq RD1.
    Mixed,
}

struct Worker {
    kind: Kind,
    s: Arc<Session>,
    st: Arc<Statements>,
    d: Arc<Dataset>,
    check: RowCheck,
    out: Vec<u8>,
    cursor: PageCursor,
    batch: Option<Batch>,
    lwt_keys: Vec<i32>,
    lwt_values: Vec<i32>,
    lwt_next: usize,
}

async fn build_case(
    name: &str,
    id: usize,
    n: usize,
    s: &Arc<Session>,
    st: &Arc<Statements>,
    d: &Arc<Dataset>,
) -> Result<Worker, Error> {
    let kind = match name {
        "RA1" => Kind::PointRead,
        "RB1" => Kind::PartitionRead,
        "RC1" => Kind::AppPaging,
        "RD1" => Kind::PointWrite,
        "RE1" => Kind::UnloggedBatch,
        "RE2" => Kind::LoggedBatch,
        "RF1" => Kind::Lwt,
        "RG1" => Kind::Mixed,
        _ => return Err(format!("unknown rust case {name:?}")),
    };
    let mut w = Worker {
        kind,
        s: s.clone(),
        st: st.clone(),
        d: d.clone(),
        check: RowCheck::default(),
        out: Vec::with_capacity(4096),
        cursor: PageCursor { state: Vec::new(), k: part_key(id as u64), next: 0, part: id as u64 },
        batch: None,
        lwt_keys: Vec::new(),
        lwt_values: Vec::new(),
        lwt_next: 0,
    };
    match w.kind {
        Kind::UnloggedBatch => {
            let mut b = Batch::new(BatchType::Unlogged);
            for _ in 0..8 {
                b.append_statement(st.page_write.clone());
            }
            w.batch = Some(b);
        }
        Kind::LoggedBatch => {
            let mut b = Batch::new(BatchType::Logged);
            for _ in 0..4 {
                b.append_statement(st.point_write.clone());
            }
            w.batch = Some(b);
        }
        Kind::Lwt => {
            if n > LWT_KEYS / LWT_KEYS_PER_WORKER {
                return Err(format!("lwt case supports at most {} workers", LWT_KEYS / LWT_KEYS_PER_WORKER));
            }
            for j in 0..LWT_KEYS_PER_WORKER {
                let key = (id + n * j) as i32;
                let rows = s
                    .execute_unpaged(&st.lwt_read, (key,))
                    .await
                    .map_err(err)?
                    .into_rows_result()
                    .map_err(err)?;
                let (v,) = rows
                    .single_row::<(i32,)>()
                    .map_err(|e| format!("lwt key {key}: {e}; run -mode prepare -scenario lwt"))?;
                w.lwt_keys.push(key);
                w.lwt_values.push(v);
            }
        }
        _ => {}
    }
    Ok(w)
}

impl Worker {
    async fn op(&mut self, seq: u64) -> Result<(), Error> {
        match self.kind {
            Kind::PointRead => self.point_read(seq).await,
            Kind::PartitionRead => self.partition_read(seq).await,
            Kind::AppPaging => self.app_page().await,
            Kind::PointWrite => self.point_write(seq).await,
            Kind::UnloggedBatch => self.unlogged_batch(seq).await,
            Kind::LoggedBatch => self.logged_batch(seq).await,
            Kind::Lwt => self.lwt().await,
            Kind::Mixed => {
                if seq % 2 == 0 {
                    self.point_read(seq).await
                } else {
                    self.point_write(seq).await
                }
            }
        }
    }

    /// Decodes the rows of one response into the owned row buffer and
    /// validates each, as the Go clients copy blobs into reused storage.
    fn consume(&mut self, result: scylla::response::query_result::QueryResult) -> Result<(), Error> {
        let rows = result.into_rows_result().map_err(err)?;
        for row in rows.rows::<(i32, i32, &[u8])>().map_err(err)? {
            let (k, r, p) = row.map_err(err)?;
            self.out.clear();
            self.out.extend_from_slice(p);
            self.check.row(&self.d, k, r, &self.out)?;
        }
        Ok(())
    }

    async fn point_read(&mut self, seq: u64) -> Result<(), Error> {
        let k = point_key(seq);
        self.check.reset(false, k, 0);
        let result = self.s.execute_unpaged(&self.st.point_read, (k,)).await.map_err(err)?;
        self.consume(result)?;
        self.check.done(1)
    }

    async fn partition_read(&mut self, seq: u64) -> Result<(), Error> {
        let k = part_key(seq);
        self.check.reset(true, k, 0);
        let mut state = PagingState::start();
        loop {
            let (result, response) =
                self.s.execute_single_page(&self.st.page_read, (k,), state).await.map_err(err)?;
            self.consume(result)?;
            match response {
                PagingStateResponse::HasMorePages { state: next } => state = next,
                PagingStateResponse::NoMorePages => break,
            }
        }
        self.check.done(ROWS_PER_PARTITION)
    }

    async fn app_page(&mut self) -> Result<(), Error> {
        let k = self.cursor.k;
        self.check.reset(true, k, self.cursor.next);
        let want = self.cursor.expect();
        let state = if self.cursor.state.is_empty() {
            PagingState::start()
        } else {
            PagingState::new_from_raw_bytes(self.cursor.state.as_slice())
        };
        let (result, response) =
            self.s.execute_single_page(&self.st.page_read, (k,), state).await.map_err(err)?;
        self.consume(result)?;
        self.check.done(want)?;
        let seen = self.check.seen;
        match response {
            PagingStateResponse::HasMorePages { state } => {
                let bytes = state.as_bytes_slice().map(|b| b.to_vec()).unwrap_or_default();
                self.cursor.advance(seen, Some(&bytes))
            }
            PagingStateResponse::NoMorePages => self.cursor.advance(seen, None),
        }
    }

    async fn point_write(&mut self, seq: u64) -> Result<(), Error> {
        let k = point_key(seq);
        let p = self.d.payload(false, k, 0);
        self.s.execute_unpaged(&self.st.point_write, (k, 0i32, p)).await.map_err(err)?;
        Ok(())
    }

    async fn unlogged_batch(&mut self, seq: u64) -> Result<(), Error> {
        let k = part_key(seq);
        let base = (seq % 16) as i32 * 8;
        let values: Vec<(i32, i32, &[u8])> =
            (base..base + 8).map(|r| (k, r, self.d.payload(true, k, r))).collect();
        self.s.batch(self.batch.as_ref().unwrap(), values).await.map_err(err)?;
        Ok(())
    }

    async fn logged_batch(&mut self, seq: u64) -> Result<(), Error> {
        let values: Vec<(i32, i32, &[u8])> = (0..4u64)
            .map(|i| {
                let k = point_key(seq * 4 + i);
                (k, 0, self.d.payload(false, k, 0))
            })
            .collect();
        self.s.batch(self.batch.as_ref().unwrap(), values).await.map_err(err)?;
        Ok(())
    }

    async fn lwt(&mut self) -> Result<(), Error> {
        let j = self.lwt_next % LWT_KEYS_PER_WORKER;
        self.lwt_next += 1;
        let (key, cur) = (self.lwt_keys[j], self.lwt_values[j]);
        let rows = self
            .s
            .execute_unpaged(&self.st.lwt_update, (cur + 1, key, cur))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        let row = rows.first_row::<Row>().map_err(err)?;
        match row.columns.first() {
            Some(Some(CqlValue::Boolean(true))) => {}
            other => return Err(format!("lwt key={key} v={cur} not applied ({other:?})")),
        }
        self.lwt_values[j] = cur + 1;
        Ok(())
    }
}

// ---------------------------------------------------------------- measurement

struct Hist {
    h: Histogram<u64>,
    overflow: i64,
}

impl Hist {
    fn new() -> Self {
        Hist { h: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(), overflow: 0 }
    }
    fn record(&mut self, d: Duration) {
        let us = (d.as_micros() as u64).max(1);
        if self.h.record(us).is_err() {
            self.overflow += 1;
        }
    }
    fn export(&self) -> Value {
        json!({"overflow": self.overflow, "count": self.h.len(),
            "p50_us": self.h.value_at_quantile(0.50), "p90_us": self.h.value_at_quantile(0.90),
            "p95_us": self.h.value_at_quantile(0.95), "p99_us": self.h.value_at_quantile(0.99),
            "max_us": self.h.max(), "mean_us": self.h.mean()})
    }
}

struct Stats {
    latency: Hist,
    dispatch: Hist,
    queue: Hist,
    schedule: Hist,
    admitted: i64,
    completed: i64,
    in_window: i64,
    drained: i64,
    errors: i64,
    timeouts: i64,
    server_errors: i64,
    other_error_samples: Vec<String>,
    offered: i64,
    rejected: i64,
    missed: i64,
    late: i64,
    queue_rejected: i64,
    deadline_rejected: i64,
    error_samples: Vec<String>,
    queue_capacity: usize,
    peak_queue: usize,
    end_queue: usize,
    backlog: Vec<usize>,
    affinity_checks: i64,
    affinity_error: String,
    cpu_seconds: f64,
    elapsed: f64,
    rate: f64,
    start: String,
    end: String,
}

impl Stats {
    fn result(&self) -> Value {
        json!({"admitted": self.admitted, "completed": self.completed, "in_window": self.in_window,
            "drained": self.drained, "errors": self.errors, "timeouts": self.timeouts, "server_errors": self.server_errors, "other_error_samples": self.other_error_samples,
            "error_samples": self.error_samples, "offered": self.offered, "rejected": self.rejected,
            "missed_schedules_over_1ms": self.missed, "queue_capacity": self.queue_capacity,
            "peak_queue": self.peak_queue, "end_queue": self.end_queue, "backlog_samples": self.backlog,
            "latency": self.latency.export(), "dispatch_latency": self.dispatch.export(),
            "queue_latency": self.queue.export(), "schedule_lag": self.schedule.export(),
            "alloc_bytes": 0, "mallocs": 0, "gc": 0,
            "cpu_seconds_including_drain": self.cpu_seconds, "duration_seconds": self.elapsed,
            "throughput": self.in_window as f64 / self.elapsed, "target_rate": self.rate,
            "start": self.start, "end": self.end, "late_arrivals": self.late,
            "queue_rejected": self.queue_rejected, "arrival_deadline_rejected": self.deadline_rejected,
            "affinity_checks": self.affinity_checks, "affinity_error": self.affinity_error})
    }
}

fn cpu_seconds() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    (r.ru_utime.tv_sec + r.ru_stime.tv_sec) as f64 + (r.ru_utime.tv_usec + r.ru_stime.tv_usec) as f64 / 1e6
}

fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    format!("{}.{:09}", d.as_secs(), d.subsec_nanos())
}

/// Checks that every thread may run only on GOCQL_BENCH_CPUS, as main.go.
fn affinity() -> Result<(), Error> {
    let expected = std::env::var("GOCQL_BENCH_CPUS").unwrap_or_else(|_| "8".into());
    for entry in std::fs::read_dir("/proc/self/task").map_err(err)? {
        let path = entry.map_err(err)?.path().join("status");
        let Ok(status) = std::fs::read_to_string(&path) else { continue };
        let line = status
            .lines()
            .find(|l| l.starts_with("Cpus_allowed_list:"))
            .ok_or_else(|| format!("missing affinity {}", path.display()))?;
        if line.trim_start_matches("Cpus_allowed_list:").trim() != expected {
            return Err(format!("{}: {line}", path.display()));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Arrival {
    seq: u64,
    scheduled: Instant,
    enqueued: Instant,
}

const OP_TIMEOUT: Duration = Duration::from_secs(10);
const DRAIN: Duration = Duration::from_secs(15);

async fn execute(w: &mut Worker, a: Arrival, end: Instant, delivery_end: Instant, stats: &Mutex<Stats>) {
    let dispatch = Instant::now();
    let budget = OP_TIMEOUT.min(delivery_end.saturating_duration_since(dispatch));
    let result = match tokio::time::timeout(budget, w.op(a.seq)).await {
        Ok(r) => r,
        Err(_) => Err("context deadline exceeded (timeout)".to_string()),
    };
    let finish = Instant::now();
    let mut s = stats.lock().unwrap();
    s.completed += 1;
    s.latency.record(finish - a.scheduled);
    s.dispatch.record(finish - dispatch);
    s.queue.record(dispatch - a.enqueued);
    match result {
        Err(e) => {
            s.errors += 1;
            if e.to_lowercase().contains("timeout") || e.contains("deadline") {
                s.timeouts += 1;
            }
            // Scylla's read/write timeout or failure message, as in main.go's serverError.
            if e.contains("Operation timed out for ") || e.contains("Operation failed for ") {
                s.server_errors += 1;
            } else if s.other_error_samples.len() < 8 {
                s.other_error_samples.push(e.clone());
            }
            if s.error_samples.len() < 8 {
                s.error_samples.push(e);
            }
        }
        Ok(()) if finish <= end => s.in_window += 1,
        Ok(()) => s.drained += 1,
    }
}

/// One measured phase: closed loop when rate is zero, otherwise open loop
/// with evenly scheduled arrivals into a bounded queue, as main.go's phase.
async fn phase(workers: Vec<Worker>, duration: Duration, rate: f64, queue_size: usize) -> (Stats, Vec<Worker>) {
    let n = workers.len();
    let queue_capacity = queue_size.max(4 * n);
    let stats = Arc::new(Mutex::new(Stats {
        latency: Hist::new(),
        dispatch: Hist::new(),
        queue: Hist::new(),
        schedule: Hist::new(),
        admitted: 0,
        completed: 0,
        in_window: 0,
        drained: 0,
        errors: 0,
        timeouts: 0,
        server_errors: 0,
        other_error_samples: Vec::new(),
        offered: 0,
        rejected: 0,
        missed: 0,
        late: 0,
        queue_rejected: 0,
        deadline_rejected: 0,
        error_samples: Vec::new(),
        queue_capacity,
        peak_queue: 0,
        end_queue: 0,
        backlog: Vec::new(),
        affinity_checks: 0,
        affinity_error: String::new(),
        cpu_seconds: 0.0,
        elapsed: duration.as_secs_f64(),
        rate,
        start: String::new(),
        end: String::new(),
    }));
    if let Err(e) = affinity() {
        stats.lock().unwrap().affinity_error = e;
    }
    let c0 = cpu_seconds();
    let start = Instant::now();
    let wall = SystemTime::now();
    let end = start + duration;
    let delivery_end = end + DRAIN;
    {
        let mut s = stats.lock().unwrap();
        s.start = rfc3339(wall);
        s.end = rfc3339(wall + duration);
    }
    let (tx, rx) = async_channel::bounded::<Arrival>(queue_capacity);

    // Monitor: per-second affinity checks and queue depth.
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
    let monitor = {
        let stats = stats.clone();
        let rx = rx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + Duration::from_secs(1), Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = &mut done_rx => return,
                    _ = tick.tick() => {
                        let e = affinity();
                        let mut s = stats.lock().unwrap();
                        s.affinity_checks += 1;
                        if let Err(e) = e { s.affinity_error = e; }
                        s.backlog.push(rx.len());
                    }
                }
            }
        })
    };

    let mut handles = Vec::with_capacity(n);
    for (id, mut w) in workers.into_iter().enumerate() {
        let stats = stats.clone();
        let rx = rx.clone();
        handles.push(tokio::spawn(async move {
            if rate > 0.0 {
                while let Ok(a) = rx.recv().await {
                    execute(&mut w, a, end, delivery_end, &stats).await;
                }
            } else {
                let mut seq = id as u64;
                loop {
                    let now = Instant::now();
                    if now >= end {
                        break;
                    }
                    {
                        let mut s = stats.lock().unwrap();
                        s.admitted += 1;
                        s.offered += 1;
                    }
                    execute(&mut w, Arrival { seq, scheduled: now, enqueued: now }, end, delivery_end, &stats).await;
                    seq += n as u64;
                }
            }
            w
        }));
    }
    drop(rx);

    let scheduler = if rate > 0.0 {
        // A dedicated thread sleeps to each scheduled arrival: the runtime's
        // timer wheel has millisecond resolution, which would add up to 1 ms
        // of scheduling lag to every open-loop latency.
        let stats = stats.clone();
        Some(std::thread::spawn(move || {
            let total = (duration.as_secs_f64() * rate).ceil() as u64;
            for seq in 0..total {
                let scheduled = start + Duration::from_secs_f64(seq as f64 / rate);
                let mut now = Instant::now();
                if now < scheduled {
                    std::thread::sleep(scheduled - now);
                    now = Instant::now();
                }
                let late = now >= end;
                if late && now >= delivery_end {
                    let mut s = stats.lock().unwrap();
                    let rest = (total - seq) as i64;
                    s.offered += rest;
                    s.rejected += rest;
                    s.deadline_rejected += rest;
                    s.missed += rest;
                    break;
                }
                let lag = now - scheduled;
                {
                    let mut s = stats.lock().unwrap();
                    s.offered += 1;
                    s.schedule.record(lag);
                    if late {
                        s.late += 1;
                    }
                    if lag > Duration::from_millis(1) {
                        s.missed += 1;
                    }
                }
                match tx.try_send(Arrival { seq, scheduled, enqueued: now }) {
                    Ok(()) => {
                        let mut s = stats.lock().unwrap();
                        s.admitted += 1;
                        s.peak_queue = s.peak_queue.max(tx.len());
                    }
                    Err(_) => {
                        let mut s = stats.lock().unwrap();
                        s.rejected += 1;
                        s.queue_rejected += 1;
                    }
                }
            }
            let now = Instant::now();
            if now < end {
                std::thread::sleep(end - now);
            }
            stats.lock().unwrap().end_queue = tx.len();
            tx.close();
        }))
    } else {
        tx.close();
        None
    };

    let mut workers = Vec::with_capacity(n);
    for h in handles {
        workers.push(h.await.expect("worker panicked"));
    }
    if let Some(t) = scheduler {
        t.join().expect("scheduler panicked");
    }
    let _ = done_tx.send(());
    let _ = monitor.await;
    let mut s = Arc::try_unwrap(stats).ok().expect("stats still shared").into_inner().unwrap();
    s.cpu_seconds = cpu_seconds() - c0;
    if let Err(e) = affinity() {
        s.affinity_error = e;
    }
    (s, workers)
}

// ---------------------------------------------------------------- main

struct Flags {
    hosts: Vec<String>,
    mode: String,
    case: String,
    concurrency: usize,
    warmup: Duration,
    duration: Duration,
    rate: f64,
    output: String,
    queue_size: usize,
}

fn parse_duration(v: &str) -> Result<Duration, Error> {
    let (num, mul) = if let Some(n) = v.strip_suffix("ms") {
        (n, 1e-3)
    } else if let Some(n) = v.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = v.strip_suffix('m') {
        (n, 60.0)
    } else {
        return Err(format!("invalid duration {v:?}"));
    };
    Ok(Duration::from_secs_f64(num.parse::<f64>().map_err(err)? * mul))
}

fn parse_flags() -> Result<Flags, Error> {
    let mut values = BTreeMap::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let name = arg.trim_start_matches('-').to_string();
        if let Some((k, v)) = name.split_once('=') {
            values.insert(k.to_string(), v.to_string());
        } else {
            let v = args.next().ok_or_else(|| format!("flag {arg} needs a value"))?;
            values.insert(name, v);
        }
    }
    let get = |k: &str, d: &str| values.get(k).cloned().unwrap_or_else(|| d.to_string());
    let known = ["hosts", "mode", "case", "concurrency", "warmup", "duration", "rate", "output", "queue-size"];
    if let Some(k) = values.keys().find(|k| !known.contains(&k.as_str())) {
        return Err(format!("unknown flag -{k}"));
    }
    Ok(Flags {
        hosts: get("hosts", "").split(',').filter(|h| !h.is_empty()).map(String::from).collect(),
        mode: get("mode", "run"),
        case: get("case", ""),
        concurrency: get("concurrency", "1").parse().map_err(err)?,
        warmup: parse_duration(&get("warmup", "30s"))?,
        duration: parse_duration(&get("duration", "120s"))?,
        rate: get("rate", "0").parse().map_err(err)?,
        output: get("output", ""),
        queue_size: get("queue-size", "1024").parse().map_err(err)?,
    })
}

async fn run() -> Result<(), Error> {
    let f = parse_flags()?;
    if f.concurrency < 1 || f.rate < 0.0 || f.duration.is_zero() {
        return Err("invalid measurement configuration".into());
    }
    affinity()?;
    eprintln!("AFFINITY initial: checked all application threads");
    let d = Arc::new(Dataset::new());
    let s = Arc::new(connect(&f.hosts).await?);
    let st = Arc::new(prepare(&s).await?);
    match f.mode.as_str() {
        "check" => {
            let mut w = build_case(&f.case, 0, 1, &s, &st, &d).await?;
            for seq in 0..256u64 {
                w.op(seq).await.map_err(|e| format!("case {} op {seq}: {e}", f.case))?;
            }
            eprintln!("CHECK rust {}: 256 operations", f.case);
            affinity()?;
            return Ok(());
        }
        "run" => {}
        m => return Err(format!("invalid mode {m:?}")),
    }
    let mut workers = Vec::with_capacity(f.concurrency);
    for id in 0..f.concurrency {
        workers.push(build_case(&f.case, id, f.concurrency, &s, &st, &d).await?);
    }
    // Prime all prepared statements on routed hosts before timing.
    for (i, w) in workers.iter_mut().enumerate() {
        w.op(i as u64).await?;
    }
    let mut warm = None;
    if !f.warmup.is_zero() {
        let (stats, back) = phase(workers, f.warmup, f.rate, f.queue_size).await;
        workers = back;
        if stats.errors > stats.server_errors {
            return Err(format!("warmup errors: {:?}", stats.other_error_samples));
        }
        warm = Some(stats.result());
    }
    let (measured, _workers) = phase(workers, f.duration, f.rate, f.queue_size).await;
    let mut result = json!({"driver": "rust", "case": f.case, "concurrency": f.concurrency,
        "runtime": format!("scylla-rust-driver 1.10.0, tokio current_thread"),
        "evidence": {"protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": PAGE_ROWS, "prefetch": 0,
            "retries": 0, "compression": "none", "tls": false, "pool": "one connection per shard",
            "timestamps": "client monotonic"},
        "measurement": measured.result()});
    if let Some(w) = warm {
        result["warmup"] = w;
    }
    if !measured.affinity_error.is_empty() {
        result["measurement_error"] = json!(measured.affinity_error);
    }
    let text = serde_json::to_string_pretty(&result).map_err(err)? + "\n";
    if f.output.is_empty() {
        print!("{text}");
    } else {
        std::fs::write(&f.output, text).map_err(err)?;
    }
    affinity()?;
    eprintln!("AFFINITY final: checked all application threads");
    if measured.errors > measured.server_errors {
        return Err(format!("measured errors: {:?}", measured.other_error_samples));
    }
    if !measured.affinity_error.is_empty() {
        return Err(measured.affinity_error);
    }
    Ok(())
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    if let Err(e) = rt.block_on(run()) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
