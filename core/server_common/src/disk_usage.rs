// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Estimated usage of the volume holding the data directory, and whether it
//! reached the configured limit.
//!
//! ```text
//! estimate = base + charged          base     = used - settled, both at the last sample
//! blocked <=> estimate >= limit  <=> charged >= block_at = settled + (limit - used)
//! ```
//!
//! - `charged` counts every byte the write sites charged since the process
//!   started, before they write. Samples never move it, so its growth is what
//!   Iggy allocated, which is how a test checks that every write site charges
//!   while the sampler keeps running.
//! - `settled` is the part of `charged` whose writes finished or failed, i.e.
//!   whose [`DiskCharge`] was dropped. Only those can be on the volume when it
//!   is measured, so a charge still in flight stays in the estimate.
//! - A periodic `statvfs` sample re-bases the estimate. A burst between two
//!   samples is counted without a syscall, and anything the write sites cannot
//!   see (hard links, temporary files replaced by rename, logs, other
//!   processes, deleted segments) is wrong for at most one interval.
//! - A charge can only raise the estimate, so any charge may set `blocked`.
//!   Only a sample sees space freed by deletes, so only a sample clears it.
//!
//! Memory ordering: `charge`, `rebase` and `refresh_blocked` use `SeqCst` on
//! `charged`, `block_at` and `blocked`, so a charge that crosses the limit while
//! a sample moves the threshold or clears the flag is seen by one of the two.
//! `settled` is released when a [`DiskCharge`] is dropped and acquired by
//! [`DiskUsage::rebase`], so every write a sample takes over finished before the
//! volume was measured. `rebase` releases `base` and `measured` and
//! [`DiskUsage::used_bytes`] acquires both, so a reader that sees the base also
//! sees every charge it subtracted. Everything else is `Relaxed`: a stale
//! `blocked` is off by at most one sample.
//!
//! Re-basing assumes `statvfs` already counts every write that finished
//! before the sample. ext4 and XFS do: they take a write's blocks out of the
//! free count as soon as the write reserves them. btrfs and OpenZFS do not:
//! they update the free count only when they commit, every 30 s on btrfs
//! (later for writes still waiting in the page cache) and every 5 s on OpenZFS
//! by default. On those filesystems the estimate can read low after a sample,
//! by the writes that finished since the last commit, until the next commit.

use crossbeam::utils::CachePadded;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The process-wide estimate. It has no value until the first sample.
pub static DISK_USAGE: DiskUsage = DiskUsage::new();

#[derive(Debug)]
pub struct DiskUsage {
    // Every charge writes these two, so each gets a cache line of its own.
    charged: CachePadded<AtomicU64>,
    settled: CachePadded<AtomicU64>,

    // Written at most once a second and read on every charge or admission, so
    // these share a line away from the two above.
    /// Wraps below zero once deletes outgrow the charges, so it is added to
    /// `charged` with wrapping arithmetic.
    base: AtomicU64,
    /// `u64::MAX` before the first sample and while the limit is disabled.
    /// Compared with `charged` without wrapping, so it is only right until
    /// `charged` wraps after 16 EiB, about 58 years of writing at 10 GB/s.
    block_at: AtomicU64,
    /// Whether new data must be refused.
    blocked: AtomicBool,
    /// Whether a sample has set `base`.
    measured: AtomicBool,
}

/// What a sample of the volume reports to [`DiskUsage::rebase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeSample {
    pub used_bytes: u64,
    /// Used bytes at which the estimate is blocked, `None` for no limit.
    pub limit_bytes: Option<u64>,
}

impl VolumeSample {
    /// A sample of a volume without a limit.
    #[must_use]
    pub const fn unlimited(used_bytes: u64) -> Self {
        Self {
            used_bytes,
            limit_bytes: None,
        }
    }

    /// The value of `charged` at which the estimate reaches the limit, given
    /// the charges settled before this sample was taken.
    fn block_at(&self, settled: u64) -> u64 {
        self.limit_bytes.map_or(u64::MAX, |limit_bytes| {
            settled.saturating_add(limit_bytes.saturating_sub(self.used_bytes))
        })
    }
}

impl DiskUsage {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            charged: CachePadded::new(AtomicU64::new(0)),
            settled: CachePadded::new(AtomicU64::new(0)),
            base: AtomicU64::new(0),
            block_at: AtomicU64::new(u64::MAX),
            blocked: AtomicBool::new(false),
            measured: AtomicBool::new(false),
        }
    }

    /// Adds `bytes` that Iggy is about to write to, or has reserved on, the
    /// volume. Hold the returned [`DiskCharge`] until the write finishes or
    /// fails.
    #[inline]
    pub fn charge(&self, bytes: u64) -> DiskCharge<'_> {
        if bytes > 0 {
            let previous_charged = self.charged.fetch_add(bytes, Ordering::SeqCst);
            let charged = previous_charged.wrapping_add(bytes);
            // Checking first keeps the flag's line shared while it is set.
            if self.reached_limit(charged) && !self.blocked.load(Ordering::SeqCst) {
                self.blocked.store(true, Ordering::SeqCst);
            }
        }
        DiskCharge { usage: self, bytes }
    }

    /// Whether new data must be refused: the estimate reached the limit and no
    /// sample since has seen it back under. A charge that compared against the
    /// threshold of the previous sample can set it again right after a sample
    /// cleared it, until the next sample. One load, no syscall.
    #[must_use]
    #[inline]
    pub fn is_blocked(&self) -> bool {
        self.blocked.load(Ordering::Relaxed)
    }

    /// Estimated bytes used on the volume: the last sample plus the bytes
    /// charged since. `None` until a sample sets the base, since the charges
    /// alone say nothing about the volume.
    #[must_use]
    #[inline]
    pub fn used_bytes(&self) -> Option<u64> {
        if !self.measured.load(Ordering::Acquire) {
            return None;
        }
        // Pairs with the release in `rebase`, which follows its acquire of
        // `settled`, which follows each settled charge's `fetch_add`. So the
        // `charged` load below sees every charge the base subtracted, and the
        // estimate never reads below the sample. The base must be loaded first.
        let base = self.base.load(Ordering::Acquire);
        Some(base.wrapping_add(self.charged.load(Ordering::Relaxed)))
    }

    /// Every byte charged since the process started. Samples do not move it.
    /// `/metrics` exports it as `disk_allocated_bytes`.
    #[must_use]
    #[inline]
    pub fn charged_bytes(&self) -> u64 {
        self.charged.load(Ordering::Relaxed)
    }

    /// Runs `measure` and re-bases the estimate on the volume it returns,
    /// passing its sample through, then sets or clears the flag. Call it from
    /// one thread only.
    ///
    /// A write that settles while `measure` runs is counted twice until the
    /// next sample, which errs towards a fuller disk. When `measure` returns
    /// `None`, the base and the flag stay as they were.
    ///
    /// On btrfs and OpenZFS the volume shows a finished write only after its
    /// commit, so the estimate reads low until then (see the module docs).
    pub fn rebase<T>(&self, measure: impl FnOnce() -> Option<(T, VolumeSample)>) -> Option<T> {
        // Pairs with the release in `DiskCharge::drop`: every write settled
        // here finished before the volume is measured.
        let settled = self.settled.load(Ordering::Acquire);
        let (sample, volume) = measure()?;
        self.base
            .store(volume.used_bytes.wrapping_sub(settled), Ordering::Release);
        self.block_at
            .store(volume.block_at(settled), Ordering::SeqCst);
        self.measured.store(true, Ordering::Release);
        self.refresh_blocked();
        Some(sample)
    }

    #[inline]
    fn reached_limit(&self, charged: u64) -> bool {
        charged >= self.block_at.load(Ordering::SeqCst)
    }

    /// Sets the flag at or over the limit. Clears it only under the limit,
    /// then checks again for a charge that crossed the limit in between and
    /// whose store the clear may have overwritten.
    fn refresh_blocked(&self) {
        if self.reached_limit(self.charged.load(Ordering::SeqCst)) {
            self.blocked.store(true, Ordering::SeqCst);
            return;
        }
        self.blocked.store(false, Ordering::SeqCst);
        if self.reached_limit(self.charged.load(Ordering::SeqCst)) {
            self.blocked.store(true, Ordering::SeqCst);
        }
    }
}

impl Default for DiskUsage {
    fn default() -> Self {
        Self::new()
    }
}

/// Bytes charged before they are written. Dropping it, once the write finished
/// or failed, lets the next sample take them over: a finished write is on the
/// volume, and a failed one allocated only what the volume shows.
#[derive(Debug)]
#[must_use = "a charge settles when dropped, so hold it until the write finishes"]
pub struct DiskCharge<'a> {
    usage: &'a DiskUsage,
    bytes: u64,
}

impl Drop for DiskCharge<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.usage.settled.fetch_add(self.bytes, Ordering::Release);
        }
    }
}

/// How many bytes of a write at `position..end` take new disk space when the
/// file's reservation covers `..reserved_end`. Writes below the end of the
/// reservation take none. The part past it does: a segment can grow past its
/// reservation because the last batch may cross it, and a file without a
/// reservation (`reserved_end` 0) takes new space for the whole write.
#[must_use]
pub const fn unreserved_bytes(position: u64, end: u64, reserved_end: u64) -> u64 {
    let start = if position > reserved_end {
        position
    } else {
        reserved_end
    };
    end.saturating_sub(start)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    const MIB: u64 = 1024 * 1024;

    fn limited(used_bytes: u64, limit_bytes: u64) -> VolumeSample {
        VolumeSample {
            used_bytes,
            limit_bytes: Some(limit_bytes),
        }
    }

    // The estimate

    #[test]
    fn given_no_sample_when_charging_should_have_no_estimate_yet() {
        let usage = DiskUsage::new();

        drop(usage.charge(3 * MIB));

        assert_eq!(usage.used_bytes(), None);
        assert_eq!(usage.charged_bytes(), 3 * MIB);
    }

    #[test]
    fn given_charges_when_rebasing_should_replace_them_with_the_sample() {
        let usage = DiskUsage::new();
        drop(usage.charge(5 * MIB));

        let sample = usage.rebase(|| Some(("sample", VolumeSample::unlimited(100 * MIB))));

        assert_eq!(sample, Some("sample"));
        assert_eq!(usage.used_bytes(), Some(100 * MIB));
    }

    #[test]
    fn given_a_sample_below_the_estimate_when_rebasing_should_lower_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        drop(usage.charge(40 * MIB));

        usage.rebase(|| Some(((), VolumeSample::unlimited(90 * MIB))));

        assert_eq!(
            usage.used_bytes(),
            Some(90 * MIB),
            "space freed since the last sample must lower the estimate"
        );
    }

    #[test]
    fn given_a_charge_from_another_thread_after_the_measurement_when_rebasing_should_keep_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        let measured = Barrier::new(2);
        let charged = Barrier::new(2);

        thread::scope(|scope| {
            scope.spawn(|| {
                measured.wait();
                drop(usage.charge(MIB));
                charged.wait();
            });
            usage.rebase(|| {
                // The volume is measured before the writer charges, so the
                // sample cannot hold its bytes; the base is stored after.
                let sample = 100 * MIB;
                measured.wait();
                charged.wait();
                Some(((), VolumeSample::unlimited(sample)))
            });
        });

        assert_eq!(
            usage.used_bytes(),
            Some(101 * MIB),
            "storing the base must not drop a charge that landed after the measurement"
        );
        assert_eq!(usage.charged_bytes(), MIB);
    }

    #[test]
    fn given_a_charge_during_the_sample_when_rebasing_should_count_it_until_the_next_sample() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));

        usage.rebase(|| {
            drop(usage.charge(MIB));
            Some(((), VolumeSample::unlimited(100 * MIB)))
        });

        assert_eq!(
            usage.used_bytes(),
            Some(101 * MIB),
            "a charge the sample may have missed must still be counted"
        );
    }

    #[test]
    fn given_a_write_in_flight_when_rebasing_should_keep_its_charge_until_it_settles() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        let write = usage.charge(MIB);

        // The write has not reached the volume, so the sample does not hold it.
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));

        assert_eq!(
            usage.used_bytes(),
            Some(101 * MIB),
            "a sample taken while a write is in flight must not drop its charge"
        );

        drop(write);
        usage.rebase(|| Some(((), VolumeSample::unlimited(101 * MIB))));

        assert_eq!(
            usage.used_bytes(),
            Some(101 * MIB),
            "the first sample that holds a settled write must replace its charge"
        );
    }

    #[test]
    fn given_a_write_in_flight_on_another_thread_when_rebasing_should_keep_its_charge() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        let charged = Barrier::new(2);
        let sampled = Barrier::new(2);

        thread::scope(|scope| {
            scope.spawn(|| {
                let write = usage.charge(MIB);
                charged.wait();
                // The bytes reach the volume only after the sample.
                sampled.wait();
                drop(write);
            });
            charged.wait();
            usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
            sampled.wait();
        });

        assert_eq!(
            usage.used_bytes(),
            Some(101 * MIB),
            "a sample must not drop the charge of a write that was still in flight"
        );
    }

    #[test]
    fn given_a_failed_write_when_its_charge_settles_should_let_the_next_sample_remove_it() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        let write = usage.charge(MIB);

        // The write failed before allocating anything.
        drop(write);
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));

        assert_eq!(usage.used_bytes(), Some(100 * MIB));
        assert_eq!(usage.charged_bytes(), MIB);
    }

    #[test]
    fn given_a_failed_sample_when_rebasing_should_keep_counting_from_the_previous_base() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        drop(usage.charge(MIB));

        let sample: Option<()> = usage.rebase(|| None);

        assert!(sample.is_none());
        assert_eq!(usage.used_bytes(), Some(101 * MIB));
    }

    #[test]
    fn given_samples_between_charges_when_reading_the_charged_total_should_count_only_charges() {
        let usage = DiskUsage::new();
        drop(usage.charge(5 * MIB));
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        drop(usage.charge(2 * MIB));
        usage.rebase(|| Some(((), VolumeSample::unlimited(50 * MIB))));

        assert_eq!(usage.charged_bytes(), 7 * MIB);
        assert_eq!(usage.used_bytes(), Some(50 * MIB));
    }

    #[test]
    fn given_more_charged_than_the_volume_holds_when_rebasing_should_wrap_the_base() {
        let usage = DiskUsage::new();
        drop(usage.charge(30000 * MIB));

        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));
        drop(usage.charge(MIB));

        assert_eq!(usage.used_bytes(), Some(101 * MIB));
        assert_eq!(usage.charged_bytes(), 30001 * MIB);
    }

    #[test]
    fn given_a_reservation_when_writing_should_count_only_the_bytes_past_it() {
        const RESERVED: u64 = 4 * MIB;

        assert_eq!(unreserved_bytes(0, MIB, 0), MIB, "no reservation");
        assert_eq!(unreserved_bytes(0, MIB, RESERVED), 0, "inside it");
        assert_eq!(
            unreserved_bytes(RESERVED - MIB, RESERVED + MIB, RESERVED),
            MIB,
            "crossing its end"
        );
        assert_eq!(
            unreserved_bytes(RESERVED + MIB, RESERVED + 2 * MIB, RESERVED),
            MIB,
            "past its end"
        );
    }

    // The flag

    #[test]
    fn given_no_sample_when_charging_should_not_block() {
        let usage = DiskUsage::new();

        drop(usage.charge(1 << 40));

        assert!(!usage.is_blocked());
    }

    #[test]
    fn given_a_charge_one_byte_short_of_the_limit_when_charging_should_stay_open() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));

        drop(usage.charge(10 * MIB - 1));

        assert!(!usage.is_blocked());
    }

    #[test]
    fn given_a_charge_reaching_the_limit_when_charging_should_block() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));

        drop(usage.charge(10 * MIB));

        assert!(usage.is_blocked());
    }

    #[test]
    fn given_settled_charges_before_a_sample_when_charging_to_the_limit_should_block() {
        let usage = DiskUsage::new();
        drop(usage.charge(30 * MIB));
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));

        drop(usage.charge(10 * MIB - 1));
        assert!(
            !usage.is_blocked(),
            "charges settled before the sample are in it, so they must not count towards the limit"
        );

        drop(usage.charge(1));
        assert!(usage.is_blocked());
    }

    #[test]
    fn given_a_sample_at_the_limit_when_rebasing_should_block_without_a_charge() {
        let usage = DiskUsage::new();

        usage.rebase(|| Some(((), limited(110 * MIB, 110 * MIB))));

        assert!(
            usage.is_blocked(),
            "space another process took must block like space Iggy charged"
        );
    }

    #[test]
    fn given_a_blocked_estimate_when_its_writes_settle_should_stay_blocked_until_a_sample() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));
        let write = usage.charge(10 * MIB);
        assert!(usage.is_blocked());

        drop(write);
        assert!(usage.is_blocked(), "only a sample may clear the flag");

        usage.rebase(|| Some(((), limited(105 * MIB, 110 * MIB))));
        assert!(
            !usage.is_blocked(),
            "a sample under the limit must clear the flag"
        );
    }

    #[test]
    fn given_a_write_in_flight_when_the_sample_is_under_the_limit_should_stay_blocked() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));
        let write = usage.charge(10 * MIB);

        // The write has not reached the volume, so the sample is under the limit.
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));
        assert!(
            usage.is_blocked(),
            "a sample must not clear the flag under a write still in flight"
        );

        drop(write);
        usage.rebase(|| Some(((), limited(100 * MIB, 110 * MIB))));
        assert!(
            !usage.is_blocked(),
            "once the write settled, a sample that does not hold it shows space was freed"
        );
    }

    #[test]
    fn given_a_failed_sample_when_rebasing_should_keep_the_flag() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), limited(110 * MIB, 110 * MIB))));

        let sample: Option<()> = usage.rebase(|| None);

        assert!(sample.is_none());
        assert!(usage.is_blocked());
    }

    #[test]
    fn given_a_disabled_limit_when_charging_should_never_block() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), VolumeSample::unlimited(100 * MIB))));

        drop(usage.charge(1 << 50));

        assert!(!usage.is_blocked());
    }

    #[test]
    fn given_an_unlimited_sample_when_computing_the_threshold_should_be_unreachable() {
        for settled in [0, 50 * MIB, 200 * MIB] {
            assert_eq!(
                VolumeSample::unlimited(100 * MIB).block_at(settled),
                u64::MAX,
                "settled = {settled}"
            );
        }
    }

    #[test]
    fn given_a_limit_disabled_by_a_sample_when_rebasing_should_unblock() {
        let usage = DiskUsage::new();
        usage.rebase(|| Some(((), limited(110 * MIB, 110 * MIB))));

        usage.rebase(|| Some(((), VolumeSample::unlimited(110 * MIB))));

        assert!(!usage.is_blocked());
    }
}
