//! Rust version of the `Twitter snowflake algorithm`.
//!
//! Source https://github.com/BinChengZhao/snowflake-rs
// License: MIT
use std::hint::spin_loop;
use std::sync::atomic::{AtomicU16, AtomicU64};
use std::time::{SystemTime, UNIX_EPOCH};

/// The `SnowflakeIdGenerator` type is a snowflake algorithm wrapper.
pub struct SnowflakeIdGenerator {
    /// epoch used by the snowflake algorithm.
    epoch: SystemTime,

    /// last_time_millis, the last time generate id is used times millis.
    last_time_millis: AtomicU64,

    /// machine_id is used to supplement id machine or sectionalization attribute.
    pub machine_id: u32,

    /// node_id, is use to supplement id machine-node attribute.
    pub node_id: u32,

    /// auto-increment record.
    idx: AtomicU16,
}

impl Clone for SnowflakeIdGenerator {
    fn clone(&self) -> Self {
        let last_time_millis = self.last_time_millis.load(std::sync::atomic::Ordering::Relaxed);
        SnowflakeIdGenerator {
            epoch: self.epoch,
            last_time_millis: AtomicU64::new(last_time_millis),
            machine_id: self.machine_id,
            node_id: self.node_id,
            idx: AtomicU16::new(self.idx.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

impl SnowflakeIdGenerator {
    /// Constructs a new `SnowflakeIdGenerator` using the UNIX epoch.
    /// Please make sure that machine_id and node_id are smaller than 32(2^5);
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// let id_generator = SnowflakeIdGenerator::new(1, 1);
    /// ```
    pub fn new(machine_id: u32, node_id: u32) -> SnowflakeIdGenerator {
        Self::with_epoch(machine_id, node_id, UNIX_EPOCH)
    }

    /// Constructs a new `SnowflakeIdGenerator` using the specified epoch.
    /// Please make sure that machine_id and node_id are smaller than 32(2^5);
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::{Duration, UNIX_EPOCH};
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// // 1 January 2015 00:00:00
    /// let discord_epoch = UNIX_EPOCH + Duration::from_millis(1420070400000);
    /// let id_generator = SnowflakeIdGenerator::with_epoch(1, 1, discord_epoch);
    /// ```
    pub fn with_epoch(machine_id: u32, node_id: u32, epoch: SystemTime) -> SnowflakeIdGenerator {
        let last_time_millis = get_time_millis(epoch);

        SnowflakeIdGenerator {
            epoch,
            last_time_millis: AtomicU64::new(last_time_millis),
            machine_id,
            node_id,
            idx: AtomicU16::new(0),
        }
    }

    fn advance_idx(&self) -> u16 {
        let mut idx = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if idx >= 4095 {
            self.idx.store(0, std::sync::atomic::Ordering::Relaxed);
            idx = 0;
        }
        idx
    }

    /// Generate an id.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// let mut id_generator = SnowflakeIdGenerator::new(1, 1);
    /// id_generator.real_time_generate();
    /// ```
    pub fn generate(&self) -> u64 {
        let mut idx = self.advance_idx();

        let mut now_millis = get_time_millis(self.epoch);

        // supplement code for 'clock is moving backwards situation'.

        // If the milliseconds of the current clock are equal to
        // the number of milliseconds of the most recently generated id,
        // then check if enough 4096 are generated,
        // if enough, then busy wait until the next millisecond.
        let last_time_millis = self.last_time_millis.load(std::sync::atomic::Ordering::Relaxed);
        if now_millis == last_time_millis {
            if idx == 0 {
                now_millis = biding_time_conditions(last_time_millis, self.epoch);
            }
        } else {
            idx = 0;
            self.idx.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        self.last_time_millis.store(now_millis, std::sync::atomic::Ordering::Relaxed);

        // last_time_millis is 64 bits，left shift 22 bit，store 42 bits ， machine_id left shift 17 bits，
        // node_id left shift 12 bits ,idx complementing bits.
        last_time_millis << 22
            | ((self.machine_id << 17) as u64)
            | ((self.node_id << 12) as u64)
            | (idx as u64)
    }
}

#[inline(always)]
/// Get the latest milliseconds of the clock.
pub fn get_time_millis(epoch: SystemTime) -> u64 {
    SystemTime::now()
        .duration_since(epoch)
        .expect("Time went backward")
        .as_millis() as u64
}

#[inline(always)]
// Constantly refreshing the latest milliseconds by busy waiting.
fn biding_time_conditions(last_time_millis: u64, epoch: SystemTime) -> u64 {
    let mut latest_time_millis: u64;
    loop {
        latest_time_millis = get_time_millis(epoch);
        if latest_time_millis > last_time_millis {
            return latest_time_millis;
        }
        spin_loop();
    }
}