/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use sysinfo::{Disks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};

#[derive(Debug)]
pub(crate) struct SystemSampler {
    process_cpu_microseconds: AtomicU64,
    process_rss_bytes: AtomicU64,
    process_vsize_bytes: AtomicU64,

    total_memory_bytes: AtomicU64,
    available_memory_bytes: AtomicU64,
    disk_total_bytes: AtomicU64,
    disk_available_bytes: AtomicU64,

    process_start_time_unix_seconds: u64,
    pid: Pid,
    data_directory: PathBuf,

    state: Mutex<SamplerState>,
}

#[derive(Debug)]
struct SamplerState {
    system: System,
    disks: Disks,
    last_refresh_at: Option<Instant>,
}

impl SystemSampler {
    pub(crate) fn new(data_directory: PathBuf) -> Self {
        let pid = sysinfo::get_current_pid().expect("Expected to resolve current PID");
        let process_start_time_unix_seconds =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let system = System::new();
        let disks = Disks::new_with_refreshed_list();

        let sampler = Self {
            process_cpu_microseconds: AtomicU64::new(0),
            process_rss_bytes: AtomicU64::new(0),
            process_vsize_bytes: AtomicU64::new(0),
            total_memory_bytes: AtomicU64::new(0),
            available_memory_bytes: AtomicU64::new(0),
            disk_total_bytes: AtomicU64::new(0),
            disk_available_bytes: AtomicU64::new(0),
            process_start_time_unix_seconds,
            pid,
            data_directory,
            state: Mutex::new(SamplerState { system, disks, last_refresh_at: None }),
        };
        sampler.refresh();
        sampler
    }

    pub(crate) fn refresh(&self) {
        let mut state = self.state.lock().expect("Expected system sampler state lock acquisition");

        state.system.refresh_memory();
        state.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );
        self.total_memory_bytes.store(state.system.total_memory(), Ordering::Relaxed);
        self.available_memory_bytes.store(state.system.available_memory(), Ordering::Relaxed);

        let now = Instant::now();
        if let Some(process) = state.system.process(self.pid) {
            self.process_rss_bytes.store(process.memory(), Ordering::Relaxed);
            self.process_vsize_bytes.store(process.virtual_memory(), Ordering::Relaxed);

            // sysinfo 0.33 doesn't expose cumulative CPU time, so integrate
            // cpu_usage() (percent since last refresh) × elapsed-time. Multi-core
            // saturation can yield cpu_usage > 100, which is the correct semantics.
            if let Some(last) = state.last_refresh_at {
                let elapsed = now.duration_since(last).as_secs_f64();
                let cpu_pct = process.cpu_usage() as f64;
                let cpu_micros_delta = ((cpu_pct / 100.0) * elapsed * 1_000_000.0) as u64;
                if cpu_micros_delta > 0 {
                    self.process_cpu_microseconds.fetch_add(cpu_micros_delta, Ordering::Relaxed);
                }
            }
        }
        state.last_refresh_at = Some(now);

        state.disks.refresh(true);
        let disk_info = match self.data_directory.canonicalize() {
            Ok(path) => state.disks.iter().find(|disk| path.starts_with(disk.mount_point())),
            Err(_) => None,
        };
        if let Some(disk) = disk_info {
            self.disk_total_bytes.store(disk.total_space(), Ordering::Relaxed);
            self.disk_available_bytes.store(disk.available_space(), Ordering::Relaxed);
        }
    }

    pub(crate) fn total_memory_bytes(&self) -> u64 {
        self.total_memory_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn available_memory_bytes(&self) -> u64 {
        self.available_memory_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn disk_total_bytes(&self) -> u64 {
        self.disk_total_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn disk_available_bytes(&self) -> u64 {
        self.disk_available_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn process_cpu_seconds_total(&self) -> f64 {
        self.process_cpu_microseconds.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
    pub(crate) fn process_resident_memory_bytes(&self) -> u64 {
        self.process_rss_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn process_virtual_memory_bytes(&self) -> u64 {
        self.process_vsize_bytes.load(Ordering::Relaxed)
    }
    pub(crate) fn process_start_time_unix_seconds(&self) -> u64 {
        self.process_start_time_unix_seconds
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::SystemSampler;

    #[test]
    #[ignore = "isolated sampler RSS comparison; run explicitly with --test-threads=1"]
    fn sampler_initialization_and_refresh_memory() {
        let sampler = SystemSampler::new(std::env::current_dir().unwrap());
        for _ in 0..3 {
            sampler.refresh();
        }
        eprintln!(
            "sampler: processes={}; resident_bytes={}; virtual_bytes={}; cpu_seconds={}",
            sampler.state.lock().unwrap().system.processes().len(),
            sampler.process_resident_memory_bytes(),
            sampler.process_virtual_memory_bytes(),
            sampler.process_cpu_seconds_total(),
        );
        assert!(sampler.total_memory_bytes() > 0);
        assert!(sampler.process_resident_memory_bytes() > 0);
    }

    #[test]
    fn sampler_retains_only_its_own_process_and_preserves_metrics() {
        let sampler = SystemSampler::new(std::env::current_dir().unwrap());
        let start_time = sampler.process_start_time_unix_seconds();
        let mut cpu_seconds = sampler.process_cpu_seconds_total();

        for _ in 0..3 {
            let state = sampler.state.lock().unwrap();
            assert_eq!(state.system.processes().len(), 1);
            assert!(state.system.process(sampler.pid).is_some());
            drop(state);

            assert!(sampler.total_memory_bytes() > 0);
            assert!(sampler.available_memory_bytes() <= sampler.total_memory_bytes());
            assert!(sampler.process_resident_memory_bytes() > 0);
            assert!(sampler.process_virtual_memory_bytes() >= sampler.process_resident_memory_bytes());
            assert!(sampler.disk_available_bytes() <= sampler.disk_total_bytes());
            assert!(start_time > 0);
            assert_eq!(sampler.process_start_time_unix_seconds(), start_time);
            assert!(sampler.process_cpu_seconds_total() >= cpu_seconds);
            cpu_seconds = sampler.process_cpu_seconds_total();
            sampler.refresh();
        }
    }
}
