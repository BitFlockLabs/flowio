#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, finish_counting_allocations_of_size, start_counting_allocations_of_size,
};
use flowio::test_support::net::resolver::resolve_local_host_with_hosts_path;
use std::fs::{OpenOptions, remove_file};
use std::io::{BufWriter, Write as _};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const HOSTS_FILE_MAX_BYTES: usize = 4 * 1024 * 1024;
static NEXT_FILE: AtomicUsize = AtomicUsize::new(0);

struct TempHostsFile {
    path: PathBuf,
}

impl TempHostsFile {
    fn blocklist_with_target(host: &str) -> Self {
        let sequence = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "flowio-resolver-hosts-alloc-{}-{sequence}",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("failed to create hosts allocation fixture");
        let mut file = BufWriter::new(file);
        let target = format!("192.0.2.44 {host}\n");
        file.write_all(target.as_bytes())
            .expect("failed to write hosts target");

        let filler = b"0.0.0.0 blocked.flowio.invalid\n";
        let mut written = target.len();
        while HOSTS_FILE_MAX_BYTES - written >= filler.len() {
            file.write_all(filler)
                .expect("failed to write hosts blocklist entry");
            written += filler.len();
        }
        let padding = [b' '; 64];
        file.write_all(&padding[..HOSTS_FILE_MAX_BYTES - written])
            .expect("failed to finish hosts boundary fixture");
        file.flush()
            .expect("failed to flush hosts allocation fixture");

        Self { path }
    }

    fn path(&self) -> &str {
        self.path
            .to_str()
            .expect("temporary hosts path should be UTF-8")
    }
}

impl Drop for TempHostsFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.path);
    }
}

#[test]
fn blocklist_hosts_lookup_does_not_materialize_the_whole_file() {
    let host = "hosts-allocation.flowio.invalid";
    let fixture = TempHostsFile::blocklist_with_target(host);

    start_counting_allocations_of_size(HOSTS_FILE_MAX_BYTES);
    let addrs = resolve_local_host_with_hosts_path(fixture.path(), host, 5432)
        .expect("maximum-sized blocklist hosts file should resolve");
    let whole_file_allocations = finish_counting_allocations_of_size();

    assert_eq!(
        addrs,
        [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 44), 5432))]
    );
    assert_eq!(
        whole_file_allocations, 0,
        "hosts lookup must retain only its reusable line buffer"
    );
}
