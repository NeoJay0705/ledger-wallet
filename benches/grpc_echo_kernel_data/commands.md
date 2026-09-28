# gRPC kernel attribution run commands

Run from the repository root. Every formal trial used the benchmark's default strict idle preflight: 3 seconds, CPU busy <=10%, target disk busy <=5%, MemAvailable >=512 MiB, loopback background <=1 MiB/4096 packets, and a free trial port. No preflight threshold was relaxed.

```sh
export BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include'

# Dense socket/TCP_INFO: 100 RPS low-load baseline, 2 payloads x 3 repeats.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 1 --channels 1 --modes fixed --rate 100 --repetitions 3 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 100 --perf-mode off --output-dir benches/grpc_echo_kernel_data/diagnostic_baseline

# Dense socket/TCP_INFO saturation: 2 payloads x 1/4/8 channels x 3 repeats.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,4,8 --modes closed --repetitions 3 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 100 --perf-mode off --output-dir benches/grpc_echo_kernel_data/diagnostic_saturation

# Same-setting kernel-off bracket before diagnostic collection.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,8 --modes closed --repetitions 1 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 0 --perf-mode off --output-dir benches/grpc_echo_kernel_data/kernel_off_bracket_before

# Kernel-off saturation reference controls: 2 payloads x 1/4/8 channels x 3 repeats.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,4,8 --modes closed --repetitions 3 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 0 --perf-mode off --output-dir benches/grpc_echo_kernel_data/kernel_off_saturation

# Same-setting kernel-off bracket after diagnostic collection.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,8 --modes closed --repetitions 1 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 0 --perf-mode off --output-dir benches/grpc_echo_kernel_data/kernel_off_bracket_after

# CAP_PERFMON stat collection, original non-verbose batch retained.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,8 --modes closed --repetitions 3 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 0 --perf-mode stat --perf-bin /home/neojhou/repos/ledger-wallet/target/grpc-echo-perf --output-dir benches/grpc_echo_kernel_data/perf_stat_targeted

# Independent verbose stat batch: includes time-enabled/time-running per event.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,8 --modes closed --repetitions 3 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 0 --perf-mode stat --perf-bin /home/neojhou/repos/ledger-wallet/target/grpc-echo-perf --output-dir benches/grpc_echo_kernel_data/perf_stat_verbose_targeted

# Representative 49 Hz DWARF CPU stack records; not used for throughput comparison.
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128,4096 --concurrencies 256 --channels 1,8 --modes closed --repetitions 1 --warmup-seconds 5 --seconds 20 --kernel-sample-ms 0 --perf-mode record --perf-bin /home/neojhou/repos/ledger-wallet/target/grpc-echo-perf --output-dir benches/grpc_echo_kernel_data/perf_record_targeted
```

The completed invocation output and each run's `run.txt`, `preflight.log`, `perf_preflight.log`, trial diagnostics, CSVs, and raw perf data are retained next to these output directories. The perf-stat target was run twice so event scheduling ratios could be checked; each batch has its own three repeats and is analyzed separately.
