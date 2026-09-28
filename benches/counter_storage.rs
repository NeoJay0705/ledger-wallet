use cpu_time::ProcessTime;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const DEFAULT_ITERATIONS: u64 = 10_000_000;
const DEFAULT_REPETITIONS: usize = 3;
const FIXED_KEY: u64 = 1;

#[derive(Clone, Copy)]
struct Config {
    iterations: u64,
    repetitions: usize,
}

#[derive(Clone, Copy)]
struct Sample {
    wall_time: Duration,
    process_cpu_time: Duration,
    final_value: u64,
}

#[derive(Clone, Copy)]
struct Metrics {
    wall_seconds: f64,
    requests_per_second: f64,
    latency_ns_per_op: f64,
    process_cpu_delta_seconds: f64,
    cpu_core_equiv: f64,
    one_core_cpu_percent: f64,
    normalized_cpu_percent: f64,
}

fn parse_positive<T>(flag: &str, value: Option<String>) -> Result<T, String>
where
    T: std::str::FromStr,
{
    let parsed = value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<T>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    Ok(parsed)
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config {
        iterations: DEFAULT_ITERATIONS,
        repetitions: DEFAULT_REPETITIONS,
    };

    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--iterations" => {
                config.iterations = parse_positive("--iterations", args.next())?;
            }
            "--repetitions" => {
                config.repetitions = parse_positive("--repetitions", args.next())?;
            }
            // Cargo passes this flag to harness-free benches.
            "--bench" => {}
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if config.iterations == 0 {
        return Err("--iterations must be greater than zero".to_owned());
    }
    if config.repetitions == 0 {
        return Err("--repetitions must be greater than zero".to_owned());
    }
    Ok(Some(config))
}

fn print_help() {
    println!(
        "Usage: counter_storage [--iterations N] [--repetitions N]\n\
         Defaults: --iterations {DEFAULT_ITERATIONS}, --repetitions {DEFAULT_REPETITIONS}"
    );
}

fn run_stack(iterations: u64) -> Sample {
    let mut value = 0_u64;
    let value_ptr = &mut value as *mut u64;

    let process_cpu_started = ProcessTime::now();
    let wall_started = Instant::now();
    for _ in 0..iterations {
        // Volatile accesses keep every scalar read and write in the measured loop.
        unsafe {
            let current = std::ptr::read_volatile(value_ptr);
            std::ptr::write_volatile(value_ptr, current.wrapping_add(1));
        }
    }
    let wall_time = wall_started.elapsed();
    let process_cpu_time = process_cpu_started.elapsed();

    Sample {
        wall_time,
        process_cpu_time,
        final_value: value,
    }
}

fn run_boxed(iterations: u64) -> Sample {
    let mut value = Box::new(0_u64);
    let value_ptr = value.as_mut() as *mut u64;

    let process_cpu_started = ProcessTime::now();
    let wall_started = Instant::now();
    for _ in 0..iterations {
        // Volatile accesses keep every heap scalar read and write in the measured loop.
        unsafe {
            let current = std::ptr::read_volatile(value_ptr);
            std::ptr::write_volatile(value_ptr, current.wrapping_add(1));
        }
    }
    let wall_time = wall_started.elapsed();
    let process_cpu_time = process_cpu_started.elapsed();

    Sample {
        wall_time,
        process_cpu_time,
        final_value: *value,
    }
}

fn run_hash_map(iterations: u64) -> Sample {
    let mut values = HashMap::from([(FIXED_KEY, 0_u64)]);

    let process_cpu_started = ProcessTime::now();
    let wall_started = Instant::now();
    for _ in 0..iterations {
        let key = black_box(FIXED_KEY);
        let values = black_box(&mut values);
        let value = values
            .get_mut(&key)
            .expect("the fixed key is inserted before timing");
        *value = value.wrapping_add(1);
        black_box(value);
    }
    let wall_time = wall_started.elapsed();
    let process_cpu_time = process_cpu_started.elapsed();

    Sample {
        wall_time,
        process_cpu_time,
        final_value: *values
            .get(&FIXED_KEY)
            .expect("the fixed key remains in the map"),
    }
}

fn calculate_metrics(sample: Sample, iterations: u64, available_parallelism: usize) -> Metrics {
    let wall_seconds = sample.wall_time.as_secs_f64();
    let process_cpu_delta_seconds = sample.process_cpu_time.as_secs_f64();
    let cpu_core_equiv = process_cpu_delta_seconds / wall_seconds;

    Metrics {
        wall_seconds,
        requests_per_second: iterations as f64 / wall_seconds,
        latency_ns_per_op: sample.wall_time.as_nanos() as f64 / iterations as f64,
        process_cpu_delta_seconds,
        cpu_core_equiv,
        one_core_cpu_percent: 100.0 * cpu_core_equiv,
        normalized_cpu_percent: 100.0 * cpu_core_equiv / available_parallelism as f64,
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn print_metrics(label: &str, metrics: Metrics) {
    println!(
        "{label} wall_seconds={:.9} rps={:.2} latency_ns_per_op={:.3} \
         process_cpu_delta_seconds={:.9} cpu_core_equiv={:.4} \
         one_core_cpu_percent={:.2}% normalized_cpu_percent={:.2}%",
        metrics.wall_seconds,
        metrics.requests_per_second,
        metrics.latency_ns_per_op,
        metrics.process_cpu_delta_seconds,
        metrics.cpu_core_equiv,
        metrics.one_core_cpu_percent,
        metrics.normalized_cpu_percent,
    );
}

fn summarize(name: &str, rounds: &[Metrics]) {
    let median_metrics = Metrics {
        wall_seconds: median(rounds.iter().map(|round| round.wall_seconds).collect()),
        requests_per_second: median(
            rounds
                .iter()
                .map(|round| round.requests_per_second)
                .collect(),
        ),
        latency_ns_per_op: median(rounds.iter().map(|round| round.latency_ns_per_op).collect()),
        process_cpu_delta_seconds: median(
            rounds
                .iter()
                .map(|round| round.process_cpu_delta_seconds)
                .collect(),
        ),
        cpu_core_equiv: median(rounds.iter().map(|round| round.cpu_core_equiv).collect()),
        one_core_cpu_percent: median(
            rounds
                .iter()
                .map(|round| round.one_core_cpu_percent)
                .collect(),
        ),
        normalized_cpu_percent: median(
            rounds
                .iter()
                .map(|round| round.normalized_cpu_percent)
                .collect(),
        ),
    };
    print_metrics(&format!("{name} median"), median_metrics);
}

fn run_benchmark(name: &str, iterations: u64, repetitions: usize, available_parallelism: usize) {
    println!("\n[{name}]");
    let mut rounds = Vec::with_capacity(repetitions);
    for round in 1..=repetitions {
        let sample = match name {
            "stack_u64" => run_stack(iterations),
            "boxed_u64" => run_boxed(iterations),
            "hash_map_u64" => run_hash_map(iterations),
            _ => unreachable!("all benchmark names are declared below"),
        };
        assert_eq!(
            sample.final_value, iterations,
            "{name} round {round} produced an unexpected final counter"
        );

        let metrics = calculate_metrics(sample, iterations, available_parallelism);
        print_metrics(&format!("round={round}"), metrics);
        println!("round={round} final_value={} status=ok", sample.final_value);
        rounds.push(metrics);
    }
    summarize(name, &rounds);
}

fn main() {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => {
            print_help();
            return;
        }
        Err(error) => {
            eprintln!("error: {error}\nUse --help to see accepted options.");
            std::process::exit(2);
        }
    };
    let available_parallelism = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);

    println!(
        "counter_storage iterations_per_round={} repetitions={} available_parallelism={}",
        config.iterations, config.repetitions, available_parallelism
    );
    run_benchmark(
        "stack_u64",
        config.iterations,
        config.repetitions,
        available_parallelism,
    );
    run_benchmark(
        "boxed_u64",
        config.iterations,
        config.repetitions,
        available_parallelism,
    );
    run_benchmark(
        "hash_map_u64",
        config.iterations,
        config.repetitions,
        available_parallelism,
    );
}
