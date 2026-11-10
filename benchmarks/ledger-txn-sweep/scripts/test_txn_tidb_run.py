import pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import txn_tidb_run as t
class ProfileTests(unittest.TestCase):
 def test_default_profiles_unchanged(self):
  self.assertEqual(t.select_profile(4),t.PROFILES[4]);self.assertEqual(t.select_profile(8),t.PROFILES[8])
 def test_extended_profile_and_invalid_cpu(self):
  self.assertEqual(t.select_profile(8,True),{'tikv':['0-3','4-7'],'tidb':'10-15','client':'8-9','sampler':'8-9'})
  with self.assertRaises(ValueError):t.select_profile(4,True)
 def test_tidb_cpuset_size_derives_4_or_6(self):
  from resource_profiles import cpus_from_list
  self.assertEqual(len(cpus_from_list(t.select_profile(8)['tidb'])),4)
  self.assertEqual(len(cpus_from_list(t.select_profile(8,True)['tidb'])),6)

import datetime
import socket
import tempfile
from unittest import mock
from batch_queue_run import NetSampler, net_rate_summary, parse_net_dev


def metrics(sync, write, extra=''):
    return '\n'.join([
        f'{t.METRICS[0]}{{store="x"}} {sync}',
        f'{t.METRICS[1]} {write}',
        f'{t.METRICS[2]} 100',
        f'{t.METRICS[3]} 200',
        extra,
    ])


class DurabilityTests(unittest.TestCase):
    def test_ok(self):
        result = t.durability_delta([metrics(10, 20)], [metrics(15, 25)])
        self.assertTrue(result['valid'])
        self.assertEqual(result['sync_count_delta_total'], 5)
        self.assertEqual(result['write_count_delta_total'], 5)

    def test_absent_counter_fails(self):
        before = metrics(10, 20).replace(f'{t.METRICS[2]} 100\n', '')
        with self.assertRaisesRegex(ValueError, 'missing durability counter'):
            t.durability_delta([before], [metrics(15, 25)])

    def test_reset_counter_fails(self):
        with self.assertRaisesRegex(ValueError, 'invalid/reset durability counter'):
            t.durability_delta([metrics(10, 20)], [metrics(9, 25)])

    def test_zero_write_delta_fails(self):
        with self.assertRaisesRegex(ValueError, 'deltas must be positive'):
            t.durability_delta([metrics(10, 20)], [metrics(15, 20)])

    def test_sums_labelled_and_unlabelled_exact_metric(self):
        text = (f'{t.METRICS[0]}{{store="a"}} 2\n'
                f'{t.METRICS[0]}{{store="b"}} 3\n'
                f'{t.METRICS[0]}_total 100\n')
        self.assertEqual(t.metric_value(text, t.METRICS[0]), 5)
        self.assertEqual(t.metric_value(f'{t.METRICS[0]} 4\n{t.METRICS[0]}{{x="y"}} 6', t.METRICS[0]), 10)

    def test_sync_write_mismatch_beyond_one_percent_fails(self):
        with self.assertRaisesRegex(ValueError, 'more than 1%'):
            t.durability_delta([metrics(10, 10)], [metrics(12, 11)])

    def test_two_store_aggregate(self):
        result = t.durability_delta(
            [metrics(0, 0), metrics(100, 100)],
            [metrics(6, 5), metrics(104, 105)])
        self.assertEqual(result['sync_count_delta_total'], 10)
        self.assertEqual(result['write_count_delta_total'], 10)


class WrapperHelperTests(unittest.TestCase):
    def test_free_port_succeeds_and_active_listener_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            with mock.patch.object(t.single_run, 'listener_diagnostics', return_value=[]):
                t.preflight_endpoint(pathlib.Path(directory), port)
                with socket.socket() as listener:
                    listener.bind(('127.0.0.1', port))
                    listener.listen()
                    with self.assertRaisesRegex(RuntimeError, 'bind failed'):
                        t.preflight_endpoint(pathlib.Path(directory), port)

    def test_tolerates_matching_recent_permission_error_only(self):
        class Sampler:
            def __init__(self):
                self.out = pathlib.Path('sampler')
                self.errors = []
        sampler = Sampler()
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp = now.isoformat()
        sampler.errors = [
            f'{stamp} client proc: /proc/123/io: Permission denied',
            f'{stamp} client proc: /proc/456/io: Permission denied',
            f'{stamp} client proc: /proc/123/io: other error',
            f'{(now-datetime.timedelta(seconds=5)).isoformat()} client proc: /proc/123/io: Permission denied',
            'unparseable client proc: /proc/123/io: Permission denied',
        ]
        tolerated = t.partition_client_exit_errors([sampler], 123, now)
        self.assertEqual(len(tolerated), 1)
        self.assertEqual(len(sampler.errors), 4)

    def test_parse_proc_net_dev_colon_spacing(self):
        parsed = parse_net_dev('''Inter-| Receive | Transmit
 face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed
eth0:  11 2 0 0 0 0 0 0 22 3 0 0 0 0 0 0
eth1 : 33 4 0 0 0 0 0 0 44 5 0 0 0 0 0 0
''')
        self.assertEqual(parsed, {'eth0': (11, 22), 'eth1': (33, 44)})

    def test_window_summary_uses_only_wholly_contained_consecutive_intervals(self):
        rows = [
            {'wall_utc': '1970-01-01T00:00:09Z', 'rx_bytes': '0', 'tx_bytes': '0'},
            {'wall_utc': '1970-01-01T00:00:10Z', 'rx_bytes': '1048576', 'tx_bytes': '1048576'},
            {'wall_utc': '1970-01-01T00:00:11Z', 'rx_bytes': '2097152', 'tx_bytes': '2097152'},
            {'wall_utc': '1970-01-01T00:00:12Z', 'rx_bytes': '3145728', 'tx_bytes': '3145728'},
            {'wall_utc': '1970-01-01T00:00:13Z', 'rx_bytes': '4194304', 'tx_bytes': '4194304'},
        ]
        summary = net_rate_summary(rows, 10_000_000_000, 12_000_000_000)
        self.assertEqual(summary['intervals'], 2)
        self.assertEqual(summary['covered_seconds'], 2)
        self.assertEqual([r['start_unix_seconds'] for r in summary['rate_intervals']], [10, 11])

    def test_resolve_connections_defaults_to_queues_and_accepts_valid_range(self):
        self.assertEqual(t.resolve_connections(None, 8), 8)
        self.assertEqual(t.resolve_connections(3, 8), 3)
        self.assertEqual(t.resolve_connections(1, 8), 1)

    def test_resolve_connections_invalid_raises_value_error(self):
        for value in (0, 9, -1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                t.resolve_connections(value, 8)

    def test_client_command_forwards_connections(self):
        command = t.tidb_client_command('client', 2, 3, 8, 5, 4, 250,
                                        'out', 'ready', 'go', 2, 0)
        index = command.index('--connections')
        self.assertEqual(command[index + 1], '5')


class TikvCpu6ProfileTests(unittest.TestCase):
    def test_cpu6_profile_and_invalid_combinations(self):
        self.assertEqual(t.select_profile(8, True, 6), {'tikv': ['0-2', '4-6'], 'tidb': '3,7,10-15', 'client': '8-9', 'sampler': '8-9'})
        for cpus, extended in ((4, True), (8, False)):
            with self.subTest(cpus=cpus, extended=extended), self.assertRaisesRegex(ValueError, '--cpus 8 and --tidb-extended'):
                t.select_profile(cpus, extended, 6)

    def test_quota_propagation_helpers(self):
        quota = t.tikv_quota_per_store(6, 2)
        self.assertEqual(quota, 3)
        self.assertEqual(quota * 2, 6)
        self.assertEqual(t.tikv_run_args('0-2', quota), ['--cpuset-cpus', '0-2', '--cpus', '3'])
        self.assertEqual(t.tikv_cpu_pct_of_quota(3, 6), 50)
        self.assertEqual(t.tikv_quota_per_store(4, 1), 4)
        self.assertEqual(t.tikv_cpu_pct_of_quota(2, 4), 50)

    def test_default_profile_and_quota_stay_unchanged(self):
        self.assertEqual(t.select_profile(8), t.PROFILES[8])
        self.assertEqual(t.tikv_quota_per_store(4, 2), 2)
        self.assertEqual(t.tikv_cpu_pct_of_quota(4, 8), 50)


if __name__ == '__main__':
    unittest.main()
