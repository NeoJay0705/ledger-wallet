import unittest

from batch_queue_run import (expected_socket_count, net_rate_summary, parse_net_dev,
                             pg_client_command, resolve_connections)


class BatchQueueConnectionsTests(unittest.TestCase):
    def command(self, mode, connections=4):
        return pg_client_command(mode, '/client', 'postgres://db', 10, 20, 8, connections,
                                 50, 250, '/out/client', '/ready', '/go')

    def test_txn_forwards_explicit_connections(self):
        cmd = self.command('txn', 3)
        self.assertEqual(cmd[cmd.index('--connections') + 1], '3')

    def test_connections_default_to_queues_and_forward(self):
        connections = resolve_connections(None, 8)
        cmd = self.command('txn', connections)
        self.assertEqual(connections, 8)
        self.assertEqual(cmd[cmd.index('--connections') + 1], '8')

    def test_invalid_connections_are_rejected(self):
        for value in (0, 9):
            with self.subTest(value=value), self.assertRaises(ValueError):
                resolve_connections(value, 8)

    def test_socket_census_uses_connections_plus_monitor_for_explicit_and_default(self):
        self.assertEqual(expected_socket_count('txn', 3, 8), 4)
        default = resolve_connections(None, 8)
        self.assertEqual(expected_socket_count('txn', default, 8), 9)

    def test_other_modes_keep_their_existing_connections_behavior(self):
        batch = self.command('batch', 3)
        self.assertNotIn('--connections', batch)
        single = self.command('single', 3)
        self.assertEqual(single[single.index('--connections') + 1], '8')


class BatchQueueNetTests(unittest.TestCase):
    def test_parse_proc_net_dev_colon_spacing(self):
        text = '''Inter-| Receive | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n  eth0: 100 2 0 0 0 0 0 0 900 3 0 0 0 0 0 0\nlo:200 4 0 0 0 0 0 0 800 5 0 0 0 0 0 0\n'''
        self.assertEqual(parse_net_dev(text), {'eth0': (100, 900), 'lo': (200, 800)})

    def test_window_summary_uses_only_wholly_contained_consecutive_intervals(self):
        rows = [
            {'wall_utc': '2025-01-01T00:00:00+00:00', 'rx_bytes': '0', 'tx_bytes': '0'},
            {'wall_utc': '2025-01-01T00:00:01+00:00', 'rx_bytes': str(1048576), 'tx_bytes': str(2097152)},
            {'wall_utc': '2025-01-01T00:00:02+00:00', 'rx_bytes': str(3*1048576), 'tx_bytes': str(6*1048576)},
            {'wall_utc': '2025-01-01T00:00:03+00:00', 'rx_bytes': str(6*1048576), 'tx_bytes': str(12*1048576)},
            {'wall_utc': '2025-01-01T00:00:04+00:00', 'rx_bytes': str(10*1048576), 'tx_bytes': str(20*1048576)},
        ]
        summary = net_rate_summary(rows, 1735689601 * 1_000_000_000, 1735689603 * 1_000_000_000)
        self.assertEqual(summary['intervals'], 2)
        self.assertEqual(summary['covered_seconds'], 2)
        self.assertEqual(summary['rx_mib_s_avg'], 2.5)
        self.assertEqual(summary['rx_mib_s_max'], 3)
        self.assertEqual(summary['tx_mib_s_avg'], 5)
        self.assertEqual(summary['tx_mib_s_max'], 6)


if __name__ == '__main__':
    unittest.main()
