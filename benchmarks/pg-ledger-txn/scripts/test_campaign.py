import pathlib, signal, subprocess, tempfile, unittest
from unittest.mock import Mock, patch
from campaign import (next_k, plateau_candidate, candidate_n, run_accepted,
                      run_accepted_calibration, run_level, signal_handler)

class CampaignCoreTests(unittest.TestCase):
    def test_per_user_k_formula(self):
        self.assertEqual(next_k(100,35,1),4025);self.assertEqual(next_k(100,35,4),1007);self.assertEqual(next_k(100,35,8),504)
    def test_latest_three_plateau_ignores_old_plateau_when_later_rising(self):
        self.assertEqual(candidate_n({1:100,2:102,4:103}),4);self.assertIsNone(candidate_n({1:100,2:102,4:103,8:140,16:150}));self.assertFalse(plateau_candidate({1:100,2:102,4:103,8:140,16:150}))
    def test_calibration_rejects_missing_sampler_closed(self):
        ok,why=run_accepted_calibration({'returncode':0,'result':{},'resource':{}},1,1);self.assertFalse(ok);self.assertIn('sampler_errors',why)
    def test_invalid_calibration_stops_before_any_level_run(self):
        from campaign import main
        bad={'returncode':1,'result':{},'resource':{},'verification':{},'sockets':{},'sampler_errors':{'errors':['bad']},'cleanup':{}}
        with tempfile.TemporaryDirectory() as td, patch('sys.argv',['campaign.py','--output',td,'--max-users','1','--min-free-memory-gb','0','--min-free-disk-gb','0']), patch('campaign.metadata',return_value={}), patch('campaign.invoke',return_value=1) as invoke, patch('campaign.load_run',return_value=bad):
            self.assertEqual(main(),2);self.assertEqual(invoke.call_count,1);self.assertIn('single_run.py',invoke.call_args.args[0][1]);self.assertTrue((pathlib.Path(td)/'campaign.json').exists());self.assertTrue((pathlib.Path(td)/'levels.csv').exists())
    def test_three_paired_repetitions_alternate_order_exactly(self):
        calls=[]
        def run(b,n,k,rep):calls.append((b,rep,k));return {'returncode':0,'result':{'business_rps':10}}
        level,attempts=run_level(4,99,3,run,lambda r,n,k:(True,[]));self.assertTrue(level['valid']);self.assertEqual(len(attempts),6);self.assertEqual(calls,[('pg',1,99),('scylla',1,99),('scylla',2,99),('pg',2,99),('pg',3,99),('scylla',3,99)])
    def test_short_run_retries_whole_paired_level_with_one_shared_larger_k(self):
        calls=[]
        def run(b,n,k,rep):calls.append((b,rep,k));return {'returncode':0,'result':{'elapsed_ms':1000 if k==100 and b=='pg' else 30000}}
        level,attempts=run_level(2,100,3,run,lambda r,n,k:(k!=100,['too_short'] if k==100 else []));self.assertTrue(level['valid']);self.assertEqual(level['retry_of_K'],100);rk=level['K'];self.assertGreater(rk,100);self.assertEqual(calls[:1],[('pg',1,100)]);self.assertEqual(calls[1:],[('pg',1,rk),('scylla',1,rk),('scylla',2,rk),('pg',2,rk),('pg',3,rk),('scylla',3,rk)]);self.assertEqual({x[2] for x in calls[1:]},{rk})
    def test_signal_waits_for_child_cleanup(self):
        child=Mock();child.poll.return_value=None
        with patch('campaign._CHILD',child),self.assertRaises(SystemExit) as raised:signal_handler(signal.SIGTERM,None)
        child.send_signal.assert_called_once_with(signal.SIGTERM);child.wait.assert_called_once_with();self.assertEqual(raised.exception.code,143)
    def test_existing_output_refused_without_touching_results(self):
        from campaign import main
        with tempfile.TemporaryDirectory() as td:
            p=pathlib.Path(td)/'journal.json';p.write_text('old')
            with patch('sys.argv',['campaign.py','--output',td]),self.assertRaises(SystemExit) as raised:main()
            self.assertEqual(raised.exception.code,2);self.assertEqual(p.read_text(),'old')
    def test_reduced_duration_requires_explicit_smoke_and_no_formal_run_starts(self):
        from campaign import main
        with tempfile.TemporaryDirectory() as td,patch('sys.argv',['campaign.py','--output',td,'--min-seconds','2','--target-seconds','3']),self.assertRaises(SystemExit) as raised:main()
        self.assertEqual(raised.exception.code,2);self.assertFalse((pathlib.Path(td)/'journal.json').exists())

if __name__=='__main__':unittest.main()
