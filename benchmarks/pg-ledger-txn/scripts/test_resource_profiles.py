import unittest
from resource_profiles import get_profile, validate_client_cpus

class ResourceProfileTests(unittest.TestCase):
    def test_approved_profiles(self):
        p3,p6=get_profile(3),get_profile(6)
        self.assertEqual((p3.db_cores,p3.quota,p3.shards,p3.client_cpus,p3.workers,p3.sampler_cpus),('0-2',3,3,'3-5',3,'6-7'))
        self.assertEqual((p6.db_cores,p6.quota,p6.shards,p6.client_cpus,p6.workers,p6.sampler_cpus),('0-5',6,6,'6-7',2,'14-15'))
    def test_diagnostic_client_cpus_reject_db_and_smt_overlap(self):
        validate_client_cpus(get_profile(6),'6,7,14,15')
        for cpus in ('0','8','13','0,14'):
            with self.subTest(cpus=cpus), self.assertRaises(ValueError): validate_client_cpus(get_profile(6),cpus)
        for cpus in ('0','8','10'):
            with self.subTest(cpus=cpus), self.assertRaises(ValueError): validate_client_cpus(get_profile(3),cpus)
    def test_cpu_list_validation(self):
        for value in ('','3-1','x','1-2-3'):
            with self.subTest(value=value), self.assertRaises(ValueError): validate_client_cpus(get_profile(6),value)
        with self.assertRaises(ValueError): get_profile(4)

if __name__=='__main__': unittest.main()
