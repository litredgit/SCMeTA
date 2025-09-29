import unittest
import pandas as pd

class TestSCMeTA(unittest.TestCase):
    
    def setUp(self):
        self.ref_path = "Data"
        self.test_type = "mzML"
        from SCMeTA import Process
        self.ref = Process()
        self.ref.load(
            path=self.ref_path + "/output/baseline_" + self.test_type, 
            data_type="process", 
            target_attr="cell_mat",
            method="one by one"
        )
    
    def test_all(self):
        from SCMeTA import Process
        self.sample = Process(renew=True)
        self.sample.load(
            path=self.ref_path + "/input", 
            data_type=self.test_type, 
            method="one by one"
        )
        self.sample.pre_process(clear_mem=True)
        self.sample.process(filter_method="any", clear_mem=True)
        self.sample.post_process()
        # Compare the processed cell_mat with the reference
        for name, ref_data in self.ref.data.items():
            sample_data = self.sample.data.get(name)
            self.assertIsNotNone(sample_data, f"Sample data for {name} is missing.")
            pd.testing.assert_frame_equal(
                sample_data.cell_mat, 
                ref_data.cell_mat, 
                atol=0.01,
                check_dtype=False
            )
            
if __name__ == '__main__':
    unittest.main()