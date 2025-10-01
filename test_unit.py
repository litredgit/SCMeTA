import unittest
import pandas as pd
from SCMeTA import Process

class TestSCMeTA(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.ref_path = "Data"
        self.test_type = "mzML"
        self.ref = Process()
        self.sample = Process()
        self.sample.load(
            path=self.ref_path + "/input", 
            data_type=self.test_type, 
            method="one by one"
        )

    def test_raw(self):
        self.ref.load(
            path=self.ref_path + "/output/baseline_" + self.test_type + "_raw", 
            data_type="process", 
            target_attr="raw",
            method="one by one"
        )
        # Compare the raw data of sample and reference
        for name, ref_data in self.ref.data.items():
            sample_data = self.sample.data.get(name)
            self.assertIsNotNone(sample_data, f"Sample data for {name} is missing.")
            pd.testing.assert_frame_equal(
                sample_data.raw, 
                ref_data.raw, 
                atol=0.01,
                check_dtype=False
            )

    def test_process(self):
        self.ref.load(
            path=self.ref_path + "/output/baseline_" + self.test_type + "_process", 
            data_type="process", 
            target_attr="process",
            method="one by one"
        )
        self.sample.pre_process(clear_mem=True)
        # Compare the process data of sample and reference
        for name, ref_data in self.ref.data.items():
            sample_data = self.sample.data.get(name)
            self.assertIsNotNone(sample_data, f"Sample data for {name} is missing.")
            pd.testing.assert_frame_equal(
                sample_data.process, 
                ref_data.process, 
                atol=0.01,
                check_dtype=False
            )

    def test_mat(self):
        self.ref.load(
            path=self.ref_path + "/output/baseline_" + self.test_type + "_mat", 
            data_type="process", 
            target_attr="cell_mat",
            method="one by one"
        )
        self.sample.pre_process()
        self.sample.gen_mat()
        self.sample.clear_memory(attributes=["process"])
        self.sample.round_mat()
        self.sample.combine_peaks()
        self.sample.denoise()
        # Compare the mat of sample and reference
        for name, ref_data in self.ref.data.items():
            sample_data = self.sample.data.get(name)
            self.assertIsNotNone(sample_data, f"Sample data for {name} is missing.")
            pd.testing.assert_frame_equal(
                sample_data.mat, 
                ref_data.mat, 
                atol=0.01,
                check_dtype=False
            )

    def test_cell_mat(self):
        self.ref.load(
            path=self.ref_path + "/output/baseline_" + self.test_type + "_cell_mat", 
            data_type="process", 
            target_attr="cell_mat",
            method="one by one"
        )
        self.sample.pre_process()
        self.sample.process(filter_method="any")
        # Compare the cell_mat of sample and reference
        for name, ref_data in self.ref.data.items():
            sample_data = self.sample.data.get(name)
            self.assertIsNotNone(sample_data, f"Sample data for {name} is missing.")
            pd.testing.assert_frame_equal(
                sample_data.cell_mat, 
                ref_data.cell_mat, 
                atol=0.01,
                check_dtype=False
            )

    def test_post(self):
        self.ref.load(
            path=self.ref_path + "/output/baseline_" + self.test_type + "_post", 
            data_type="process", 
            target_attr="cell_mat",
            method="one by one"
        )
        self.sample.pre_process()
        self.sample.process(filter_method="any")
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
            
def gen_ref(
    path:str = "Data",
    type:str = "mzML",
    savelist:list = ["raw", "process", "mat", "cell_mat", "post"]
    ):
    ref = Process()
    # Generate reference raw data
    ref.load(
        path=path + "/input", 
        data_type=type, 
        method="one by one"
    )
    if "raw" in savelist:
        ref.save(
            path=path + "/output/baseline_" + type + "_raw",
            attr="raw"
        )

    # Generate reference process data
    ref.pre_process(clear_mem=True)
    if "process" in savelist:
        ref.save(
            path=path + "/output/baseline_" + type + "_process", 
            attr="process"
        )

    # Generate reference mat data
    ref.gen_mat()
    ref.clear_memory(attributes=["process"])
    ref.round_mat()
    ref.combine_peaks()
    ref.denoise()
    if "mat" in savelist:
        ref.save(
            path=path + "/output/baseline_" + type + "_mat", 
            attr="mat"
        )
    
    # Generate reference cell_mat data
    ref.merge_cell()
    ref.filter_assem()
    ref.filter_mat(method="any")
    ref.clear_memory()
    if "cell_mat" in savelist:
        ref.save(
            path=path + "/output", 
            attr="cell_mat"
        )

    # Generate reference post processed cell_mat data
    ref.post_process()
    if "post" in savelist:
        ref.save(
            path=path + "/output/baseline_" + type + "_post", 
            attr="cell_mat"
        )

if __name__ == '__main__':
    # Uncomment the following line to generate reference data
    # gen_ref()
    unittest.main()