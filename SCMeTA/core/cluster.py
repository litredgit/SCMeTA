import os
import logging

import pandas as pd
import numpy as np

from SCMeTA.method import (
    filter_occ,
    to_mat,
    to_list,
    find_cell,
    merge_cell,
    noise_subtract,
    filter_assem,
    filter_mat,
    normalize,
    round_columns,
    convert_format
)
from SCMeTA.batch import combat_batch_correction
from SCMeTA.method.fill import fill_mat
from SCMeTA.file import load_data, load_from_database
from SCMeTA.config import PARAMETERS
from SCMeTA.accelerate import MultiProcessing

from SCMeTA.file.format import SCData

logger = logging.getLogger(__name__)

class Process:
    def __init__(
        self, ref_mz: float = 760.58, mz1: float = 760.58, mz2: float = 791.34, mz3: float = 732.55
    ):
        self.data: dict[str, SCData] = {}

        self.__mz1: float = mz1
        self.__mz2: float = mz2
        self.__mz3: float = mz3
        self.ref_mz: float = ref_mz
        self.__dir = None
        self.__mp = MultiProcessing()
        pass

    def load(
        self,
        path: str,
        file_name: str | None = None,
        data_type: str = "thermo",
        method: str = "MultiThread"
    ):
        """
        Add a file to the MSProcess
        Args:
            path: File path or directory path.
            file_name: File Name, cannot work if path is a directory.
            data_type: Data type, thermo_raw file / water wiff file / mzML file / processed csv file are supported.
            method: Method to load the data, default "MultiThread", can be "one by one", "MultiProcess".
        """
        self.data.update(load_data(path, file_name, data_type, method))
        self.__dir = os.path.dirname(path)
        if not self.data:
            raise ValueError("No data loaded")
        logger.info(f"Loaded files in path {[path]}.")

    def load_database(self, file_id: int | list[int]):
        """
        Load data from database.
        Args:
            file_id: File ID in the database.

        Returns:

        """
        data = load_from_database(file_id)
        for key, value in data.items():
            self.data.update(load_data(value, key, "database"))

    def load_processed(self, path, file_name: str | None = None, file_type: str = "cell_mat"):
        """
        Load processed data.
        Args:
            path: File path or directory path.
            file_name: File Name, cannot work if path is a directory.
            file_type: target file type, default "cell_mat", other types saved in SCData is also supported.

        Returns:

        """
        if file_type in ["cell_mat", "mat", "process", "raw"]:
            if os.path.isdir(path):
                for file in os.listdir(path):
                    if file.endswith(".csv"):
                        file_name = os.path.basename(file).split(".")[0]
                        data = pd.read_csv(os.path.join(path, file), index_col=0)
                        data.columns = [float(i) for i in data.columns]
                        self.data[file_name] = SCData(name=file_name).__setattr__(file_type, data)
            elif os.path.isfile(path) and path.endswith(".csv"):
                data = pd.read_csv(path, index_col=0)
                data.columns = [float(i) for i in data.columns]
                self.data[file_name] = SCData(name=file_name).__setattr__(file_type, data)
            else:
                raise ValueError("File not found or not a csv file.")
        else:
            raise ValueError(f"File_type{file_type} not found in SCData. Choose from ['cell_mat', 'mat', 'process', 'raw']")


    def offset_preprocess(
        self,
        file_list: list | None = None,
        offset: float | None = None,
        offset_method: str = "same",
    ):
        """
        offset the data
        Args:
            file_list: File name list.
            offset: Offset of the data, if None, it will ignore the offset step.
            offset_method: Method to set offset, default "same", can be "separatedly".
        """
        if offset_method == "same":
            for file in file_list:
                self.data[file].set_offset(offset=offset)
            print(f"Set Offset {offset} all the same.")
        elif offset_method == "separatedly":
            for file in file_list:
                offset = float(input(f"Please input the offset of {file}: "))
                self.data[file].set_offset(offset=offset)
                print(f"Set Offset {offset} in {file}.")
        else:
            raise ValueError("offset_method must be 'same' or 'separatedly'")
        logger.info(f"Set Offset done.")
            
    def cut_preprocess(
        self,
        file_list: list | None = None,
        cut_range: tuple[int, int] | None = None,
        cut_method: str = "same",
    ):
        """
        Cut the data
        Args:
            file_list: File name list.
            cut_range: Cut range of the data, if None, it will ignore the cut step.
            cut_method: Method to cut the data, default "same", can be "separatedly".
        """
        if cut_method == "same":
            for file in file_list:
                self.data[file].cut(start=cut_range[0], end=cut_range[1])
                print(f"Cut range {cut_range[0]} to {cut_range[1]} all the same.")
        elif cut_method == "separatedly":
            for file in file_list:
                cut_range = (
                    int(input(f"Please input the start of cut range of {file}: ")),
                    int(input(f"Please input the end of cut range of {file}: ")),
                )
                self.data[file].cut(start=cut_range[0], end=cut_range[1])
                print(f"Cut range {cut_range[0]} to {cut_range[1]} in {file}.")
        else:
            raise ValueError("cut_method must be 'same' or 'separatedly'")
        logger.info("Cut range done.")
        
    def filter_occ(
        self,
        count: int = PARAMETERS.count,
        resolution: float = PARAMETERS.resolution,
        file_name: str | None = None,
    ):
        """
        Filter out the data with low occurrence
        Args:
            count: Minimum occurrence of the data, default 10
            resolution: Resolution of the data, default 0.01
            file_name: File name, if None, all files will be processed.
        """
        if file_name is None:
            # self.__mp.run(self.data, _filter_occ, resolution, count)
            for ms_data in self.data.values():
                ms_data.process = filter_occ(ms_data.raw.copy(), resolution, count)
        else:
            self.data[file_name].process = filter_occ(
                self.data[file_name].raw.copy(), resolution, count
            )
            print(f"only for {file_name}")
        logger.info(f"Combine peaks rounded to {resolution}, Filter out mz occurred < {count}.")

    def gen_mat(self, file_name: str | None = None):
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.mat = to_mat(data=ms_data.process, min_intensity=PARAMETERS.min_intensity)
        else:
            self.data[file_name].mat = to_mat(data=self.data[file_name].process, min_intensity=PARAMETERS.min_intensity)
            print(f"only for {file_name}")
        logger.info("Data format converted to crosstab.")
        print(f"before:\n{self.data[file_name].process.head(5) if file_name else next(iter(self.data.values())).process.head(5)}\nafter:\n{self.data[file_name].mat.head(5) if file_name else next(iter(self.data.values())).mat.head(5)}")

    def round_mat(self, resolution_intensity: float = PARAMETERS.resolution_intensity, file_name: str | None = None):
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.mat = round_columns(ms_data.mat, resolution_intensity)
        else:
            self.data[file_name].mat = round_columns(
                self.data[file_name].mat, resolution_intensity
            )
            print(f"only for {file_name}")
        logger.info(f"Intensity rounded to resolution_intensity{resolution_intensity}.")

    def denoise(self, max_ratio: float = PARAMETERS.maxratio, file_name: str | None = None):
        """
        Find cell and subtract noise.
        Args:
            max_ratio: If ref_mz_intensity > max_ratio * ref_mz_max_intensity, the cell will be extracted
            file_name: File name, if None, all files will be processed.
        """
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.cell_pos = find_cell(
                    ms_data.mat, self.ref_mz, max_ratio=max_ratio
                )
                logger.info(f"Find cells with mz{self.ref_mz} > {max_ratio} * max intensity.")
                ms_data.mat = noise_subtract(ms_data.mat, ms_data.cell_pos)
        else:
            ms_data = self.data[file_name]
            ms_data.cell_pos = find_cell(
                ms_data.mat, self.ref_mz, max_ratio=max_ratio
            )
            print(f"only for {file_name}")
            logger.info(f"Find cells with mz{self.ref_mz} > {max_ratio} * max intensity.")
            ms_data.mat = noise_subtract(ms_data.mat, ms_data.cell_pos)
            self.data[file_name] = ms_data
        logger.info("Noise subtracted!")

    def merge_cell(self, adjacent: int = PARAMETERS.adjacent, file_name: str | None = None):
        """
        Combine the adjacent cells
        Args:
            adjacent: Number of adjacent cells to be combined, default 3.
            If number is larger than the number of cells, the data will be dropped.
            file_name: File name, if None, all files will be processed.
        """
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.cell_mat = merge_cell(
                    ms_data.mat, ms_data.cell_pos, adjacent=adjacent
                )
        else:
            self.data[file_name].cell_mat = merge_cell(
                self.data[file_name].mat,
                self.data[file_name].cell_pos,
                adjacent=adjacent,
            )
            print(f"only for {file_name}")
        logger.info(f"Merge adjacent cells less than {adjacent}.")

    def filter_assem(self, snr: float = PARAMETERS.snr, file_name: str | None = None):
        """
        Filter out the data with Intensity/Noise < snr
        Args:
            snr: Minimum SNR of the data, default 3.0
            file_name: File name, if None, all files will be processed.
        """
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.cell_mat = filter_assem(
                    ms_data.mat, ms_data.cell_mat, ms_data.cell_pos, snr
                )
        else:
            self.data[file_name].cell_mat = filter_assem(
                self.data[file_name].mat,
                self.data[file_name].cell_mat,
                self.data[file_name].cell_pos,
                snr,
            )
            print(f"only for {file_name}")
        logger.info(f"Filter out the data with SNR < {snr}.")

    def gen_process(self, file_name: str | None = None):
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.process = to_list(ms_data.cell_mat, ms_data.cell_pos)
        else:
            self.data[file_name].process = to_list(
                self.data[file_name].cell_mat, self.data[file_name].cell_pos
            )

    def filter_mat(
        self,
        threshold: float = PARAMETERS.threshold,
        name_list: list[str] | None = None,
        lock_mz: bool = PARAMETERS.lock,
        method: str = "all"
    ):
        """
        Filter out the data with low occurrence
        Args:
            threshold: Minimum occurrence rate of the data, default 0.2
            name_list: File name list, if None, all files will be processed.
            lock_mz: If True, the mz you select in lock mz file will be locked, default False.
            method: Method to filter the data, default "all", can be "all", "any", "none".
        """
        if name_list is None:
            name_list = list(self.data.keys())
        else:
            print(f"only for {','.join(name_list)}")
        mat_list = [self.data[name].cell_mat for name in name_list]
        total_mat = list(filter_mat(mat_list, threshold, lock_mz, method))
        for index, name in enumerate(name_list):
            self.data[name].cell_mat = total_mat[index]
        logger.info(f"Filter out mz occurred > {threshold} * cell_count.")

    def normalize(self,
                  data: dict[str, SCData],
                  normalize_method: list[str],
                  file_name: str | None = None):
        if file_name is None:
            for ms_data in data.values():
                ms_data.cell_mat = normalize(
                    ms_data.cell_mat, normalize_method, mz=self.ref_mz
                )
        else:
            data[file_name].cell_mat = normalize(
                data[file_name].cell_mat, normalize_method, mz=self.ref_mz
            )
            print(f"only for {file_name}")
        logger.info(f"Normalized by {normalize_method}.")

    def fill(self,
             data: dict[str, SCData],
             file_name: str | None = None,
             fillna_method: str = "knn"
             ):
        if file_name is None:
            for ms_data in data.values():
                ms_data.cell_mat = fill_mat(ms_data.cell_mat, fillna_method)
        else:
            data[file_name].cell_mat = fill_mat(
                data[file_name].cell_mat, fillna_method
            )
            print(f"only for {file_name}")
        logger.info(f"Fillna with {fillna_method}.")

    def combat(self, data: dict[str, SCData], tag_list: list[str], file_name: str | None = None):
        data = combat_batch_correction(data, tag_list)
        return data

    def info(self, file_name: str | None = None):
        """
        Print the information of the MSProcess
        Args:
            file_name: File name, if None, all files' info will be printed.
        """
        if file_name is None:
            for ms_data in self.data.values():
                logger.info(
                    " ".join(
                        [
                            f"File name: {ms_data.name}",
                            f"Cells count: {ms_data.cell_count}",
                            f"Peaks count: {ms_data.cell_mat.shape[1]}",
                        ]
                    )
                )
        else:
            ms_data = self.data[file_name]
            logger.info(
                " ".join(
                    [
                        f"File name: {ms_data.name}",
                        f"Cells count: {ms_data.cell_count}",
                        f"Peaks count: {ms_data.cell_mat.shape[1]}",
                    ]
                )
            )

    def clear_memory(self, file_name: str | None = None):
        """
        Clear the memory of the MSProcess
        Args:
            file_name: File name, if None, all files will be processed.
        """
        if file_name is None:
            for value in self.data.values():
                value.clear()
        else:
            self.data[file_name].clear()
            print(f"only for {file_name}")
        logger.info("Memory cleared")

    def FormatConvert(
            self, 
            path: str | None = None,
            data_type: str = "cell_mat",
            type: str = "MetaboAnalyst",
            specify_label: bool = False
            ):
        """
        Convert the data to the specified format for further analysis in other softwares or websites like MetaboAnalyst.
        Args:
            path: Path to .csv like data. If none, SCData will be processed.
            data_type: File type, default "cell_mat", other types saved in SCData is also supported.
        """
        indict = {}
        outdf = pd.DataFrame()

        # Load data from path if path is not None
        if path is not None:
            self.load_processed(path=path, file_type=data_type)
        
        # indict = self.data, for convert_format
        if data_type in ["cell_mat", "mat", "process", "raw"]:
            indict = {name: self.data[name].__getattribute__(data_type) for name in self.data.keys()}
        else:
            raise ValueError("Data type not found in SCData. Choose from ['cell_mat', 'mat', 'process', 'raw']")

        # Optional: Specify the label
        if specify_label:
            logger.info("Specify the label.")
            print(f"Label: {indict.keys()}")
            label = input("Please input the label for each file by order, separated by comma: ")
            indict = {key: value for key, value in zip(label.split(","), indict.values())}
            print(f"Label changed to {indict.keys()}.")

        outdf = convert_format(indict=indict, type=type)

        if self.__dir is None:
            dir_path = path
        else:
            dir_path = self.__dir
        if dir_path is None:
            raise ValueError("Path is None, please specify the path.")
        
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        outdf.to_csv(os.path.join(dir_path, f"{type}.csv"))


    def save(self, file_name: str | None = None, data_type: str = "cell_mat", path: str | None = None):
        """
        Save the MSProcess
        Args:
            file_name: File name, if None, all files will be processed.
            data_type: File type, default "cell_mat", other types saved in SCData is also supported.
            path: Path to save the file, default None,
             which means the file will be saved in the same directory as the data you loaded.
        """
        if path is None:
            dir_path = os.path.join(self.__dir, "Process")
        else:
            dir_path = path

        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        if data_type not in ["cell_mat", "mat", "process", "raw"]:
            raise ValueError("Data type not supported.")
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.__getattribute__(data_type).to_csv(
                    os.path.join(dir_path, f"{ms_data.name}_{data_type}.csv")
                )
        else:
            self.data[file_name].__getattribute__(data_type).to_csv(
                os.path.join(dir_path, f"{file_name}_{data_type}.csv")
            )
        logger.info(f"Data saved in {dir_path}.")

    def pre_process(
        self,
        file_name: str | list[str] | None = None,
        offset: float | None = None,
        cut_range: tuple[int, int] | None = None,
        resolution: float = PARAMETERS.resolution,
        count: int = PARAMETERS.count,
        offset_method: str = "same",
        cut_method: str = "same",
        ):
        """
        Pre-process the data, including peak_combine, offset, cut, and round.
        Args:
            file_name: File name
            offset: Offset of the data, if None, it will ignore the offset step.
            cut_range: Cut range of the data, if None, it will ignore the cut step.
            resolution: Resolution of the data, default 0.01.
            count: Minimum occurrence of the data, default 10.
            offset_method: Method to set offset, default "same", can be "separatedly".
            cut_method: Method to cut the data, default "same", can be "separatedly".
        Returns:

        """
        if offset is None and cut_range is None:
            logger.info("No offset or cut range provided, skipping offset and cut.")
        else:
            # convert file_name or self.data.keys() to list
            if isinstance(file_name, str):
                file_list = [file_name]
            elif isinstance(file_name, list):
                file_list = file_name
            elif file_name is None:
                file_list = list(self.data.keys())
            else:
                raise ValueError("File name not str or list.")
            
            # Check if the file is in the data dictionary
            for file in file_list:
                if file not in list(self.data.keys()):
                    raise ValueError(f"{file} not in data")
                
            # do offset and cut
            if offset is not None:
                self.offset_preprocess(file_list, offset, offset_method)
            if cut_range is not None:
                self.cut_preprocess(file_list, cut_range, cut_method)

        self.filter_occ(resolution=resolution, count=count)
        logger.info("Pre-process finished.")

    def process(
            self,
            max_ratio: float = PARAMETERS.maxratio,
            adjacent: int = PARAMETERS.adjacent,
            snr: float = PARAMETERS.snr,
            resolution_intensity: float = PARAMETERS.resolution_intensity,
            threshold: float = PARAMETERS.threshold,
            lock_mz: bool = PARAMETERS.lock,
            filter_method: str = "all"
    ):
        """
        Args:
            max_ratio: If ref_mz_intensity > max_ratio * ref_mz_max_intensity, the cell will be extracted
            adjacent: The number of adjacent cells to be combined
            snr: Signal to noise ratio
            resolution_intensity: Resolution of the mass intensity
            threshold: Threshold of the data
            lock_mz: If True, the mz you select in lock mz file will be locked
            filter_method: Method of filtering
        """
        if not self.data:
            raise ValueError("No data loaded")

        self.gen_mat()
        self.round_mat(resolution_intensity=resolution_intensity)
        self.denoise(max_ratio=max_ratio)
        self.merge_cell(adjacent=adjacent)
        self.filter_assem(snr=snr)
        self.filter_mat(threshold=threshold, lock_mz=lock_mz, method=filter_method)
        self.info()
        self.clear_memory()
        logger.info("Process finished.")
        return self.data

    def post_process(
            self,
            data: dict[str, SCData] = None,
            normalize_method=None,
            fillna_method: str = "none",
            tags: list[str] = None
    ):
        """
        Args:
            data: Dict of MSData
            normalize_method: Method of normalization
            fillna_method: Method of filling nan
            tags: Tags of the data
        Returns:
            Dict of MSData
        """
        if normalize_method is None:
            normalize_method = ["mz"]
        if data is None:
            data = self.data
        self.normalize(data=data, normalize_method=normalize_method)
        self.fill(data=data, fillna_method=fillna_method)
        logger.info("Post process finished.")
        # self.combat()
        return data
