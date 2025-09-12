import os
import pandas as pd
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
from SCMeTA.config import PARAMETERS, setup_logger
from SCMeTA.accelerate import MultiProcessing

from SCMeTA.file.format import SCData

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

    def load(
        self,
        path: str,
        data_type: str = "thermo",
        method: str = "MultiThread"
    ):
        """
        Add a file to the MSProcess
        Args:
            path: File path or directory path.
            data_type: Data type, support:
                "thermo": [".raw", ".txt"],
                "process": [".csv"],
                "waters": [".wiff", ".txt"],
                "mzML": [".mzML", ".mzml"].
            method: Method to load the data, default "MultiThread", can be "one by one", "MultiProcess".
        """
        self.logger = setup_logger(log_file=os.path.dirname(path) + "/process.log")
        self.logger.info(f"Load files in path {[path]}.")
        self.data.update(load_data(path, data_type, method))
        self.__dir = os.path.dirname(path)
        if not self.data:
            raise ValueError("No data loaded")

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

    def load_processed(self, path: str, file_type: str = "cell_mat"):
        """
        Load processed data.
        Args:
            path: File path or directory path.
            file_type: target file type, default "cell_mat", other types saved in SCData is also supported.

        Returns:

        """
        if file_type in ["cell_mat", "mat", "process", "raw"]:
            if os.path.isdir(path):
                for file in os.listdir(path):
                    if file.endswith(".csv"):
                        # read csv file
                        file_name = os.path.basename(file).split(".")[0]
                        data = pd.read_csv(os.path.join(path, file), index_col=0)
                        data.columns = [float(i) for i in data.columns]
                        # set self.data[file_name] to SCData with read data
                        value_scdata = SCData(name=file_name)
                        setattr(value_scdata, file_type, data)
                        self.data[file_name] = value_scdata
            elif os.path.isfile(path) and path.endswith(".csv"):
                file_name = os.path.basename(path).split(".")[0]
                data = pd.read_csv(path, index_col=0)
                data.columns = [float(i) for i in data.columns]
                value_scdata = SCData(name=file_name)
                setattr(value_scdata, file_type, data)
                self.data[file_name] = value_scdata
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
        self.logger.info(f"Set Offset done.")
            
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
        self.logger.info("Cut range done.")
        
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
        self.logger.info(f"Combine mz rounded to res: {resolution}, Filter mz occurred < count: {count}.")
        if file_name is None:
            # self.__mp.run(self.data, _filter_occ, resolution, count)
            for ms_data in self.data.values():
                ms_data.process = filter_occ(ms_data.raw.copy(), resolution, count)
        else:
            self.data[file_name].process = filter_occ(
                self.data[file_name].raw.copy(), resolution, count
            )

    def gen_mat(self, file_name: str | None = None):
        self.logger.info("Convert data format to crosstab.")
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.mat = to_mat(data=ms_data.process, min_intensity=PARAMETERS.min_intensity)
        else:
            self.data[file_name].mat = to_mat(data=self.data[file_name].process, min_intensity=PARAMETERS.min_intensity)

    def round_mat(self, resolution_intensity: float = PARAMETERS.resolution_intensity, file_name: str | None = None):
        self.logger.info(f"Round intensity to res_int: {resolution_intensity}.")
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.mat = round_columns(ms_data.mat, resolution_intensity)
        else:
            self.data[file_name].mat = round_columns(
                self.data[file_name].mat, resolution_intensity
            )

    def denoise(self, max_ratio: float = PARAMETERS.maxratio, file_name: str | None = None):
        """
        Find cell and subtract noise.
        Args:
            max_ratio: If ref_mz_intensity > max_ratio * ref_mz_max_intensity, the cell will be extracted
            file_name: File name, if None, all files will be processed.
        """
        self.logger.info(f"Find cells: ref_mz: {self.ref_mz} > max_ratio: {max_ratio} * max intensity and subtract noise.")
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.cell_pos = find_cell(
                    ms_data.mat, self.ref_mz, max_ratio=max_ratio
                )
                ms_data.mat = noise_subtract(ms_data.mat, ms_data.cell_pos)
        else:
            ms_data = self.data[file_name]
            ms_data.cell_pos = find_cell(
                ms_data.mat, self.ref_mz, max_ratio=max_ratio
            )
            ms_data.mat = noise_subtract(ms_data.mat, ms_data.cell_pos)
            self.data[file_name] = ms_data

    def merge_cell(self, adjacent: int = PARAMETERS.adjacent, file_name: str | None = None):
        """
        Combine the adjacent cells
        Args:
            adjacent: Number of adjacent cells to be combined, default 3.
            If number is larger than the number of cells, the data will be dropped.
            file_name: File name, if None, all files will be processed.
        """
        self.logger.info(f"Merge cell < adjacent: {adjacent} scans.")
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

    def filter_assem(self, snr: float = PARAMETERS.snr, file_name: str | None = None):
        """
        Filter out the data with Intensity/Noise < snr
        Args:
            snr: Minimum SNR of the data, default 3.0
            file_name: File name, if None, all files will be processed.
        """
        self.logger.info(f"Filter data with SNR < {snr}.")
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
        self.logger.info(f"Filter mz occurred > threshold: {threshold} * cell_count.")
        if name_list is None:
            name_list = list(self.data.keys())
        mat_list = [self.data[name].cell_mat for name in name_list]
        total_mat = list(filter_mat(mat_list, threshold, lock_mz, method))
        for index, name in enumerate(name_list):
            self.data[name].cell_mat = total_mat[index]

    def normalize(self,
                  data: dict[str, SCData],
                  normalize_method: list[str],
                  file_name: str | None = None):
        self.logger.info(f"Normalize by {normalize_method}.")
        if file_name is None:
            for ms_data in data.values():
                ms_data.cell_mat = normalize(
                    ms_data.cell_mat, normalize_method, mz=self.ref_mz
                )
        else:
            data[file_name].cell_mat = normalize(
                data[file_name].cell_mat, normalize_method, mz=self.ref_mz
            )

    def fill(self,
             data: dict[str, SCData],
             file_name: str | None = None,
             fillna_method: str = "knn"
             ):
        self.logger.info(f"Fill NaN by {fillna_method}.")
        if file_name is None:
            for ms_data in data.values():
                ms_data.cell_mat = fill_mat(ms_data.cell_mat, fillna_method)
        else:
            data[file_name].cell_mat = fill_mat(
                data[file_name].cell_mat, fillna_method
            )

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
                self.logger.info(
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
            self.logger.info(
                " ".join(
                    [
                        f"File name: {ms_data.name}",
                        f"Cells count: {ms_data.cell_count}",
                        f"Peaks count: {ms_data.cell_mat.shape[1]}",
                    ]
                )
            )

    def clear_memory(self, file_name: str | None = None, attributes:list[str] | None = None):
        """
        Clear the memory of the MSProcess
        Args:
            file_name: File name, if None, all files will be processed.
        """
        if attributes is None:
            attributes_to_clear = ["mat"]
        else:
            attributes_to_clear = []
            for attr in attributes:
                if hasattr(self, attr):
                    attributes_to_clear.append(attr)
                else:
                    self.logger.warning(f"Attribute '{attr}' does not exist in SCData and will be skipped.")

        self.logger.info(f"Clear attributes: {', '.join(attributes_to_clear)}")
        if file_name is None:
            for value in self.data.values():
                value.clear(attributes=attributes_to_clear)
        else:
            self.data[file_name].clear(attributes=attributes_to_clear)

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
        self.logger.info(f"Convert data to {type} format and save in {path if path else self.__dir}.")
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
            self.logger.info("Specify the label.")
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

        self.logger.info(f"Save data in {dir_path}.")
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

    def pre_process(
        self,
        file_name: str | list[str] | None = None,
        offset: float | None = None,
        cut_range: tuple[int, int] | None = None,
        resolution: float = PARAMETERS.resolution,
        count: int = PARAMETERS.count,
        offset_method: str = "same",
        cut_method: str = "same",
        clear_mem: bool = False
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
            clear_mem: If True, clear the SCData.raw after pre_process, default False.
        Returns:

        """
        self.logger.info("Pre-process begin.")

        if offset is None and cut_range is None:
            self.logger.info("No offset or cut range.")
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
                
            self.logger.info(f"For files in {file_list}.")
            # do offset and cut
            if offset is not None: 
                self.offset_preprocess(file_list, offset, offset_method)
            if cut_range is not None:
                self.cut_preprocess(file_list, cut_range, cut_method)

        self.filter_occ(resolution=resolution, count=count)
        if clear_mem:
            self.clear_memory(attributes=["raw"])

    def process(
            self,
            max_ratio: float = PARAMETERS.maxratio,
            adjacent: int = PARAMETERS.adjacent,
            snr: float = PARAMETERS.snr,
            resolution_intensity: float = PARAMETERS.resolution_intensity,
            threshold: float = PARAMETERS.threshold,
            lock_mz: bool = PARAMETERS.lock,
            filter_method: str = "all",
            clear_mem: bool = False
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
            clear_mem: If True, clear the SCData.process and SCData.mat after process, default False(clear mat).
        """
        if not self.data:
            raise ValueError("No data loaded")
        self.logger.info("Process begin.")
        if not clear_mem:
            self.gen_mat()
            self.round_mat(resolution_intensity=resolution_intensity)
            self.denoise(max_ratio=max_ratio)
            self.merge_cell(adjacent=adjacent)
            self.filter_assem(snr=snr)
            self.filter_mat(threshold=threshold, lock_mz=lock_mz, method=filter_method)
            self.info()
            self.clear_memory()
        else:
            self.gen_mat()
            self.clear_memory(attributes=["process"])
            self.round_mat(resolution_intensity=resolution_intensity)
            self.denoise(max_ratio=max_ratio)
            self.merge_cell(adjacent=adjacent)
            self.filter_assem(snr=snr)
            self.clear_memory(attributes=["mat"])
            self.filter_mat(threshold=threshold, lock_mz=lock_mz, method=filter_method)
            self.info()

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
        self.logger.info("Post process begin.")
        if normalize_method is None:
            normalize_method = ["mz"]
        if data is None:
            data = self.data
        self.normalize(data=data, normalize_method=normalize_method)
        self.fill(data=data, fillna_method=fillna_method)
        # self.combat()
        return data
