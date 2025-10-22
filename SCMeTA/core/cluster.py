import os
import pandas as pd
from functools import wraps
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
    convert_format,
    combine_peaks,
)
from SCMeTA.batch import combat_batch_correction
from SCMeTA.method.fill import fill_mat
from SCMeTA.file import load_data, load_from_database
from SCMeTA.config import setup_config, setup_logger
from SCMeTA.accelerate import MultiProcessing
from SCMeTA.file.format import SCData
from SCMeTA.tool import check_path

def use_default_param(param_mapping: list[str]):
    """ A decorator to use instance variables as default values for function parameters."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Fill in missing parameters from instance attributes
            for param in param_mapping:
                if param not in kwargs and param not in args:
                    kwargs[param] = self.__getattribute__("PARAMETERS")[param]
            return func(self, *args, **kwargs)
        return wrapper
    return decorator

class Process:
    def __init__(
        self, ref_mz: float = 760.58, config_path: str | None = None, renew: bool = False, 
        mz1: float = 760.58, mz2: float = 791.34, mz3: float = 732.55
    ):
        self.PARAMETERS, self.INCLUDE_LIST, self.EXCLUDE_LIST = setup_config(config_path=config_path, renew=renew)
        self.data: dict[str, SCData] = {}

        self.__mz1: float = mz1
        self.__mz2: float = mz2
        self.__mz3: float = mz3
        self.ref_mz: float = ref_mz
        self.__dir: str | None=None
        self.__mp = MultiProcessing()

    def load(
        self,
        path: str,
        log_file: str = '',
        data_type: str = "thermo",
        target_attr: str = "raw",
        load_range: tuple[int, int] | None = None,
        method: str = "MultiThread"
    ):
        """
        Add a file to the MSProcess
        Args:
            path: File path or directory path.
            data_type: Data type, support:
                "thermo": [".raw", ".txt"],
                "process": [".csv"],
                "mzML": [".mzML", ".mzml"].
            target_attr: Attribute to load the data, default "raw", other attributes in SCData is also supported.
            method: Method to load the data, default "MultiThread", can be "one by one", "MultiProcess".
        """
        # set up logger
        if log_file == 'data':
            log_file=os.path.dirname(path) + "/scmeta.log"
        self.logger = setup_logger(log_file=log_file)
        # check data_type
        if data_type not in ["thermo", "process", "waters", "mzML", "database"]:
            raise ValueError("Data type not supported.")
        if target_attr not in ["cell_mat", "mat", "process", "raw"]:
            target_attr = "raw"
            self.logger.warning(f"target_attr {target_attr} not supported and will be redirected to \"raw\", choose from (\"raw\", \"process\", \"mat\", \"cell_mat\")")
        # load data
        self.logger.info(f"Load files in path {[path]}.")
        self.data.update(load_data(path, data_type, target_attr, load_range, method))
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

    def cut(self, file_name: str | None = None, ranges: list[list[int, int]] | list[int, int] | None = None, type: str = "cut"):
        """
        Cut or drop scan ranges from one or more files' data.
        Args:
            file_name: Single file name, or None for all files.
            ranges: Cut/drop ranges to apply. If None, nothing will be done.
            type: "cut" to keep ranges, "drop" to remove ranges.
        """
        if ranges is None:
            self.logger.info("No cut ranges provided.")
            return

        if file_name is None:
            for file in self.data.keys():
                FUNC = {
                    "cut": self.data[file].cut,
                    "drop": self.data[file].drop,
                }
                FUNC[type](ranges=ranges)
        else:
            file = file_name
            FUNC = {
                "cut": self.data[file].cut,
                "drop": self.data[file].drop,
            }
            FUNC[type](ranges=ranges)

        self.logger.info(f"{type} ranges {ranges}.")

    def offset(self, file_name: str | None = None, offset: float | None = None):
        """
        Apply an m/z offset to one or more files' data.
        Args:
            file_name: Single file name, or None for all files.
            offset: Offset value to apply. If None, nothing will be done.
        """
        if offset is None:
            self.logger.info("No offset provided.")
            return

        if file_name is None:
            for file in self.data.keys():
                self.data[file].set_offset(offset)
        else:
            self.data[file_name].set_offset(offset)

        self.logger.info(f"Offset {offset}.")

    @use_default_param(["res_mz"])
    def round_mz(self, res_mz: float):
        """
        Round the mz to the specified resolution and combine the peaks
        Args:
            res_mz: Resolution of the mz, default 0.01
        """
        self.logger.info(f"Combine mz rounded to res_mz: {res_mz}.")
        for ms_data in self.data.values():
            ms_data.raw = combine_peaks(ms_data.raw, res_mz)

    @use_default_param(["count"])
    def filter_occ(
        self,
        count: int,
        file_name: str | None = None,
    ):
        """
        Filter out the data with low occurrence
        Args:
            count: Minimum occurrence of the data, default 10
            file_name: File name, if None, all files will be processed.
        """
        self.logger.info(f"Filter mz occurred < count: {count}.")
        if file_name is None:
            # self.__mp.run(self.data, _filter_occ, resolution, count)
            for ms_data in self.data.values():
                ms_data.process = filter_occ(ms_data.raw.copy(), count)
        else:
            self.data[file_name].process = filter_occ(
                self.data[file_name].raw.copy(), count
            )

    @use_default_param(["min_intens"])
    def gen_mat(self, min_intens: float, file_name: str | None = None):
        self.logger.info(f"Convert data format to crosstab with min_intens {min_intens}.")
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.mat = to_mat(data=ms_data.process, min_intens=min_intens)
        else:
            self.data[file_name].mat = to_mat(data=self.data[file_name].process, min_intens=min_intens)
    
    @use_default_param(["res_intens"])
    def round_mat(self, res_intens: float, file_name: str | None = None):
        self.logger.info(f"Round intensity to res_intens: {res_intens}.")
        if file_name is None:
            for ms_data in self.data.values():
                ms_data.mat = round_columns(ms_data.mat, res_intens)
        else:
            self.data[file_name].mat = round_columns(
                self.data[file_name].mat, res_intens
            )

    @use_default_param(["mz_interval"])
    def combine_peaks(self, mz_interval: float, file_name: str | None = None):
        """
        Combine the columns which the difference is less than mz_interval
        Args:
            mz_interval: Minimum difference of m/z to be combined, default 0.01
            file_name: File name, if None, all files will be processed.
        """
        if mz_interval > 0:
            self.logger.info(f"Combine peaks with mz interval < {mz_interval}.")
            if file_name is None:
                for ms_data in self.data.values():
                    ms_data.mat = combine_peaks(ms_data.mat, mz_interval=mz_interval)
            else:
                self.data[file_name].mat = combine_peaks(
                    self.data[file_name].mat, mz_interval=mz_interval
                )
        else:
            self.logger.info(f"mz_interval <= 0, skip combine peaks step.")

    @use_default_param(["max_ratio"])
    def denoise(self, max_ratio: float, file_name: str | None = None):
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

    @use_default_param(["adjacent"])
    def merge_cell(self, adjacent: int, file_name: str | None = None):
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

    @use_default_param(["snr"])
    def filter_assem(self, snr: float, file_name: str | None = None):
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

    @use_default_param(["threshold", "lock_mz"])
    def filter_mat(
        self,
        threshold: float,
        lock_mz: bool,
        name_list: list[str] | None = None,
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
        self.logger.info(f"Filter mz occurred > threshold: {threshold} * cell_count with lock_mz {lock_mz}.")
        if lock_mz and (self.INCLUDE_LIST is None or self.EXCLUDE_LIST is None):
            raise ValueError("INCLUDE_LIST or EXCLUDE_LIST is None, cannot lock m/z.")
        if name_list is None:
            name_list = list(self.data.keys())
        mat_list = [self.data[name].cell_mat for name in name_list]
        total_mat = list(filter_mat(
            mat_list=mat_list, threshold=threshold, lock=lock_mz, method=method,
            INCLUDE_LIST=self.INCLUDE_LIST, EXCLUDE_LIST=self.EXCLUDE_LIST
        ))
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
            
    def show(self):
        pass

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
                if attr in ["raw", "process", "mat", "cell_mat"]:
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
            type: Format type, default "MetaboAnalyst", other types is not supported yet.
            specify_label: If True, specify the label for each file, default False.

        Returns: save a .csv file in the path or self.__dir
        """
        indict = {}
        outdf = pd.DataFrame()
        # Load data from path if path is not None
        if path is not None:
            self.load(path=path, data_type="process", target_attr=data_type)
        self.logger.info(f"Convert data to {type} format and save in {path if path else self.__dir}.")
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
            os.makedirs(dir_path, exist_ok=True)
        outdf.to_csv(os.path.join(dir_path, f"{type}.csv"))


    def save(self, file_name: str | None = None, suffix: str | None = None, attr: str = "cell_mat", path: str | None = None):
        """
        Save the MSProcess
        Args:
            file_name: File name, if None, all files will be processed.
            suffix: Suffix to add to the file name, default None.
            attr: Target save attr in SCData, default "cell_mat", other types saved in SCData is also supported.
            path: Path to save the file, default None,
             which means the file will be saved in the same directory as the data you loaded.
        """
        if path is None:
            path = os.path.join(self.__dir, "Process")
        dir_path = check_path(path, do='w')
        self.logger.info(f"Save data in {dir_path}.")

        if attr not in ["cell_mat", "mat", "process", "raw"]:
            raise ValueError("Data type not supported.")
        if suffix is not None:
            if file_name is None:
                for ms_data in self.data.values():
                    ms_data.__getattribute__(attr).to_csv(
                        os.path.join(dir_path, f"{ms_data.name}_{attr}.csv")
                    )
            else:
                self.data[file_name].__getattribute__(attr).to_csv(
                    os.path.join(dir_path, f"{file_name}_{attr}.csv")
                )
        else:
            if file_name is None:
                for ms_data in self.data.values():
                    ms_data.__getattribute__(attr).to_csv(
                        os.path.join(dir_path, f"{ms_data.name}.csv")
                    )
            else:
                self.data[file_name].__getattribute__(attr).to_csv(
                    os.path.join(dir_path, f"{file_name}.csv")
                )

    @use_default_param(["res_mz", "count"])
    def pre_process(
        self,
        res_mz: float,
        count: int,
        file_name: str | list[str] | None = None,
        offset: float | None = None,
        ranges: list[list[int, int]] | list[int, int] | None = None,
        type: str = "cut",
        clear_mem: bool = False
        ):
        """
        Pre-process the data, including peak_combine, offset, cut, and round.
        Args:
            count: Minimum occurrence of the data, default 10.
            res_mz: Resolution of the mz, default 0.01.
            mz_interval: Minimum difference of m/z to be combined, default 0.01
            file_name: File name
            offset: Offset of the data, if None, it will ignore the offset step.
            ranges: Cut range of the data, if None, it will ignore the cut step.
            type: Type of cut, "cut" or "drop", default "cut".
            clear_mem: If True, clear the SCData.raw after pre_process, default False.
        Returns:

        """
        self.logger.info("Pre-process begin.")
        self.cut(file_name=file_name, ranges=ranges, type=type)
        self.offset(file_name=file_name, offset=offset)
        self.round_mz(res_mz=res_mz)
        self.filter_occ(count=count)
        if clear_mem:
            self.clear_memory(attributes=["raw"])

    @use_default_param([
        "min_intens",
        "res_intens",
        "mz_interval",
        "max_ratio",
        "adjacent",
        "snr",
        "threshold",
        "lock_mz"
    ])
    def process(
            self,
            min_intens: float,
            res_intens: float,
            mz_interval: float,
            max_ratio: float,
            adjacent: int,
            snr: float,
            threshold: float,
            lock_mz: bool,
            filter_method: str = "all",
            clear_mem: bool = False
    ):
        """
        Args:
            min_intens: Minimum intensity for generating the matrix
            res_intens: Resolution of the mass intensity
            max_ratio: If ref_mz_intensity > max_ratio * ref_mz_max_intensity, the cell will be extracted
            adjacent: The number of adjacent cells to be combined
            snr: Signal to noise ratio
            threshold: Threshold of the data
            lock_mz: If True, the mz you select in lock mz file will be locked
            filter_method: Method of filtering
            clear_mem: If True, clear the SCData.process and SCData.mat after process, default False(clear mat).
        """
        self.logger.info("Process begin.")
        self.gen_mat(min_intens=min_intens)
        if clear_mem:
            self.clear_memory(attributes=["process"])
        self.round_mat(res_intens=res_intens)
        self.combine_peaks(mz_interval=mz_interval)
        self.denoise(max_ratio=max_ratio)
        self.merge_cell(adjacent=adjacent)
        self.filter_assem(snr=snr)
        self.clear_memory()
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