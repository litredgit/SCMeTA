import os

def sort_filename(data:dict, sort: str = "alphabetical") -> dict:
    """
    Sort loaded data 
    Args:
        data: dict with file_name keys to be sorted
        sort: sort key, default is "alphabetical"
    """
    if sort == "alphabetical":
        data = dict(sorted(data.items(), key=lambda x: x[0]))
    else:
        sorted_list = []
        try:
            sorted_list = sorted(data.keys(), key=lambda x: sort(x))
        except TypeError:
            sorted_list = sort.split(",")
        try:
            data = {name: data[name] for name in sorted_list}
        except KeyError as e:
            raise KeyError(f"KeyError: {e}. Please check the sort key")
    return data

def remove_fix():
    pass

def check_path(path: str, do: str = 'r', type: list = ['csv'], sort: str = "alphabetical") -> dict | str:
    """
    Check and handle the given path based on the operation mode (read/write).

    Args:
        path: The path to check.
        do: Operation mode, 'r' for read, 'w' for write.
        type: File type to filter for (used in 'r' mode).

    Returns:
        For 'r' mode: A dictionary with sorted filenames as keys and their absolute paths as values.
        For 'w' mode: The directory path where files can be written.
    """
    type_lower = [ext.lower() for ext in type]
    if do == 'r':
        if not os.path.exists(path):
            raise FileNotFoundError(f"The path '{path}' does not exist.")
        if os.path.isfile(path):
            if any(path.lower().endswith(f".{ext}") for ext in type_lower):
                files = {os.path.basename(path).split('.')[0]: os.path.abspath(path)}
            else:
                raise ValueError(f"The file '{path}' does not have one of the required extensions: {type}.")
        elif os.path.isdir(path):
            files = {
                os.path.basename(f).split('.')[0]: os.path.abspath(os.path.join(path, f))
                for f in os.listdir(path)
                if any(f.lower().endswith(f".{ext}") for ext in type_lower)
            }
        else:
            raise ValueError(f"The path '{path}' is neither a file nor a directory.")
        
        if len(files) == 0:
            raise ValueError(f"No files with extensions {type} found in the provided path.")
        # Sort files by filename alphabetically
        files = sort_filename(data=files, sort=sort)
        return files
    elif do == 'w':
        if os.path.isfile(path) and any(path.lower().endswith(f".{ext}") for ext in type_lower):
            return os.path.dirname(path)
        elif os.path.isdir(path):
            return path
        else:
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Failed to create directory '{path}': {e}")
            return path
    else:
        raise ValueError(f"Invalid operation mode '{do}'. Use 'r' for read or 'w' for write.")
