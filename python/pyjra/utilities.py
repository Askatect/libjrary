from pyjap.logger import LOG

def dataframe_to_tabular(df, df_index: bool = False):
    from pandas import DataFrame
    df = DataFrame(df)
    return Tabular(
        data = [tuple(row) for row in df.to_records(index = df_index)],
        columns = df.columns.tolist(),
        datatypes = [type(item) for item in df.dtypes.tolist()]
    )

class Tabular():
    def __init__(
        self, 
        data: list[tuple], 
        columns: list[str] = None, 
        datatypes: list[type] = None
    ):
        if isinstance(data, list):
            data, columns, datatypes, error = self.__validata(data, columns, datatypes)
        if error is not None:
            LOG.error(error)
            raise ValueError(error)
        
        self.data = data
        self.columns = columns
        self.datatypes = datatypes
        return
    
    def __validata(
        self,
        data: list[tuple] = None, 
        columns: list[str] = None,
        datatypes: list[str] = None
    ):
        # Check columns come as a list of strings.
        if not(columns is None or (isinstance(columns, list) and all(isinstance(item, str) for item in columns))):
            return data, columns, datatypes, "The columns should be a list of strings."
        
        # Check datatypes come as a list of types.
        if not(datatypes is None or (isinstance(datatypes, list) and all(isinstance(item, type) for item in datatypes))):
            return data, columns, datatypes, "The data types should be a list of Python types."
        
        # Check amount of columns and datatypes agree.
        if not(columns is None or datatypes is None):
            if len(columns) != len(datatypes):
                return "The number of columns supplied did not match the number of datatypes supplied."
            
        # Check data is a list.
        if not isinstance(data, list):
            return data, columns, datatypes, "The `data` parameter should be a list."

        # Get number of columns.
        if columns is not None:
            self.col_count = len(columns)
        elif datatypes is not None:
            self.col_count = len(columns)
        else:
            self.col_count = len(data[0])
        
        # Get number of rows.
        self.row_count = len(data)

        # Check number of columns or build columns list.
        if columns is not None and len(columns) != self.col_count:
            return data, columns, datatypes, "Number of provided columns does not match the data."
        else:
            columns = [f"Column{c}" for c in range(self.col_count)]

        # Check number datatypes or build datatypes list.
        if datatype is not None:
            if len(datatypes) != self.col_count:
                return data, columns, datatypes, "Number of provided datatypes does not match the data."
        else:
            datatypes = [type(data[0][c]) for c in range(self.col_count)]

        # Check that all rows are tuples of the correct length.
        for row in data:
            if not isinstance(row, tuple):
                return data, columns, datatypes, "All rows of data should be tuples."
            if self.col_count != len(row):
                return data, columns, datatypes, "All rows of the data should be the same length and should be the length of the number of columns if supplied."
            
        # Check datatypes of columns.
        for c in range(self.col_count):
            datatype = datatypes[c]
            if not all(item is None or isinstance(item, datatype) for item in [data[r][c] for r in range(self.row_count)]):
                return data, columns, datatypes, f"Not all elements of column {c + 1} were of the same datatype or weren't the given datatype if datatypes was supplied."
            
        return data, columns, datatypes, None

def extract_param(string: str, prefix: str, suffix: str, case_insensitive_search: bool = True):
    if case_insensitive_search:
        search_string = string.lower()
        prefix = prefix.lower()
        suffix = suffix.lower()
    else:
        search_string = string
    prefix_loc = search_string.find(prefix)
    if prefix_loc == -1:
        return None
    prefix_loc += len(prefix)
    suffix_loc = search_string.find(suffix, prefix_loc)
    return string[prefix_loc:suffix_loc]

def validate_date(datestring: str, format: str = '%Y-%m-%d'):
    import datetime
    try:
        date = datetime.datetime.strptime(datestring, format)
    except:
        LOG.info(f'String "{datestring}" is not in the format "{format}".')
        return None
    else:
        return date

def rgb_to_hex(rgb: tuple):
    hexcode = "#"
    for value in rgb:
        hexcode += f"{value:02x}"
    return hexcode

def hex_to_rgb(hexcode: str):
    hexcode = hexcode.lstrip('#')
    l = len(hexcode)//3
    return tuple(int(hexcode[i:i + l], 16) for i in range(0, 3*l, l))

def hsl_to_rgb(hsl: tuple):
    h = hsl[0]
    l = hsl[2]
    c = (1 - abs(2*l - 1))*hsl[1]
    x = c*(1 - abs(((h/60.0) % 2) - 1))
    m = l - c/2
    if h < 60:
        rgb = (c, x, 0)
    elif h < 120:
        rgb = (x, c, 0)
    elif h < 180:
        rgb = (0, c, x)
    elif h < 240:
        rgb = (0, x, c)
    elif h < 300:
        rgb = (x, 0, c)
    elif h < 360:
        rgb = (c, 0, x)
    return tuple(int(255*(value + m)) for value in rgb)

def rgb_to_hsl(rgb: tuple):
    r = rgb[0]/255.0
    g = rgb[1]/255.0
    b = rgb[2]/255.0
    cmax = max(r, g, b)
    cmin = min(r, g, b)
    l = (cmax + cmin)/2
    delta = cmax - cmin
    if delta == 0:
        h = 0
        s = 0
    else:
        s = delta/(1 - abs(2*l - 1))
        if cmax == r:
            h = 60*((g - b)/delta % 6)
        elif cmax == g:
            h = 60*(2 + (b - r)/delta)
        elif cmax == b:
            h = 60*(4 + (r - g)/delta)
    return (int(h), s, l)

def hex_to_hsl(hexcode: str):
    return rgb_to_hsl(hex_to_rgb(hexcode))

def hsl_to_hex(hsl: tuple):
    return rgb_to_hex(hsl_to_rgb(hsl))

def linear_interpolation(x, xmin, xmax, ymin, ymax):
    if xmin == xmax:
        return ymin
    else:
        return ymin + (ymax - ymin)*(x - xmin)/(xmax - xmin)

def gradient_rgb(target, lower, upper, rgbmin, rgbmax):
    return tuple(int(linear_interpolation(target, lower, upper, rgbmin[i], rgbmax[i])) for i in range(0, 3))

def gradient_hex(target, lower, upper, hexmin, hexmax):
    return rgb_to_hex(gradient_rgb(target, lower, upper, hex_to_rgb(hexmin), hex_to_rgb(hexmax)))
