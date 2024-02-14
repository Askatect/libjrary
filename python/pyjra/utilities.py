from pyjra.logger import LOG

from pandas import DataFrame

def tabulate(table: list[tuple[str]], header: int = 1, null: str = '', name: str = None) -> str:
    """
    ### tabulate

    Version: 1.1
    Authors: JRA
    Date: 2024-02-14

    #### Explanation:
    Converts a given table to a formatted text table as a string.

    #### Parameters:
    - table (list[list[str]]): The table to format as a list of rows, and each row a list of values.
    - header (int): The number of header rows for the table to have. Defaults to 1.
    - null (str): The default value to use for missing entries.

    #### Returns:
    - tabular (str): The formatted text table.

    #### Usage:
    >>> tabulate(
            [['Dragon', 'Colour'], 
            ['Sapphira', 'Blue'], 
            ['Thorn', 'Red'], 
            ['Glaedr', 'Gold'], 
            ['Firnen', 'Green'], 
            ['Shruikan', 'Black']],
            name = Alagaesian Dragons
        )
    #===================#
    |Alagaesian Dragons |
    #==========#========#
    |  Dragon  | Colour |
    #==========#========#
    | Sapphira | Blue   |
    +----------+--------+
    | Thorn    | Red    |
    +----------+--------+
    | Glaedr   | Gold   |
    +----------+--------+
    | Firnen   | Green  |
    +----------+--------+
    | Shruikan | Black  |
    +----------+--------+

    #### Tasklist:
    - Add a text columnising function for justified text (adding new lines and hyphenating on to new lines if needed).
    - Add a config dictionary?
    - Options to left/right justify rows/columns.
    - Style options.

    #### History:
    - 1.1 JRA (2024-02-14): Added name for titling tables.
    - 1.0 JRA (2024-02-02): Initial version.
    """
    widths = []
    r = len(table) - 1
    for r in range(len(table)):
        widths = widths + [0]*(max(0, len(table[r]) - len(widths)))
        table[r] = [str(value) for value in table[r]]
        for c in range(len(table[r])):
            widths[c] = max(widths[c], len(table[r][c] or null))
    tabular = ''
    for r in range(len(table)):
        for c in range(len(widths)):
            tabular += ('#' if r <= header else '+') + (widths[c] + 2)*('=' if r <= header else '-')
        tabular += ('#' if r <= header else '+') + '\n'
        for c in range(len(widths)):
            try:
                value = table[r][c] or null
            except IndexError:
                value = null
            pad = widths[c] + 1 - len(value)
            if r < header:
                left_pad = (pad // 2)
                right_pad = (pad // 2) + (pad % 2)
            else:
                left_pad = pad
                right_pad = 0
            tabular += '| ' + left_pad*' ' + value + right_pad*' '
        tabular += '|\n'
    for c in range(len(widths)):
        tabular += ('#' if r < header else '+') + (widths[c] + 2)*('=' if r < header else '-')
    tabular += ('#' if r < header else '+') + '\n'
    if name is not None:
        total_width = sum(widths) + 3*len(widths) - 1
        pad = total_width - len(name)
        left_pad = pad // 2
        right_pad = pad // 2 + pad % 2
        title = '#' + total_width*'=' + '#\n'
        title += '|' + left_pad*' ' + name + right_pad*' ' + '|\n'
        return title + tabular
    else:
        return tabular

class Tabular():
    def __init__(
        self, 
        data: list[tuple]|DataFrame|str|dict, 
        columns: list[str] = None, 
        datatypes: list[type] = None,
        row_separator: str = '\n',
        col_separator: str = ',',
        header: bool = True,
        name: str = None
    ):
        # Check columns come as a list of strings.
        if not(columns is None or (isinstance(columns, list) and all(isinstance(item, str) for item in columns))):
            error = "The columns should be a list of strings."
            LOG.error(error)
            raise ValueError(error)
        
        # Check datatypes come as a list of Python types.        
        if not(datatypes is None or (isinstance(datatypes, list) and all(isinstance(item, type) for item in datatypes))):
            error = "The data types should be a list of Python types."
            LOG.error(error)
            raise ValueError(error)

        # Check for same number of columns and datatypes.        
        if columns is not None and datatypes is not None and len(columns) != len(datatypes):
            error = "The number of columns supplied did not match the number of datatypes supplied."
            LOG.error(error)
            raise ValueError(error)
        
        # Check that the separators are strings.        
        if not (isinstance(row_separator, str) and isinstance(col_separator, str)):
            error = "The separators must be strings."
            LOG.error(error)
            raise ValueError(error)
        
        # Extract data from a delimited string.
        if isinstance(data, str):
            data = [tuple(row.split(col_separator)) for row in data.split(row_separator)]
            if header and columns is None:
                columns = list(data[0])
                data = data[1:]
            else:
                columns = columns or None
            self.datatypes = datatypes or len(columns)*[str]

        # Validate list of data rows.
        if isinstance(data, list):
            error = self.__validata(data, columns, datatypes)
            if error is not None:
                LOG.error(error)
                raise ValueError(error)
            
        # Extract data from DataFrame.
        elif isinstance(data, DataFrame):
            self.row_count, self.col_count = data.shape
            self.datatypes = datatypes or [type(item) for item in data.columns.tolist()]
            self.columns = data.columns.tolist()
            self.data = [tuple(row) for row in data.to_records(index = False)]

        # Extract data from dictionary.
        elif isinstance(data, dict):
            self.row_count = 1
            self.col_count = len(data)
            self.datatypes = [type(value) for value in data.values()]
            self.columns = data.keys()
            self.data = [tuple(data.values())]

        self.name = name or 'Tabular'
        self.row_based = True
        
        # Validate datatypes.
        if not(datatypes is None and (isinstance(data, DataFrame) or isinstance(data, dict))):
            self.__transpose(row_based = False)
            for c in range(self.col_count):
                datatype = self.datatypes[c]
                if all((value is None) or isinstance(value, datatype) for value in self.data[c]):
                    continue
                try:
                    col = []
                    for value in self.data[c]:
                        if value is None:
                            col.append(None)
                        else:
                            col.append(datatype(value))
                    self.data[c] = col
                except ValueError as e:
                    error = f"Could not convert column {c + 1} to {datatype}. {e}"
                    LOG.error(error)
                    raise ValueError(e)

        # Ensure row-based storage.
        self.__transpose(row_based = True)
        return
    
    def __str__(self) -> str:
        self.__transpose(row_based = True)
        return tabulate(table = [tuple(self.columns)] + self.data, header = 1, name = self.name)
    
    def __repr__(self) -> str:
        return f"""Tabular(data = {self.data},
    columns = {self.columns or 'None'},
    datatypes = {[str(datatype) for datatype in self.datatypes] or 'None'}
)"""
    
    def __getitem__(self, key: str|int|list[str]|list[int]|slice) -> list[tuple]|tuple:
        if isinstance(key, str):
            key = self.col_pos(key)
        elif (isinstance(key, list) and all(isinstance(item, str) for item in key)):
            key = [self.col_pos(item) for item in key]
        if isinstance(key, int):
            return self.data[key]
        elif isinstance(key, slice):
            start = key.start
            stop = key.stop
            step = key.step
            return self.data[start:stop:step]
        elif (isinstance(key, list) and all(isinstance(item, int) for item in key)):
            output = []
            for pos in key:
                output.append(self.data[pos])
            return output
        else:
            error = f"Invalid key passed to {self}."
            LOG.error(error)
            raise ValueError(error)
                
    def __validata(
        self,
        data: list[tuple] = None, 
        columns: list[str] = None,
        datatypes: list[str] = None
    ):
        # Check data is a list.
        if not isinstance(data, list):
            return "The `data` parameter should be a list."

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
            return "Number of provided columns does not match the data."
        else:
            columns = columns or [f"Column{c + 1}" for c in range(self.col_count)]

        # Check that all rows are tuples of the correct length.
        for row in data:
            if not isinstance(row, tuple):
                return "All rows of data should be tuples."
            if self.col_count != len(row):
                return "All rows of the data should be the same length and should be the length of the number of columns if supplied."
            
        # Check number datatypes or build datatypes list.
        if datatypes is not None:
            if len(datatypes) != self.col_count:
                return "Number of provided datatypes does not match the data."
        else:
            datatypes = []
            for c in range(self.col_count):
                for r in range(self.row_count):
                    datatype = type(None)
                    if data[r][c] is not None:
                        datatype = type(data[r][c])
                        break
                datatypes.append(datatype)

        self.datatypes = datatypes
        self.columns = columns
        self.data = data            
        return None
    
    def __init_no_check():
        return
    
    @staticmethod
    def tabular_from_tabular():
        output = Tabular.__new__(Tabular)
        output.__init_no_check()
        return output

    def col_pos(self, col: str) -> int:
        if col in self.columns:
            return self.columns.index(col)
        else:
            error = f'Column "{col}" not found.'
            LOG.error(error)
            raise ValueError(error)
    
    def __transpose(self, row_based: bool = None):
        if row_based is not None:
            if self.row_based == row_based:
                return
            else:
                self.row_based = row_based
        data = []
        if row_based:
            for c in range(self.col_count):
                data.append(tuple([self.data[r][c] for r in range(self.row_count)]))
        else:
            for r in range(self.row_count):
                data.append(tuple([self.data[c][r] for c in range(self.col_count)]))
        self.data = data
        return
    
    def to_dataframe(self) -> DataFrame:
        return DataFrame(data = self.data, columns = self.columns)
    
    def to_dict(self, row: str|int = 1) -> dict:
        if isinstance(row, str):
            row = self.col_pos(row)
        return dict(zip(self.columns, self.data[row]))
    
    def to_delimited(self, row_separator: str = '\n', col_separator: str = ',', header: bool = True) -> str:
        output = row_separator.join([col_separator.join([str(value) for value in row]) for row in self.data])
        if header:
            return col_separator.join(self.columns) + row_separator + output
        else:
            return output
    
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