from pyjra.logger import LOG

from pandas import DataFrame

def justify_text(text: str, width: int = 64, tab_length: int = 4) -> str:
    """
    ### justify_text

    Version: 1.0
    Authors: JRA
    Date: 2024-02-15

    #### Explanation:
    Fits text into a column of a given width.
    
    #### Parameters:
    - text (str): The text to justify.
    - width (int): The width - in characters - to fit the text into. Defaults to 64 characters.
    - tab_length (int): The number of spaces each tab represents. Defaults to 4.

    #### Returns:
    - (str): Justified text.

    #### Usage:
    >>> justify_text(
            text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
            width = 32
        )
    '''
    Lorem ipsum dolor sit amet,
    consectetur adipiscing elit.
    Sed do eiusmod tempor
    incididunt ut labore et dolore
    magna aliqua. Ut enim ad minim
    veniam, quis nostrud
    exercitation ullamco laboris
    nisi ut aliquip ex ea commodo
    consequat.
    '''
    
    #### History:
    - 1.0 JRA (2024-20-15): Initial version.
    """
    justified = ""
    text = text.replace('\t', tab_length*' ')
    while len(text) > width:
        index = text.rfind('\n', 0, width)
        if index >= 0:
            justified += text[0:index] + '\n'
            text = text[index + 1:]
            continue
        index = text.rfind(' ', 0, width)
        if index >= 0:
            justified += text[0:index] + '\n'
            text = text[index + 1:]
            continue
        justified += text[0:width - 1] + '-\n'
        text = text[width - 1:]
    return justified + text

def align_text(text: str, alignment: str = 'centre', width: int = 64, tab_length: int = 4) -> str:
    """
    ### align_text

    Version: 1.0
    Authors: JRA
    Date: 2024-02-15

    #### Explanation:
    Aligns a given text (as a string) to a particular width and alignment.

    #### Requirements:
    - justify_text (func): Performs the initial justification to the given width.

    #### Parameters:
    - text (str): The text to align.
    - alignment (str): The choice of alignment. Defaults to centred.
        - 'justify': Fits the text to a column without padding (see the behaviour of `justify_text`).
        - 'centre': Adds padding to centre the text within the given width.
        - 'left': Left aligns the text and adds padding up the given width.
        - 'right': Adds padding to right align the text to the given width.
    - width (int): The width (in characters) to fit the text to. Defaults to 64 characters.
    - tab_length (int): The number of spaces each tab represents. Defaults to 4.
    
    #### Returns:
    - (str): Aligned text.

    #### Usage:
    >>> align_text(
            text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
        )
    '''
     Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do
    eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut
    enim ad minim veniam, quis nostrud exercitation ullamco laboris
                nisi ut aliquip ex ea commodo consequat.
    '''
    
    #### History:
    - 1.0 JRA (2024-02-15): Initial version.
    """
    text = justify_text(text, width, tab_length)
    if alignment == 'justify':
        return text
    aligned = ''
    for line in text.split('\n'):
        pad = width - len(line)
        if alignment == 'centre':
            left_pad = (pad // 2) + (pad % 2)
            right_pad = (pad // 2)
        elif alignment == 'left':
            left_pad = 0
            right_pad = pad
        elif alignment == 'right':
            left_pad = pad
            right_pad = 0
        else:
            error = f'Alignment "{alignment}" not recognised. Should be one of "justify", "centre", "left" or "right".'
            LOG.error(error)
            raise ValueError(error)
        aligned += left_pad*' ' + line + right_pad*' ' + '\n'
    return aligned[:-1]

def tabulate(table: list[tuple[str]], header: int = 1, null: str = '', name: str = None) -> str:
    """
    ### tabulate

    Version: 2.0
    Authors: JRA
    Date: 2024-02-15

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
            table = [('Dragon', 'Colour'), 
                ('Sapphira', 'Blue'), 
                ('Thorn', 'Red'), 
                ('Glaedr', 'Gold'), 
                ('Firnen', 'Green'), 
                ('Shruikan', 'Black')],
            name = 'Alagaesian Dragons'
        )
    ╔═══════════════════╗
    ║     Alagaesian    ║
    ║      Dragons      ║
    ╚═══════════════════╝
    ╒══════════╤════════╕
    │  Dragon  │ Colour │
    ╞══════════╪════════╡
    │ Sapphira │   Blue │
    ├──────────┼────────┤
    │    Thorn │    Red │
    ├──────────┼────────┤
    │   Glaedr │   Gold │
    ├──────────┼────────┤
    │   Firnen │  Green │
    ├──────────┼────────┤
    │ Shruikan │  Black │
    ╘══════════╧════════╛

    #### Tasklist:
    - Add a config dictionary?
        - Options to left/right justify rows/columns.
        - Maximum column width.
    - Style options.

    #### History:
    - 2.0 JRA (2024-02-15): Redesigned the border characters and utilised pyjra.utilities.align_text.
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
    col_count = len(widths)
    if name is None:
        tabular = ''
    else:
        width = sum(widths) + 3*col_count - 3
        name = align_text(text = name, alignment = 'centre', width = width).split('\n')
        tabular = '╔' + (width + 2)*'═' + '╗\n'
        for line in name:
            tabular += '║ ' + line + ' ║\n'
        tabular += '╚' + (width + 2)*'═' + '╝\n'
    for r in range(len(table)):
        if r == 0:
            tabular += '╒'
        elif r <= header:
            tabular += '╞'
        else:
            tabular += '├'
        for c in range(col_count):
            if r == 0:
                tabular += (widths[c] + 2)*'═' + ('╕\n' if c == col_count - 1 else '╤')
            elif r <= header:
                tabular += (widths[c] + 2)*'═' + ('╡\n' if c == col_count - 1 else '╪')
            else:
                tabular += (widths[c] + 2)*'─' + ('┤\n' if c == col_count - 1 else '┼')
        for c in range(col_count):
            try:
                value = table[r][c] or null
            except IndexError:
                value = null
            value = align_text(
                text = value, 
                alignment = 'centre' if r < header else 'right', 
                width = widths[c]
            )
            tabular += '│ ' + value + ' '
        tabular += '│' + '\n'
    tabular += '╘'
    for c in range(col_count):
        tabular += (widths[c] + 2)*'═' + ('' if c == col_count - 1 else '╧')
    return tabular + '╛'

def format_json(json: str) -> str:
    """
    ### format_json

    Version: 1.0
    Authors: JRA
    Date: 2024-02-16

    #### Explanation:
    Formats a JSON string into a pretty format.

    #### Parameters:
    - json (str): The JSON string to format.

    #### Returns:
    - json (str): The formatted JSON string.

    #### Usage:
    >>> format_json('{"name": "Hollow Knight", "age": 6, "platforms": ["Nintendo", "Xbox", "PlayStation", "PC"], "multiplayer": false, "dreamers": {"Lurien": "watcher", "Monomon": "teacher", "Herrah": "beast"}}')
    '''
    {
        "name": "Hollow Knight",
        "age": 6,
        "platforms": [
            "Nintendo",
            "Xbox",
            "PlayStation",
            "PC"
        ],
        "multiplayer": false,
        "dreamers": {
            "Lurien": "watcher",
            "Monomon": "teacher",
            "Herrah": "beast"
        }
    }
    '''

    #### History:
    - 1.0 JRA (2024-02-16): Initial version (based on pyjra.formatting.jsonformatterv2).
    """
    LOG.info("Cleaning JSON and identifying terms...")
    json = json.replace('\n', '').replace('\t', '')
    json_words = []
    word = ''
    string = False
    for char in json:
        if char == '"' and not string:
            string = True
            word += char
        elif string:
            word += char
            if char == '"':
                string = False
                json_words.append(word)
                word = ''
        elif char in ('{', '}', '[', ']', ':', ','):
            json_words.append(word)
            word = ''
            json_words.append(char)
        elif char == ' ':
            continue
        else:
            word += char

    LOG.info('Formatting the JSON...')
    json = ''
    levels = []
    value = False
    for word in json_words:
        if len(word) == 0:
            continue
        elif word in ('{', '['):
            if value:
                value = False
                json += word
            else:
                json += '\n' + len(levels)*'\t' + word
            if word == '{':
                levels.append('object')
            else:
                levels.append('array')
        elif word in ('}', ']'):
            value = False
            levels.pop()
            json += '\n' + len(levels)*'\t' + word
        elif word == ':':
            value = True
            json += ': '
        elif value or word == ',':
            value = False
            json += word
        else:
            json += '\n' + len(levels)*'\t' + word

    LOG.info('Returning formatted JSON.')
    return json

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
    from datetime import datetime
    try:
        date = datetime.strptime(datestring, format)
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
        self.__transpose(row_based = True)
        return f"Tabular(\n\tdata = {self.data},\n\tcolumns = {self.columns or 'None'},\n\tdatatypes = {[str(datatype) for datatype in self.datatypes] or 'None'}\n)"
    
    def __getitem__(self, key: int|list[int]|slice):
        if isinstance(key, int):
            return self.tabular_from_tabular(
                data = self.data[key],
                columns = self.columns,
                datatypes = self.datatypes,
                row_count = self.row_count,
                col_count = self.col_count,
                row_based = self.row_based
            )
        elif isinstance(key, slice):
            start = key.start
            stop = key.stop
            step = key.step
            return self.tabular_from_tabular(
                data = self.data[start:stop:step],
                columns = self.columns,
                datatypes = self.datatypes,
                row_count = self.row_count,
                col_count = self.col_count,
                row_based = self.row_based
            )
        elif (isinstance(key, list) and all(isinstance(item, int) for item in key)):
            output = []
            for pos in key:
                output.append(self.data[pos])
            return self.tabular_from_tabular(
                data = output,
                columns = self.columns,
                datatypes = self.datatypes,
                row_count = self.row_count,
                col_count = self.col_count,
                row_based = self.row_based
            )
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
    
    def __init_no_check(
        self, 
        data: list[tuple], 
        columns: list[str], 
        datatypes: list[type], 
        row_count: int, 
        col_count: int, 
        row_based: bool
    ):
        self.datatypes = datatypes
        self.columns = columns
        self.data = data
        self.row_count = row_count
        self.col_count = col_count
        self.row_based = row_based
        return
    
    @staticmethod
    def tabular_from_tabular(
        data: list[tuple], 
        columns: list[str], 
        datatypes: list[type], 
        row_count: int, 
        col_count: int, 
        row_based: bool
    ):
        output = Tabular.__new__(Tabular)
        output.__init_no_check(
            data,
            columns,
            datatypes, 
            row_count, 
            col_count, 
            row_based
        )
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
            row_based = not self.row_based
        self.row_based = row_based
        data = []
        if row_based:
            for r in range(self.row_count):
                data.append(tuple([self.data[c][r] for c in range(self.col_count)]))
        else:
            for c in range(self.col_count):
                data.append(tuple([self.data[r][c] for r in range(self.row_count)]))
        self.data = data
        return
    
    def to_dataframe(self) -> DataFrame:
        return DataFrame(data = self.data, columns = self.columns)
    
    def to_dict(self, row: str|int = 0) -> dict:
        if isinstance(row, str):
            row = self.col_pos(row)
        return dict(zip(self.columns, self.data[row]))
    
    def to_delimited(self, row_separator: str = '\n', col_separator: str = ',', header: bool = True) -> str:
        output = row_separator.join([col_separator.join([str(value) for value in row]) for row in self.data])
        if header:
            return col_separator.join(self.columns) + row_separator + output
        else:
            return output
        
    def to_html(
        self, 
        colours: dict = {
			'main': '#181848', 
			'positive': '#1b8c1b', 
			'null': '#8c8c1b', 
			'negative': '#8c1b1b', 
			'black': '#000000', 
			'grey': '#cfcfcf', 
			'white': '#ffffff', 
			'dark_accent': '#541b8c', 
			'light_accent': '#72abe3'
		}
    ):
        col_info = []
        for c in range(self.col_count):
            col_info.append({'numeric': False})
            if self.datatypes[c] not in (int, float):
                continue
            self.__transpose(row_based = False)
            col = [value for value in self.data[c] if value is not None]
            if len(col) == 0:
                continue
            column = self.columns[c]
            col_info[c]['numeric'] = True
            col_info[c]['min'] = min(col)
            col_info[c]['max'] = max(col)
            col_info[c]['signed'] = (col_info[c]['min'] < 0)
        self.__transpose(row_based = True)
        html = f'<table style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid {colours["black"]};border-collapse:collapse">\n'
        html += f'\t<tr style="color:{colours["white"]}">\n\t\t<th style="background-color:{colours["dark_accent"]};border:2px solid {colours["black"]}">{self.columns[0]}</th>\n'
        for column in self.columns[1:]:
            html += f'\t\t<th style="background-color:{colours["main"]};border:2px solid {colours["black"]}">{column}</th>\n'
        html += '\t</tr>\n'
        for r, row in enumerate(self.data):
            html += f'\t<tr>\n\t\t<td style="border:2px solid {colours["black"]};background-color:{colours["dark_accent"]};color:{colours["white"]}">{str(row[0])}</td>\n'
            for c in range(1, self.col_count):
                value = row[c]
                if col_info[c]['numeric'] and value is not None:
                    if col_info[c]['signed']:
                        fontcolour = colours['white']
                        if value < 0:
                            background = gradient_hex(value, col_info[c]['min'], 0, colours['negative'], colours['null'])
                        else:
                            background = gradient_hex(value, col_info[c]['max'], 0, colours["positive"], colours["null"])
                    else:
                        background = gradient_hex(value, col_info[c]['min'], col_info[c]['max'], colours["white"], colours["light_accent"])
                        fontcolour = colours["black"]
                else:
                    if r % 2 == 0:
                        background = colours["white"]
                    else:
                        background = colours["grey"]
                    fontcolour = colours["black"]
                html += f'\t\t<td style="border:1px solid {colours["black"]};background-color:{background};color:{fontcolour}">{str(value)}</td>\n'
            html += '\t</tr>\n'
        html += '</table>'
        return html
