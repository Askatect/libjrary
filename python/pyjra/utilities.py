"""
# pyjra.utilities

Version: 1.0
Authors: JRA
Date: 2024-03-05

#### Explanation:
Useful Python utility items.

#### Requirements:
- pyjra.logger.LOG (const)
- pandas.DataFrame (class)
- io.StringIO (class)
- csv.reader (func)

#### Artefacts:
- justify_text (func): Fits text into a column of a given width.
- align_text (func): Aligns a given text (as a string) to a particular width and alignment.
- tabulate (func): Converts a given table to a formatted text table as a string.
- format_json (func): Formats a JSON string into a pretty format.
- extract_param (func): Extracts a parameter from a string, particularly for connection strings.
- validate_date (func): Checks a string to see if it contains valid datetime components.
- rgb_to_hex (func): Converts RGB to a hexcode.
- hex_to_rgb (func): Converts a hexcode to RGB.
- hsl_to_rgb (func): Converts HSL to RGB.
- rgb_to_hsl (func): Converts RGB to HSL.
- hex_to_hsl (func): Converts hexcodes to HSL.
- hsl_to_hex (func): Converts HSL to hexcode.
- linear_interpolation (func): Performs a linear interpolation.
- gradient_rgb (func): Finds colour on a linear gradient as an RGB value.
- gradient_hex (func): Finds colour on a linear gradient as a hexcode.
- Tabular (class): Class for handling tabulated data.

#### Usage:
>>> import pyjra.utilities
>>> from pyjra.utilities import Tabular

#### History:
- 1.0 JRA (2024-03-05): Initial version.
"""
from pyjra.logger import LOG

from pandas import DataFrame
from io import StringIO
from csv import reader

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

    #### Requirements:
    - align_text (func)

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
    """
    ### extract_param

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Extracts a parameter from a string, particularly for connection strings.

    #### Parameters:
    - string (str): The string to extract from.
    - prefix (str): Substring indicating the start of the parameter.
    - suffix (str): Substring indicating the end of the parameter.
    - case_insensitive_search (bool): If true, the case of all involved strings is ignored. Defaults to true.

    #### Returns:
    - None if prefix not found else the parameter substring.

    #### Usage:
    >>> extract_param('server=server_name;database=db_name;encrypt=yes', 'database=', ';', True)
    'db_name'
    
    #### Tasklist:
    - Make this more like [jra].[ufn_extract_param] by adding an option to include the prefix and/or suffix.

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
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
    """
    ### validate_date

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Checks a string to see if it contains valid datetime components.

    #### Requirements:
    - datetime.datetime

    #### Parameters:
    - datestring (str): The string to validate.
    - format (str): The format to validate against, using Python datetime codes.

    #### Returns:
    - None if date is invalid else date (datetime) of datetime components found.

    #### Usage:
    >>> validate_date('Birthday: 03/10/1999', 'Birthday: %d/%m/%Y')
    datetime.datetime(1999, 10, 3, 0, 0)

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    from datetime import datetime
    try:
        date = datetime.strptime(datestring, format)
    except:
        LOG.info(f'String "{datestring}" is not in the format "{format}".')
        return None
    else:
        return date

def rgb_to_hex(rgb: tuple[int]):
    """
    ### rgb_to_hex

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Converts RGB to a hexcode.

    #### Parameters:
    - rgb (tuple[int]): RGB values.

    #### Returns:
    - hexcode (str)

    #### Usage:
    >>> rgb_to_hex((24, 72, 72))
    '#184848'

    #### Tasklist:
    - Add validation to ensure that RGB values are integers in [0, 256).

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    hexcode = "#"
    for value in rgb:
        hexcode += f"{value:02x}"
    return hexcode

def hex_to_rgb(hexcode: str):
    """
    ### hex_to_rgb

    Version: 1.0
    Authors: JRA
    Date: 2024-05-03

    #### Explanation:
    Converts a hexcode to RGB.

    #### Parameters:
    - hexcode (str): The hexcode to convert.

    #### Returns:
    - (tuple[int])

    #### Usage:
    >>> hex_to_rgb('#184848')
    (24, 72, 72)
    >>> hex_to_rgb('#FFF')
    (255, 255, 255)

    #### Tasklist: 
    - Add validation to ensure an actual hexcode is received.

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    hexcode = hexcode.lstrip('#')
    l = len(hexcode)//3
    return tuple(int(hexcode[i:i + l], 16) for i in range(0, 3*l, l))

def hsl_to_rgb(hsl: tuple):
    """
    ### hsl_to_rgb

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Converts HSL to RGB.

    #### Parameters:
    - hsl (tuple): The HSL code, given as the following:
        - (int): Hue in degrees.
        - (float): Saturation in [0, 1].
        - (float): Lightness in [0, 1].
    
    #### Returns:
    - (tuple[int])

    #### Usage:
    >>> hsl_to_rgb((180, .5, .19))
    (24, 72, 72)

    #### Tasklist:
    - Ensure valid HSL is received.

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
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

def rgb_to_hsl(rgb: tuple[int]):
    """
    ### rgb_to_hsl

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Converts RGB to HSL.

    #### Parameters:
    - rgb (tuple[int]): The RGB value to convert.

    #### Returns:
    - (tuple): The HSL code, given as the following:
        - (int): Hue in degrees.
        - (float): Saturation in [0, 1].
        - (float): Lightness in [0, 1].
    
    #### Usage:
    >>> rgb_to_hsl((24, 72, 72))
    (180, .5, .19)

    #### Tasklist:
    - Ensure RGB is received.

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
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
    """
    ### hex_to_hsl

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Converts hexcodes to HSL.

    #### Requirements:
    - rbg_to_hsl (func)
    - hec_to_rgb (func)

    #### Parameters:
    - hexcode (str): The hexcode to convert.

    #### Returns:
    - (tuple): The HSL code, given as the following:
        - (int): Hue in degrees.
        - (float): Saturation in [0, 1].
        - (float): Lightness in [0, 1].

    #### Usage:
    >>> hex_to_hsl('#184848')
    (180, .5, .19)

    #### History:
    - 1.0 JRA (2024-05-03): Initial version.
    """
    return rgb_to_hsl(hex_to_rgb(hexcode))

def hsl_to_hex(hsl: tuple):
    """
    ### hsl_to_hex

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Converts HSL to hexcode.

    #### Requirements:
    - rgb_to_hex (func)
    - hsl_to_rgb (func)

    #### Parameters:
    - hsl (tuple): The HSL code, given as the following:
        - (int): Hue in degrees.
        - (float): Saturation in [0, 1].
        - (float): Lightness in [0, 1].

    #### Returns:
    - (str)

    #### Usage:
    >>> hsl_to_hex((180, .5, .19))
    '#184848'

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    return rgb_to_hex(hsl_to_rgb(hsl))

def linear_interpolation(
    x: int|float, 
    xmin: int|float, 
    xmax: int|float, 
    ymin: int|float, 
    ymax: int|float
):
    """
    ### linear_interpolation

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Performs a linear interpolation.

    #### Parameters:
    - x (int|float): The x value of the domain.
    - xmin (int|float): The lower bound of the domain.
    - xmax (int|float): The upper bound of the domain.
    - ymin (int|float): The lower bound of the range.
    - ymax (int|float): The upper bound of the range.

    #### Returns:
    - (float): The interpolated value in the range. The lower bound of the range is returned if the domain has measure zero.

    #### Usage:
    >>> linear_interpolation(1, 0, 2, -10, 10)
    0.

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    if xmin == xmax:
        return ymin
    else:
        return ymin + (ymax - ymin)*(x - xmin)/(xmax - xmin)

def gradient_rgb(
    target: int|float, 
    lower: int|float, 
    upper: int|float, 
    rgbmin: tuple[int], 
    rgbmax: tuple[int]
):
    """
    ### gradient_rgb

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Finds colour on a linear gradient as an RGB value.

    #### Requirements:
    - linear_interpolation (func)

    #### Parameters:
    - target (int|float): The value to map into the linear gradient.
    - lower (int|float): The lower bound of the domain.
    - upper (int|float): The upper bound of the domain.
    - rgbmin (tuple[int]): The start of the gradient as RGB.
    - rgbmax (tuple[int]): The end of the gradient as RGB.

    #### Returns:
    - (tuple[int])

    #### Usage:
    >>> gradient_rgb(1, 0, 2, (0, 0, 0), (24, 72, 72))
    (12, 36, 36)

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    return tuple(int(linear_interpolation(target, lower, upper, rgbmin[i], rgbmax[i])) for i in range(0, 3))

def gradient_hex(
    target: int|float, 
    lower: int|float, 
    upper: int|float, 
    hexmin: int|float, 
    hexmax: int|float
):
    """
    ### gradient_hex

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Finds colour on a linear gradient as a hexcode.
    
    #### Requirements:
    - rgb_to_hex (func)
    - gradient_rgb (func)
    - hex_to_rgb (func)

    #### Parameters:
    - target (int|float): The value to map into the linear gradient.
    - lower (int|float): The lower bound of the domain.
    - upper (int|float): The upper bound of the domain.
    - hexmin (tuple[int]): The start of the gradient as a hexcode.
    - hexmax (tuple[int]): The end of the gradient as a hexcode.

    #### Returns:
    - (str)

    #### Usage:
    >>> gradient_hex(1, 0, 2, '#000000', '#184848')
    '#0C2424'

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    return rgb_to_hex(gradient_rgb(target, lower, upper, hex_to_rgb(hexmin), hex_to_rgb(hexmax)))

class Tabular():
    """
    ## Tabular

    Version: 1.0
    Authors: JRA
    Date: 2024-03-05

    #### Explanation:
    Class for handling tabulated data.

    #### Artefacts:
    - data (list[tuple]): The data stored in the Tabular.
    - columns (list[str]): The columns of the Tabular.
    - datatypes (list[type]): The datatypes of the Tabular.
    - row_count(int): The number of rows in the Tabular.
    - col_count (int): The number of columns in the Tabular.
    - row_based (bool): If true, this indicates that the data is stored as a list of rows. If false, the data is stored as a list of columns.
    - name (str): The name to associate with the Tabular (optional). Defaults to None.
    - __init__ (func): Initialises the Tabular class.
    - __str__ (func): Writes the data to a pretty text table.
    - __repr__ (func): Displays an input that would yield the instance.
    - __getitem__ (func): Allows use of indexes and slices to produce a new Tabular from a subset of the data of the instance.
    - __validata (func): The validation process for raw data that checks the following.
        - Check data is a list.
        - Get number of columns.
        - Get number of rows.
        - Check number of columns or build columns list.
        - Check that all rows are tuples of the correct length.
        - Check number datatypes or build datatypes list.
    - __init_no_check (func): Initialises a Tabular instance without performing validation checks.
    - tabular_from_tabular (func): Creates a new Tabular object from the existing instance without performing validation checks.
    - transpose (func): Tranposes the storage of the data between a list of rows and a list of columns.
    - col_pos (func): Returns the column number of a given column name.
    - delete_columns (func): Delete columns from the current Tabular.
    - get_column (func): Retrieves a column from the data.
    - to_dataframe (func): Converts the Tabular to a pandas DataFrame.
    - to_dict (func): Returns a row of the data, with columns as keys and cells as values.
    - to_delimited (func): Returns the Tabular as a delimited string.
    - to_stream (func): Returns the Tabular as a delimited string stream.
    - to_html (func): Returns the Tabular as a HTML table.

    #### Usage:
    >>> matrix = Tabular(
            data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
            columns = ['v1', 'v2', 'v3']
            name = 'Matrix'
        )
    >>> matrix.transpose()
    >>> str(matrix)
    '''
    ╔══════════════╗
    ║    Matrix    ║
    ╚══════════════╝
    ╒════╤════╤════╕
    │ v1 │ v2 │ v3 │
    ╞════╪════╪════╡
    │  1 │  4 │  7 │
    ├────┼────┼────┤
    │  2 │  5 │  8 │
    ├────┼────┼────┤
    │  3 │  6 │  9 │
    ╘════╧════╧════╛
    '''
    >>> matrix.datatypes
    [<class 'int'>, <class 'int'>, <class 'int'>]

    #### History:
    - 1.0 JRA (2024-03-05): Initial version.
    """
    def __init__(
        self, 
        data: list[tuple]|DataFrame|str|StringIO|dict, 
        columns: list[str] = None, 
        datatypes: list[type] = None,
        row_separator: str = '\n',
        col_separator: str = ',',
        header: bool = True,
        name: str = None
    ):
        """
        ### __init__

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Initialises the Tabular class.

        #### Requirements:
        - Tabular.__validata (func)
        - Tabular.transpose (func)

        #### Parameters:
        - data (list[tuple]): The data stored in the Tabular.
        - columns (list[str]): The columns of the Tabular.
        - datatypes (list[type]): The datatypes of the Tabular.
        - row_count(int): The number of rows in the Tabular.
        - col_count (int): The number of columns in the Tabular.
        - row_based (bool): If true, this indicates that the data is stored as a list of rows. If false, the data is stored as a list of columns.
        - name (str): The name to associate with the Tabular (optional). Defaults to None.

        #### Usage:
        >>> Tabular(
                data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)],
                columns = ['v1', 'v2', 'v3'],
                name = 'Matrix'
            )

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
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
        
        # Extract data from a delimited string or stream.
        if isinstance(data, str) or isinstance(data, StringIO):
            from csv import reader
            if isinstance(data, str):
                data = StringIO(data)
            data = [tuple(row) for row in reader(data)]
            for r in range(len(data)):
                data[r] = tuple(None if value == "" else value for value in data[r])
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

        self.name = name
        self.row_based = True
        
        # Validate datatypes.
        if not(datatypes is None and (isinstance(data, DataFrame) or isinstance(data, dict))):
            self.transpose(row_based = False)
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
        self.transpose(row_based = True)
        return
    
    def __str__(self) -> str:
        """
        ### __str__

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Writes the data to a pretty text table.

        #### Requirements:
        - tabulate (func)

        #### Returns:
        - (str)

        #### Usage:
        >>> print(matrix)
        ╔══════════════╗
        ║    Matrix    ║
        ╚══════════════╝
        ╒════╤════╤════╕
        │ v1 │ v2 │ v3 │
        ╞════╪════╪════╡
        │  1 │  2 │  3 │
        ├────┼────┼────┤
        │  4 │  5 │  6 │
        ├────┼────┼────┤
        │  7 │  8 │  9 │
        ╘════╧════╧════╛

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)
        return tabulate(table = [tuple(self.columns)] + self.data, header = 1, name = self.name)
    
    def __repr__(self) -> str:
        """
        ### __repr__

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Displays an input that would yield the instance.

        #### Requirements:
        - Tabular.transpose (func)

        #### Returns:
        - (str)

        #### Usage:
        >>> matrix.__repr__()
        Tabular(
            data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)],
            columns = ['v1', 'v2', 'v3'],
            datatypes = ["<class 'int'>", "<class 'int'>", "<class 'int'>"]
        )

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)
        return f"Tabular(\n\tdata = {self.data},\n\tcolumns = {self.columns or 'None'},\n\tdatatypes = {[str(datatype) for datatype in self.datatypes] or 'None'}\n\tname = {self.name})"
    
    def __getitem__(self, key: int|list[int]|slice):
        """
        ### __getitem__

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Allows use of indexes and slices to produce a new Tabular from a subset of the data of the instance.

        #### Requirements:
        - Tabular.taular_from_tabular (func)

        #### Parameters:
        - key (int|list[int]|slice)

        #### Returns:
        - (Tabular)

        #### Usage:
        >>> matrix[1:3]

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)

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
            error = f"Invalid key passed to {self.name or 'Tabular'}."
            LOG.error(error)
            raise ValueError(error)
                
    def __validata(
        self,
        data: list[tuple] = None, 
        columns: list[str] = None,
        datatypes: list[str] = None
    ):
        """
        ### __validata

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        The validation process for raw data that checks the following.
            - Check data is a list.
            - Get number of columns.
            - Get number of rows.
            - Check number of columns or build columns list.
            - Check that all rows are tuples of the correct length.
            - Check number datatypes or build datatypes list.

        #### Parameters:
        - data (list[tuple]): The data stored in the Tabular.
        - columns (list[str]): The columns of the Tabular.
        - datatypes (list[type]): The datatypes of the Tabular.
        
        #### Usage:
        >>> matrix.__validata(data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)], columns = ['v1', 'v2', 'v3'])

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
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
        row_based: bool,
        name: str|None = None
    ):
        """
        ### __init_no_check

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Initialises a Tabular instance without performing validation checks.

        #### Parameters:
        - data (list[tuple]): The data stored in the Tabular.
        - columns (list[str]): The columns of the Tabular.
        - datatypes (list[type]): The datatypes of the Tabular.
        - row_count(int): The number of rows in the Tabular.
        - col_count (int): The number of columns in the Tabular.
        - row_based (bool): If true, this indicates that the data is stored as a list of rows. If false, the data is stored as a list of columns.
        - name (str): The name to associate with the Tabular (optional). Defaults to None.

        #### Usage:
        >>> matrix = __init_no_check(
                data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
                columns = ['v1', 'v2', 'v3'],
                datatypes = [<class 'int'>, <class 'int'>, <class 'int'>],
                row_count = 3,
                col_count = 3,
                row_based = True,
                name = 'Matrix'
            )

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.datatypes = datatypes
        self.columns = columns
        self.data = data
        self.row_count = row_count
        self.col_count = col_count
        self.row_based = row_based
        self.name = name
        return
    
    @staticmethod
    def tabular_from_tabular(
        data: list[tuple], 
        columns: list[str], 
        datatypes: list[type], 
        row_count: int, 
        col_count: int, 
        row_based: bool,
        name: str|None = None
    ):
        """
        ### tabular_from_tabular

        Version: 1.0
        Authors: JRA
        Date: 2024-03-50

        #### Explanation:
        Creates a new Tabular object from the existing instance without performing validation checks.
        
        #### Requirements:
        - Tabular.__init_no_check (func)

        #### Parameters:
        - data (list[tuple]): The data stored in the Tabular.
        - columns (list[str]): The columns of the Tabular.
        - datatypes (list[type]): The datatypes of the Tabular.
        - row_count(int): The number of rows in the Tabular.
        - col_count (int): The number of columns in the Tabular.
        - row_based (bool): If true, this indicates that the data is stored as a list of rows. If false, the data is stored as a list of columns.
        - name (str): The name to associate with the Tabular (optional). Defaults to None.

        #### Returns:
        - output (Tabular)

        #### Usage:
        >>> matrix = tabular_from_tabular(
                data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
                columns = ['v1', 'v2', 'v3'],
                datatypes = [<class 'int'>, <class 'int'>, <class 'int'>],
                row_count = 3,
                col_count = 3,
                row_based = True,
                name = 'Matrix'
            )

        #### History: 
        - 1.0 JRA (2024-03-05): Initial version.
        """
        output = Tabular.__new__(Tabular)
        output.__init_no_check(
            data,
            columns,
            datatypes, 
            row_count, 
            col_count, 
            row_based,
            name
        )
        return output

    def transpose(self, row_based: bool = None):
        """
        ### transpose

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Tranposes the storage of the data between a list of rows and a list of columns.

        #### Parameters:
        - row_based (bool): If true, Tabular data will be stored as a list of rows. If false, Tabular data will be stored as a list of columns. If None, the storage method is inverted. Defaults to None.

        #### Returns:
        - self (Tabular)

        #### Usage:
        >>> matrix.transpose()

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
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
        return self
    
    def col_pos(self, col: str) -> int:
        """
        ### col_pos

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Returns the column number of a given column name.

        #### Parameters:
        - col (str): The column to find the position number of.

        #### Returns:
        - (int)

        #### Usage:
        >>> matrix.col_pos('v1')
        0

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        if col in self.columns:
            return self.columns.index(col)
        else:
            error = f'Column "{col}" not found.'
            LOG.error(error)
            raise ValueError(error)
        
    def delete_columns(self, columns: int|str|list[int]|list[str]):
        """
        ### delete_columns

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Delete columns from the current Tabular.

        #### Requirements:
        - Tabular.col_pos

        #### Parameters:
        - columns (int|str|list[int]|list[str]): The list of column indexes or names to be deleted.
        
        #### Returns:
        - self (Tabular)

        #### Usage:
        >>> matrix.delete_columns('v1').__repr__()
        '''
        Tabular(
            data = [(2, 3), (5, 6), (8, 9)],
            columns = ['v2', 'v3'],
            datatypes = [<class 'int'>, <class 'int'>, <class 'int'>]
            name = 'Matrix'
        )
        '''

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        if not isinstance(columns, list):
            columns = list(columns)
        for c in range(len(columns)):
            if isinstance(columns[c], int):
                if columns[c] >= self.col_count:
                    columns[c] = None
            elif isinstance(columns[c], str):
                try:
                    columns[c] = self.col_pos(columns[c])
                except ValueError:
                    columns[c] = None                    
        columns = [column for column in columns if column is not None]
        self.transpose(row_based = False)
        for c in columns:
            del self.data[c]
            del self.columns[c]
            del self.datatypes[c]
        self.col_count -= len(columns)
        self.transpose(row_based = True)
        return self

        
    def get_column(self, column: int|str) -> tuple:
        """
        ### get_column

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Retrieves a column from the data.

        #### Requirements:
        - Tabular.col_pos (func)
        - Tabular.transpose (func)

        #### Parameters:
        - column (int|str): The column name or index to retrieve.

        #### Returns:
        - (tuple)

        #### Usage:
        >>> matrix.get_column('v3')
        (3, 6, 9)

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        if isinstance(column, str):
            column = self.col_pos(column)
        elif isinstance(column, int):
            if column >= self.col_count:
                error = f'There are only {self.col_count} columns - there is no column at index {column}.'
                LOG.error(error)
                raise AttributeError(error)
        output = self.transpose(row_based = False).data[column]
        self.transpose(row_based = True)
        return output
    
    def to_dataframe(self) -> DataFrame:
        """
        ### to_dataframe

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Converts the Tabular to a pandas DataFrame.

        #### Returns:
        - pandas.DataFrame

        #### Usage:
        >>> matrix.to_dataframe()
        <pandas.DataFrame>

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)
        return DataFrame(data = self.data, columns = self.columns)
    
    def to_dict(self, row: int = 0) -> dict:
        """
        ### to_dict

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Returns a row of the data, with columns as keys and cells as values.

        #### Parameters:
        - row (int): The index of the row to return.

        #### Returns:
        - (dict)

        #### Usage:
        >>> matrix.to_dict(1)
        {'v1': 4, 'v2': 5, 'v3': 6}

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)
        return dict(zip(self.columns, self.data[row]))
    
    def to_delimited(
        self, 
        row_separator: str = '\n', 
        col_separator: str = ',', 
        header: bool = True, 
        wrap_left: str = '', 
        wrap_right: str = ''
    ) -> str:
        """
        ### to_delimited

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Returns the Tabular as a delimited string.

        #### Parameters:
        - row_separator (str): The delimiter to have between rows. Defaults to newline.
        - col_separator (str): The delimited to have between columns. Defaults to a comma.
        - header (bool): If true, the column names form the first row of the output.
        - wrap_left (str): The string to wrap all cell values with on the left. Defaults to the empty string.
        - wrap_right (str): The string to wrap all cell values with on the right. Defaults to the empty string.
        
        #### Returns:
        - (str)

        #### Usage:
        >>> matrix.to_delimited(header = False)
        '''
        1,2,3
        4,5,6
        7,8,9
        '''

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)
        output = row_separator.join([col_separator.join([wrap_left + str(value or "") + wrap_right for value in row]) for row in self.data])
        if header:
            return col_separator.join([wrap_left + str(value or "") + wrap_right for value in self.columns]) + row_separator + output
        else:
            return output
        
    def to_stream(
        self, 
        row_separator: str = '\n', 
        col_separator: str = ',', 
        header: bool = True, 
        wrap_left: str = '', 
        wrap_right: str = ''
    ) -> StringIO:
        """
        ### to_stream

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Returns the Tabular as a delimited string stream.

        #### Requirements:
        - Tabular.to_delimited (func)

        #### Parameters:
        - row_separator (str): The delimiter to have between rows. Defaults to newline.
        - col_separator (str): The delimited to have between columns. Defaults to a comma.
        - header (bool): If true, the column names form the first row of the output.
        - wrap_left (str): The string to wrap all cell values with on the left. Defaults to the empty string.
        - wrap_right (str): The string to wrap all cell values with on the right. Defaults to the empty string.

        #### Returns:
        - (StringIO)

        #### Usage:
        >>> matrix.to_stream()

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        self.transpose(row_based = True)
        return StringIO(self.to_delimited(row_separator, col_separator, header, wrap_left, wrap_right))
        
    def to_html(
        self, 
        colours: dict[str, str] = {
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
    ) -> str:
        """
        ### to_html

        Version: 1.0
        Authors: JRA
        Date: 2024-03-05

        #### Explanation:
        Returns the Tabular as a HTML table.

        #### Requirements:
        - Tabular.transpose (func)
        - gradient_hex (func)

        #### Parameters:
        - colours (dict[str, str]): The colours to use in the HTML table as hexcodes.

        #### Returns:
        - html (str)

        #### Usage:
        >>> matrix.to_html()
        '''
        <table id="Matrix";style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid #000000;border-collapse:collapse">
            <tr style="color:#ffffff">
                    <th style="background-color:#541b8c;border:2px solid #000000">v1</th>
                    <th style="background-color:#181848;border:2px solid #000000">v2</th>
                    <th style="background-color:#181848;border:2px solid #000000">v3</th>
            </tr>
            <tr>
                    <td style="border:2px solid #000000;background-color:#541b8c;color:#ffffff">1</td>
                    <td style="border:1px solid #000000;background-color:#ffffff;color:#000000">2</td>
                    <td style="border:1px solid #000000;background-color:#ffffff;color:#000000">3</td>
            </tr>
            <tr>
                    <td style="border:2px solid #000000;background-color:#541b8c;color:#ffffff">4</td>
                    <td style="border:1px solid #000000;background-color:#b8d5f1;color:#000000">5</td>
                    <td style="border:1px solid #000000;background-color:#b8d5f1;color:#000000">6</td>
            </tr>
            <tr>
                    <td style="border:2px solid #000000;background-color:#541b8c;color:#ffffff">7</td>
                    <td style="border:1px solid #000000;background-color:#72abe3;color:#000000">8</td>
                    <td style="border:1px solid #000000;background-color:#72abe3;color:#000000">9</td>
            </tr>
        </table>
        '''

        #### History:
        - 1.0 JRA (2024-03-05): Initial version.
        """
        col_info = []
        for c in range(self.col_count):
            col_info.append({'numeric': False})
            if self.datatypes[c] not in (int, float):
                continue
            self.transpose(row_based = False)
            col = [value for value in self.data[c] if value is not None]
            if len(col) == 0:
                continue
            column = self.columns[c]
            col_info[c]['numeric'] = True
            col_info[c]['min'] = min(col)
            col_info[c]['max'] = max(col)
            col_info[c]['signed'] = (col_info[c]['min'] < 0)
        self.transpose(row_based = True)
        html = f'<table id="{self.name or "Tabular"}";style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid {colours["black"]};border-collapse:collapse">\n'
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
