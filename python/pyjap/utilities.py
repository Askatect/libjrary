from pyjap.logger import LOG

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
