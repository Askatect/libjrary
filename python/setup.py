from setuptools import setup, find_packages
import re as regex
from os import listdir
from datetime import datetime

with open("README.md", "r") as f:
    old_version = f.readline().split(':')[-1].strip()
if not regex.fullmatch(r"^\d+\.\d+\.\d+$", old_version):
    raise Exception("Existing version number could not be read.")

new_version = input(f"Existing pyjap version is {old_version}.\nEnter new version: ")
if not regex.fullmatch(r"^\d+\.\d+\.\d+$", new_version):
    raise Exception("Incorrect format for version number!")
old_major, old_minor, old_patch = [int(x) for x in old_version.split('.')]
new_major, new_minor, new_patch = [int(x) for x in new_version.split('.')]
if new_major < old_major:
    raise Exception("Cannot revert major version.")
elif new_major == old_major:
    if new_minor < old_minor:
        raise Exception("Cannot revert minor version.")
    elif new_minor == old_minor:
        if new_patch <= old_patch:
            raise Exception("Must be a new patch.")
print("New version accepted.")

docstring = {}
docstring['v'] = new_version
# docstring['Author'] = 'JRA'
docstring['Date'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
docstring['Explanation'] = 'The libjrary features several generic tools for Python and SQL that I find useful.'
with open("requirements.txt", "r", encoding = 'utf-16') as f:
    docstring['Requirements'] = '- ' + '\n- '.join(line.strip() for line in f)
docstring['Attributes'] = '\n'.join(['- ' + f for f in listdir('pyjap') if f.endswith('.py')])
docstring['Usage'] = f">>> pip install <path>/pyjap-{new_version}-py3-none-any.whl\nor\n>>> powershell -File <path>\librare_pyjap_to_wheel.ps1"
with open("README.md", "r") as f:
    lines = f.readlines()
    try:
        history = lines[(lines.index('History:\n') + 1):]
    except ValueError as error:
        print(f"History section not found in existing README.md. {error}")
    else:
        history.insert(0, f"- {new_version} JRA ({docstring['Date']}): {input('Enter update notes: ')}\n")
        docstring['History'] = ''.join(historic for historic in history if historic != '\n')

with open("README.md", "w") as f:
    for key, value in docstring.items():
        sep = ' ' if key in ('v', 'Author', 'Date') else '\n'
        f.write("{}{}:{}{}\n".format((sep if sep == '\n' else ''), key, sep, value))

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name = "pyjap",
    version = new_version,
    author = "JRA",
    description = "Python tools from the libjrary.",
    long_description = long_description,
    packages = find_packages(),
    python_requires = '>=3.10'
)