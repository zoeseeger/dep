import os
import glob
import win32com.client
from bs4 import BeautifulSoup


def writeFile(filename:str, text:str):
    """Write text to file in the current directory."""

    with open(filename, 'w') as w:
        w.write(text)


def writeLinesFile(dir:str, filename:str, lines:list):
    """Write list of lines to file in the current directory."""

    with open(dir + '/' + filename, 'w') as w:
        for line in lines:
            w.write(f"{line}\n")


def findFirstFile(pattern:str):
    """Return first file or None that match pattern."""

    try:
        file = glob.glob(pattern)[0]
        file = os.path.abspath(file)
        return file
    except IndexError:
        print(f"Could not find confluence doc ({pattern}) in current directory. continuing . . .")
        return None


def doc2Html(filename:str, debug:bool):
    """Convert exported doc file to HTML."""

    tempfile = filename.replace('.doc', '_temp.html')

    # if html exists and debug on dont make html
    if debug and os.path.exists(tempfile):
        return tempfile

    doc = win32com.client.GetObject(filename)
    doc.SaveAs(FileName=tempfile, FileFormat=8)
    doc.Close()
    return tempfile


def readHtmlAndHeatSoup(html_file:str):
    """Read HTML file and return BeautifulSoup object."""

    with open(html_file, "r", encoding="UTF-16") as file:
        html = file.read()
    return BeautifulSoup(html, "html.parser")


def getUnderHeaderHtml(heading):
    """Add each sibling text to list. Should have anything under header except what to add to list."""

    list_ = []

    # for each sibling
    for sib in heading.find_next_siblings():

        # break at next heading
        if sib.name in ("h1", "h2", "h3", "h4"):
            return list(filter(None, list_))

        # add item
        else:
            list_.extend(sib.text.replace('\u00a0', ' ').replace('·', ' ').strip().split('\n'))

    return list(filter(None, list_))


def getTableUnderHeaderHtml(heading):
    """Find div and get all sibling rows of div."""

    list_ = []
    row_header = []

    # for each sibling
    for sib in heading.find_next_siblings():

        # break at next heading
        if sib.name in ("h1", "h2", "h1"):
            return row_header, list_

        # table div
        elif sib.name == "div":

            # row tag
            rows = sib.find_all('tr')

            # first row is header
            row_header = [el.text.strip() for el in rows[0].find_all('td')]

            # rest of rows
            for row in rows[1:]:
                list_.append([el.text.strip().replace('\n', '').replace('  ', ' ').replace('\u00a0', ' ') for el in row.find_all('td')])

    return row_header, list_

def makeOutputDirectory(output_dir):
    """If not exists, make directory."""

    if os.path.exists(output_dir):
        return
    else:
        os.mkdir(output_dir)