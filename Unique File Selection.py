import json
import os
import re
from multiprocessing import Pool
from typing import Dict
from zipfile import ZipFile, ZIP_BZIP2

import arrow
from bs4 import BeautifulSoup, Tag


def read_html_from_archive(path: str) -> Dict[str, list]:
    """
    Read html files from archive and return it as dict
    :param path: path to archive with html files
    :return: dict with html files by company names
    """

    html_dict = {}
    with ZipFile(path, 'r', ZIP_BZIP2) as zf:
        f_names = zf.namelist()
        for f_name in f_names:
            with zf.open(f_name) as fn:
                html_dict[f_name] = fn.read()

    return html_dict


def _parse_table(table: Tag) -> list:
    """
    Parse html table Tag (bs4 instance) to list with lists
    :param table: Tag with html table (bs4 instance)
    :return: list with lists
    """

    table_contents = []
    for tr in table.find_all('tr'):
        td_contents = []
        for td in tr.find_all('td'):
            td_contents.append(td.text.strip())
        table_contents.append(td_contents)
    return table_contents


def parse_html(html_dict: Dict[str, list]) -> Dict[str, list]:
    """
    Got html_dict with html files from archive. Convert it to target dict
    :param html_dict: dict with html files (texts)
    :return: dict where keys is company names and values are target paresed tables
    """

    parse_html_contents = {}
    pattern = r"([A-Z]{3})(.*)"
    for key, value in html_dict.items():
        try:
            company_name = re.match(pattern, key).group(1)
            table = BeautifulSoup(value, "html.parser").find("table")
            table_contents = _parse_table(table)
            parse_html_contents[company_name] = table_contents
        except AttributeError:
            print(key, "error")
            continue

    return parse_html_contents


def _sort_filenames_by_date(arch_filenames: list) -> list:
    """
    Convert filenames to date objects, sorting, recovery filenames from date objects
    :param arch_filenames: list of archive filenames
    :return: sorted by date archive filenames
    """

    date_objects = []
    for arch_name in arch_filenames:
        date_obj = arrow.get((arch_name[8:-4]), 'D_M_YYYY_H_m')
        date_objects.append(date_obj)

    recovered_arch_filenames = []
    for date_obj in sorted(date_objects):
        recovered_arch_name = 'archive_{d}_{m}_{y}_{H}_{M}.zip'.format(d=date_obj.day,
                                                                       m=date_obj.month,
                                                                       y=date_obj.year,
                                                                       H=date_obj.hour,
                                                                       M=date_obj.minute)
        recovered_arch_filenames.append(recovered_arch_name)

    return recovered_arch_filenames


def generate_sorted_paths_to_archives(arch_dir: str) -> list:
    """
    :param arch_dir: path to folder with all archives
    :return: sorted by date paths to archives
    """

    arch_filenames = os.listdir(arch_dir)
    sorted_by_date_arch_filenames = _sort_filenames_by_date(arch_filenames)

    sorted_by_date_arch_paths = []
    for f_name in sorted_by_date_arch_filenames:
        path = os.path.join(arch_dir, f_name)
        sorted_by_date_arch_paths.append(path)

    return sorted_by_date_arch_paths


def get_html_dict(path: str) -> Dict[str, list]:
    """
    Complate parsing cycle for arhcive. It's necessary for multiprocessing map function.
    :param path: path to archive
    :return: parsed dict
    """

    raw_html_dict = read_html_from_archive(path)
    html_dict = parse_html(raw_html_dict)

    return html_dict


def main():
    """
    Main logic
    Sort, parse and check duplicates
    Save dupticates to file
    """

    sorted_paths = generate_sorted_paths_to_archives(ARCH_DIR)

    with Pool(CORES_TO_USE) as p:
        html_dicts = p.map(get_html_dict, sorted_paths)

    duplicate_arch_paths = []
    previous_html_dict = None
    for path, html_dict in zip(sorted_paths, html_dicts):
        if html_dict == previous_html_dict:
            duplicate_arch_paths.append(path)

        previous_html_dict = html_dict

    with open(DUPLICATE_ARCH_PATHS_RESULT_FILE, "w") as f:
        json.dump(duplicate_arch_paths, f, indent=2)

    print(duplicate_arch_paths)


if __name__ == "__main__":
    CORES_TO_USE = 6
    ARCH_DIR = 'C:\\Users\\Lola\\Desktop\\Files_from_server\\YahooFinanceHistoryDownloader\\downloaded_data'
    DUPLICATE_ARCH_PATHS_RESULT_FILE = 'archives_to_remove_list.json'

    main()

