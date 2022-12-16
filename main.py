from bs4 import BeautifulSoup
import requests
import csv
import os

# constants:
list_field_names = ['pairs']
# headers are needed for imitating user-like behavior
headers = {
    'User-Agent': 'My User Agent 1.0',
    'From': 'webscraping@domain.com'
}


def csv_init(filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, list_field_names)
        writer.writeheader()


def add_to_csv(pairs_list, filename):
    rec_dict = {'pairs': 'rec'}
    for pair in pairs_list:
        rec = pair.text
        with open("crypto_list.csv", "a", newline='') as csvfile:
            writer = csv.DictWriter(csvfile, list_field_names)
            rec_dict['pairs'] = rec
            writer.writerow(rec_dict)


def get_next_url(offset):
    base_list_url = f'https://finance.yahoo.com/crypto/?count=100&offset={offset}'
    return base_list_url


def parse_list(filename):
    urls_cnt = 0
    while True:
        url = get_next_url(urls_cnt)
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, 'lxml')

        parsed_pairs = soup.find_all(attrs={"data-test": "quoteLink"})
        add_to_csv(parsed_pairs, filename)
        print(urls_cnt)
        if urls_cnt > 9100:
            break
        urls_cnt += 100


def make_history_url(pair):
    base_history_url = f'https://finance.yahoo.com/quote/{pair}/history?p={pair}'
    return base_history_url


def load_history_for_pair(pair):
    url = make_history_url(pair)

    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    req_for_download = soup.find('a', class_='Fl(end) Mt(3px) Cur(p)')
    url_for_download = req_for_download['href']
    # adjust url_for_download here to set dataframe you need
    filename = req_for_download['download']
    file_binaries = requests.get(url_for_download, headers=headers)
    with open("history_data\\" + filename, "wb")as file:
        file.write(file_binaries.content)
    print(filename)


def download_list(list_filename):
    with open(list_filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if os.path.isfile("history_data\\" + row['pairs'] + '.csv'):
                # print(row['pairs'] + '.csv already exists')
                continue

            try:
                load_history_for_pair(row['pairs'])

            except Exception as err:
                print(row, "failed", err)


if __name__ == '__main__':
    filename = 'crypto_list.csv'
    csv_init(filename)
    parse_list(filename)
    download_list(filename)
