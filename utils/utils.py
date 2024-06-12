import datetime
import pandas as pd


allowed_fields = ["Name", "Full name", "Date of birth", "Age", "City of birth", "Country of birth", "Position", "Current club", "National team", "Dead", "URL", "Appearances", "Goals", "Scraping timestamp"]


# yield all player URLs
def url_loader(file):
    with open(file) as f:
        for line in f:
            yield line.rstrip('\n')


# return a dataframe containing all previously written data
def player_data_loader(file):
    data = pd.read_csv(file, sep=";")
    data.rename(columns={col: col.replace('_', ' ') for col in data.columns}, inplace=True)
    return data


def month_to_number(month):
    month = month.lower()[:3]
    return {
            'jan': 1,
            'feb': 2,
            'mar': 3,
            'apr': 4,
            'may': 5,
            'jun': 6,
            'jul': 7,
            'aug': 8,
            'sep': 9,
            'oct': 10,
            'nov': 11,
            'dec': 12
    }[month]


# parse different date formats to DD.MM.YYYY
def parse_date(date):
    if date.startswith('('):
        date = date.split(')')[1].strip()
    if '(' in date:
        date = date.split('(')[0].strip()

    day, month, year = None, None, None
    for el in date.split(' '):
        el = el.replace(',', '')
        if el.isdigit():
            if not day: day = el
            elif not year: year = el
        else: month = month_to_number(el)

    return day + '.' + str(month) + '.' + year


# get difference in years for two provided dates
def calculate_age(d1, d2):
    d1, d2 = d1.split('.'), d2.split('.')
    d1 = datetime.date(int(d1[2]), int(d1[1]), int(d1[0]))
    d2 = datetime.date(int(d2[2]), int(d2[1]), int(d2[0]))
    age = d2.year - d1.year
    if d2.month < d1.month or (d2.month == d1.month and d2.day < d1.day):
        age -= 1
    return str(age)
