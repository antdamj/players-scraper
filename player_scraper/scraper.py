import datetime
import requests
from bs4 import BeautifulSoup
from utils import parse_date, calculate_age, allowed_fields
from db_manager import DatabaseManager
import pandas as pd


class Scraper:
    def __init__(self, url: str, database: DatabaseManager):
        self.url = url
        self.database = database
        self.soup = None
        self.features = {key: None for key in allowed_fields}

    def parse(self):
        try:
            response = requests.get(self.url)
            self.soup = BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions as e:
            # logger.log(this url not found)
            print(f"Error while reading {self.url}: {e}")

        if self.soup.find(class_='vcard') is None:  # not a footballer page
            return

        self.features['Name'], self.features['Full name'] = self.player_name()
        self.features['Date of birth'], self.features['Age'], self.features["Dead"] = self.player_birthday()
        self.features['City of birth'], self.features['Country of birth'] = self.player_birthplace()
        self.features['Position'] = self.prop("Position(s)")
        self.features["Current club"] = self.prop("Current team")
        self.features["National team"] = self.player_national()
        self.features["Appearances"], self.features["Goals"] = self.player_performance()
        self.features["Scraping timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.features["URL"] = self.url

        # perform database write of this result
        self.db_write()

    def prop(self, name):
        element = self.soup.find('th', class_='infobox-label', string=lambda text: text and name in text)
        return element.next_sibling.text.strip().split('[')[0] if element else None

    def player_name(self):
        # fetch name from Wiki article title
        name = self.soup.find(class_="mw-page-title-main").text
        if '(' in name:
            name = name.split('(')[0].strip()

        # fetch full player name, if not found, fill with name from title
        full_name = self.prop("Full name")
        if not full_name:
            full_name = name

        return name, full_name

    def player_birthday(self):
        day_of_birth = self.prop("Date of birth")
        day_of_death = self.prop("Date of death")
        age = None

        if day_of_birth:
            day_of_birth = parse_date(day_of_birth)
            today = parse_date(datetime.date.today().strftime("%d %B %Y"))
            age = calculate_age(day_of_birth, today)
        if day_of_death:
            day_of_death = parse_date(day_of_death)
            age = calculate_age(day_of_birth, day_of_death)

        return day_of_birth, age, day_of_death

    def player_birthplace(self):
        birthplace = self.prop("Place of birth")
        if birthplace is not None:
            birthplace = birthplace.split(', ', 1)
            country = birthplace[0]
            city = birthplace[1] if len(birthplace) == 2 else None
            return country, city
        else:
            return None, None

    def player_national(self):
        international = self.soup.find_all('th', class_='infobox-header')
        if international is not None:
            international = [x for x in international if "International career" in x.text]
            if len(international) > 0:
                international = international[0]
            else:
                return None
            for row in international.find_all_next('tr'):
                if row.find(class_='infobox-header') is not None:
                    return None
                th = row.find('th', class_='infobox-label')
                if th is not None:
                    span = th.find('span')
                    if span and '–' in span.text and not span.text.split('–')[-1].strip():
                        return row.find('td', class_='infobox-data infobox-data-a').text.strip()

    def player_performance(self):
        appearances, goals = 0, 0
        club = self.soup.find_all('th', class_='infobox-header')
        if club is not None:
            club = [x for x in club if "Senior career" in x.text]
            if len(club) > 0:
                club = club[0]
            else:
                return None, None
            for row in club.find_all_next('tr'):
                th = row.find('th', class_='infobox-label')
                if th and (c := th.next_sibling).text.strip() == self.features["Current club"]:
                    appearances_str = c.next_sibling.text.strip()
                    if appearances_str.isdigit():
                        appearances += int(appearances_str)
                    goals_str = c.next_sibling.next_sibling.text.strip()[1:-1]
                    if goals_str.isdigit():
                        goals += int(goals_str)

        return str(appearances), str(goals)

    def db_write(self):
        features_as_df = pd.DataFrame([self.features])
        self.database.insert_data(features_as_df)
