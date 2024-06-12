import sys
from utils import player_data_loader, url_loader
from player_scraper import Scraper
from db_manager import DatabaseManager


def main(csv_file, restart):
    database = DatabaseManager('players_database')
    if restart: database.restart()

    old_data = player_data_loader('data/playersData.csv')
    database.insert_data(old_data)

    for url in url_loader('data/' + csv_file):
        scraper = Scraper(url, database)
        scraper.parse()

    database.enrich_player_data()
    averages = database.averages()
    club, filtered = database.find_players()

    # something to do with acquired info...


if __name__ == '__main__':
    if len(sys.argv) == 2: main('playersURLs.csv', False)
    elif len(sys.argv) == 3: main(sys.argv[2], False)
    elif len(sys.argv) == 4: main(sys.argv[2], True if sys.argv[3][0].upper == 'Y' else False)
