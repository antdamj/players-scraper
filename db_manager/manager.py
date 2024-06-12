import sqlite3 as sql
import pandas as pd
from utils import allowed_fields


class DatabaseManager:
    def __init__(self, file):
        self.database = file
        self.connection = None
        self.cursor = None
        self.connect()

    # create a database and remember its connection cursor
    def connect(self):
        # create a database file if not already present
        with open(self.database, 'a') as _: pass

        self.connection = sql.connect(self.database)
        self.cursor = self.connection.cursor()

        # create a player table if it already does not exist
        self.cursor.execute("""select name from sqlite_master where type='table' and name='player'""")
        if not self.cursor.fetchone():
            self.cursor.execute("""create table player(url, name, fullname, birth_date, age, birth_place, birth_country, positions, current_club, national_team, appearances_current_club, goals_current_club, scraping_timestamp)""")

    def insert_data(self, data: pd.DataFrame):

        # for initial data: keep only columns that are required
        # fields_intersection = [f for f in allowed_fields if f in data.columns]
        # data = data[fields_intersection]

        # for initial data: add all missing required columns
        # missing_columns = [f for f in allowed_fields if f not in data.columns]
        # data = data.assign(**{m: None for m in missing_columns})


        for row in data.iterrows():
            row = row[1]  # because iterrows returns tuple (row_index, row_data)

            #  remove all unnecessary columns and fill with missing necessary and add data if existing
            fields_intersection = [f for f in allowed_fields if f in row.index]
            new_row = row.loc[fields_intersection]
            missing_columns = [f for f in allowed_fields if f not in data.columns]
            for field in missing_columns:
                new_row[field] = None
                if field in data.columns: new_row[field] = row[field]
            row = new_row

            # check if this player is already in the database
            self.cursor.execute("""select * from player where url=(?)""", (row["URL"],))

            if self.cursor.fetchone():  # player already in
                query = """update player set name=?, fullname=?, birth_date=?, age=?, birth_place=?, birth_country=?, positions=?, current_club=?, national_team=?, appearances_current_club=?, goals_current_club=?, scraping_timestamp=? where url=?"""
                self.cursor.execute(query, (
                row['Name'], row['Full name'], row['Date of birth'], row['Age'], row['City of birth'],
                row['Country of birth'], row['Position'], row['Current club'], row['National team'], row['Appearances'], row['Goals'], row['Scraping timestamp'], row['URL'], ))

            else:  # have to add player
                query = """insert into player(url, name, fullname, birth_date, age, birth_place, birth_country, positions, current_club, national_team, appearances_current_club, goals_current_club, scraping_timestamp) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                self.cursor.execute(query, (
                row['URL'], row['Name'], row['Full name'], row['Date of birth'], row['Age'], row['City of birth'],
                row['Country of birth'], row['Position'], row['Current club'], row['National team'], row['Appearances'], row['Goals'], row['Scraping timestamp']))


        self.connection.commit()

    def restart(self):
        self.cursor.execute("""drop table player""")
        self.connection.commit()

    def enrich_player_data(self):
        # add two new columns if not existing
        columns = self.cursor.execute("""pragma table_info(player)""").fetchall()
        ac, gpcg = False, False  # age category, goals per club game
        for column in columns:
            if column[1] == 'age_category': ac = True
            if column[1] == 'goals_per_club_game': gpcg = True

        if not ac: self.cursor.execute("""alter table player add age_category""")
        if not gpcg: self.cursor.execute("""alter table player add goals_per_club_game""")

        # update the new columns
        self.cursor.execute("""
            update player set age_category = case
                when age <= 23 then 'Young'
                when age between 24 and 32 then 'MidAge'
                when age >= 33 then 'Old'
            end
        """)

        self.cursor.execute("""
            update player set goals_per_club_game = case
                when appearances_current_club > 0 then cast(goals_current_club as float) / appearances_current_club
                else 0
            end
        """)

        self.connection.commit()

    def averages(self):
        result = self.cursor.execute("""
            select current_club, avg(age) as avgAge, avg(appearances_current_club) as avgAppearances, count(*) as countPlayers
            from player where current_club is not NULL group by current_club
        """).fetchall()
        self.connection.commit()
        return result

    def find_players(self):
        # select a random club
        club = self.cursor.execute("""select current_club from player where current_club is not null order by random() limit 1""").fetchone()

        players = self.cursor.execute("""
            with filtered_player as (select * from player where current_club = ?)
            select
                p1.name,
                count(p2.name) as count_player
                from filtered_player p1 left join player p2 on
                p1.age > p2.age and p1.positions = p2.positions and p1.appearances_current_club < p2.appearances_current_club
                GROUP BY
        p1.name, p1.current_club, p1.positions, p1.age, p1.appearances_current_club;
        """, (club[0], )).fetchall()
        self.connection.commit()

        return club, players
