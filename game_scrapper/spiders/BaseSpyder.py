import scrapy
import sqlite3
import os
from os.path import abspath, join, split
from game_scrapper.utils import get_db_connection

class BaseSpyder(scrapy.Spider):
    method = "" # discover or update
    console_slugs = []
    qnt_games = 100
    name = ''

    def _row_to_dict(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor(dictionary=True)

    def _get_game_list(self):
        if self.method == 'discover':
            query = f"""
            select Game.name as game, Console.name as console, Console.slug as slug, Game_Console.id as game_id
            from Game_Console
            left join Game_Console_Site on Game_Console_Site.fk_GameConsole_id = Game_Console.id
            inner join Game on Game_Console.fk_game_id = Game.id
            inner join Console on Game_Console.fk_console_id = Console.id
            where Game_Console_Site.fk_Site_id != (select id from Site where name = %s)  or Game_Console_Site.fk_Site_id is null
            """
            if self.console_slugs:
                query = query + ' and slug in ({})'.format(', '.join(self.console_slugs))
        elif self.method == 'update':
            query = f"""
            select Game.name as game, Console.name as console, Console.slug as slug, Game_Console_Site.id as product_id
            from Game_Console
            inner join Game_Console_Site on Game_Console_Site.fk_GameConsole_id = Game_Console.id
            inner join Game on Game_Console.fk_game_id = Game.id
            inner join Console on Game_Console.fk_console_id = Console.id
            where Game_Console_Site.fk_Site_id = (select id from Site where name = %s)
            """
        if not self.method: raise(Exception('self.method with invalid value ' + self.method))
        query += f' limit {self.qnt_games}'
        self.log(query + '\n' * 5)
        # games = self.cursor.execute(query, (self.name,))
        self.cursor.execute(query, ('kabum',))
        return self.cursor.fetchall()


