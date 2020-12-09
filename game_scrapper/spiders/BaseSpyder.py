import scrapy
import os
import logging
from game_scrapper.utils import get_db_connection

class BaseSpyder(scrapy.Spider):
    method = "" # discover or update
    console_slugs = []
    qnt_games = 200
    name = ''

    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor(dictionary=True)

    def _get_game_list(self):
        if self.method == 'discover':
            self.cursor.execute('select `offset` from Site where name = %s', (self.name,))
            # offset = self.cursor.fetchall()[0]['offset']
            offset = self.cursor.fetchone()['offset']
            query = f"""
            select Game.name as game, Console.name as console, Console.slug as slug, Game_Console.id as game_id
            from Game_Console
            left join Game_Console_Site on Game_Console_Site.fk_GameConsole_id = Game_Console.id
            inner join Game on Game_Console.fk_game_id = Game.id
            inner join Console on Game_Console.fk_console_id = Console.id
            where Game_Console_Site.fk_Site_id != (select id from Site where name = %s)  or Game_Console_Site.fk_Site_id is null
            
            """
            if self.console_slugs:
                query += ' and slug in ({})'.format(', '.join(self.console_slugs))
            query += ' order by Game.id asc limit %s offset %s'
        
            self.cursor.execute(query, (self.name, self.qnt_games,  offset))

        elif self.method == 'update':
            query = f"""
            select Game.name as game, Console.name as console, Console.slug as slug, Game_Console_Site.id as product_id
            from Game_Console
            inner join Game_Console_Site on Game_Console_Site.fk_GameConsole_id = Game_Console.id
            inner join Game on Game_Console.fk_game_id = Game.id
            inner join Console on Game_Console.fk_console_id = Console.id
            where Game_Console_Site.fk_Site_id = (select id from Site where name = %s)
            limit %s
            """
            self.cursor.execute(query, (self.name, self.qnt_games))
            logging.log(logging.INFO, self.cursor.statement)
        else: raise(Exception('self.method with invalid value ' + self.method))
        
        return self.cursor.fetchall()

    def closed(self, reason):
        if reason != 'finished': return
        query = 'update Site set `offset` = (Site.`offset` + %s) mod (select count(1) from Game_Console) where name = %s'
        self.cursor.execute(query, (self.qnt_games, self.name))
        self.conn.commit()
