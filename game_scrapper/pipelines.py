# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
import os
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from time import time
from .utils import get_db_connection
from mysql.connector.errors import IntegrityError
from datetime import datetime


class IgdbPipeline:

    def process_item(self, item, spider):
        if spider.name != 'igdb':
            return item
        self.validate_item(item)
        self.clear_item(item)
        return item

    def validate_item(self, item):
        adapter = ItemAdapter(item)
        missing_fields = []
        required_fields = ('name', 'cover', 'consoles',
                           'first_release_date', 'categories')
        for key in required_fields:
            if bool(adapter.get(key)) == False:
                missing_fields.append(key)
        if missing_fields:
            raise DropItem('missing fields:' + ', '.join(missing_fields))

    def clear_item(self, item):
        adapter = ItemAdapter(item)

        # cover
        adapter['cover'] = adapter.get('cover')['image_id']

        # consoles
        consoles = []
        for i in adapter.get('consoles'):
            consoles.append(i['name'].title())
        adapter['consoles'] = consoles

        # categories
        categories = []
        for i in adapter.get('categories'):
            categories.append(i['name'])
        adapter['categories'] = categories

        # screenshots
        screenshots = []
        if adapter.get('screenshots'):
            for i in adapter.get('screenshots'):
                screenshots.append(i['image_id'])
        adapter['screenshots'] = screenshots


class StoreGamePipeline:

    def __init__(self):
        self.connection = get_db_connection()
        self.cursor = self.connection.cursor()
        self.consoles = self._get_consoles()
        self.categories = self._get_categories()

    def _get_consoles(self):
        consoles = {}
        self.cursor.execute('SELECT id, name from Console')
        rows = self.cursor.fetchall()
        for row in rows:
            consoles[row[1].title()] = row[0]
        return consoles

    def _get_categories(self):
        categories = {}
        self.cursor.execute('SELECT * FROM Category')
        rows = self.cursor.fetchall()
        for row in rows:
            categories[row[1]] = row[0]
        return categories

    def _set_category(self, name):
        self.cursor.execute('INSERT INTO Category(name) VALUES (%s)', (name,))
        self.categories[name] = self.cursor.lastrowid

    def process_item(self, item, spider):
        if spider.name != 'igdb':
            return item
        adapter = ItemAdapter(item)

        # insert game
        name = adapter['name']
        cover = adapter['cover']
        release_date = datetime.fromtimestamp(
            adapter['first_release_date']).strftime('%Y-%m-%d %H:%M:%S')
        summary = adapter.get('summary')
        storyline = adapter.get('storyline')
        try:
            self.cursor.execute("""INSERT INTO Game(name, cover, release_date, summary, storyline)
                            VALUES (%s, %s, %s, %s, %s) """, (name, cover, release_date, summary, storyline))
        except IntegrityError:
            DropItem(f'Game alread in db: {name}')
            return
        game_id = self.cursor.lastrowid
        # insert screenshots
        for image_id in adapter['screenshots']:
            self.cursor.execute(
                'INSERT INTO Screenshot VALUES (%s, %s)', (image_id, game_id))

        # associate consoles
        consoles = set(adapter['consoles']).intersection(self.consoles)
        for console in consoles:
            self.cursor.execute('INSERT INTO Game_Console(fk_game_id, fk_console_id) VALUES(%s, %s)',
                                (game_id, self.consoles[console]))

        # associate categories
        for category in adapter['categories']:
            if category not in self.categories.keys():
                self._set_category(category)
            cat_id = self.categories[category]
            self.cursor.execute('INSERT INTO Game_Category Values (%s, %s)',
                                (game_id, cat_id))
        self.connection.commit()
        logging.log(logging.INFO, f'INSERTED {name} into DATABASE')


class ScrappedItemPipeline:

    def process_item(self, item, spider):
        if spider.name == 'igdb':
            return item
        adapter = ItemAdapter(item)
        price = adapter.get('price')
        url = adapter.get('url')
        if type(price) != float or price <= 0:
            raise DropItem('invalid price value for {} price'.format(url))

        self._save_or_update(adapter, spider)

    def _save_or_update(self, item, spider):
        price = item.get('price')
        url = item.get('url')
        site = spider.name
        product_id = item.get('product_id')
        game_console_id = item.get('game_console_id')
        if spider.method == 'discover':
            query = """ INSERT INTO Game_Console_Site(fk_GameConsole_id, fk_Site_id, price, url)
            VALUES (%s, (Select id from Site where name = %s), %s, %s)
            """
            self.cursor.execute(query, (game_console_id, site, price, url))
            self.connection.commit()
            return

        # update price
        self.cursor.execute(
            """
            update Game_Console_Site set
            updated_at = if(price like %s, updated_at, CURRENT_TIMESTAMP),
            price = if(price like %s, price, %s)
            where Game_Console_Site.id = %s;
            """,
            (price, price, price, product_id)
        )
        logging.log(logging.INFO, self.cursor.statement)
        self.connection.commit()

    def __init__(self):
        self.connection = get_db_connection()
        self.cursor = self.connection.cursor()
