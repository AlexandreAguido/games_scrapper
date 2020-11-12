# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlite3
import logging
import os
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class IgdbPipeline:

    def process_item(self, item, spider):
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

        #cover
        adapter['cover'] = adapter.get('cover')['image_id']
        
        #consoles
        consoles = []
        for i in adapter.get('consoles'):
            consoles.append(i['name'].title())
        adapter['consoles'] = consoles

        #categories
        categories = []
        for i in adapter.get('categories'):
            categories.append(i['name'])
        adapter['categories'] = categories
        
        #screenshots
        screenshots = []
        if adapter.get('screenshots'):
            for i in adapter.get('screenshots'):
                screenshots.append(i['image_id'])
        adapter['screenshots'] = screenshots
    
class StoreGamePipeline:

    def __init__(self):
        sqlite_path = os.environ.get('SQLITE_PATH')
        if not sqlite_path: exit()
        
        self.connection = sqlite3.connect(sqlite_path)
        self.cursor = self.connection.cursor()
        self.consoles = self._get_consoles()
        self.categories = self._get_categories()

    def _get_consoles(self):
        consoles = {}
        rows = self.cursor.execute('SELECT id, name from Console').fetchall()
        for row in rows:
            consoles[row[1].title()] = row[0]
        return consoles

    def _get_categories(self):
        categories = {}
        rows = self.cursor.execute('SELECT * FROM CATEGORY').fetchall()
        for row in rows:
            categories[row[1]] = row[0]
        return categories
        
    def _set_category(self, name):
        self.cursor.execute('INSERT INTO CATEGORY(name) VALUES (?)', (name,))
        self.categories[name] = self.cursor.lastrowid
        
    def process_item(self, item, spyder):
        adapter = ItemAdapter(item)

        #insert game
        name = adapter['name']
        cover = adapter['cover']
        release_date = adapter['first_release_date']
        summary = adapter.get('summary')
        storyline = adapter.get('storyline')
        try:
            self.cursor.execute("""INSERT INTO Game(name, cover, release_date, summary, storyline)
                            VALUES (?, ?, ?, ?, ?) """, (name, cover, release_date, summary, storyline))
        except sqlite3.IntegrityError:
            DropItem(f'Game alread in db: {name}')
            return
        game_id = self.cursor.lastrowid
        #insert screenshots
        for image_id in adapter['screenshots']:
            self.cursor.execute('INSERT INTO Screenshot VALUES (?, ?)', (image_id, game_id))

        #associate consoles
        consoles = set(adapter['consoles']).intersection(self.consoles)
        for console in consoles:
            self.cursor.execute('INSERT INTO Game_Console(fk_game_id, fk_console_id) VALUES(?, ?)', 
            (game_id, self.consoles[console]))
        
        #associate categories
        for category in adapter['categories']:
            if category not in self.categories.keys():
                self._set_category(category)
            cat_id = self.categories[category]
            self.cursor.execute('INSERT INTO Game_Category Values (?, ?)',
            (game_id, cat_id))
        self.connection.commit()
        logging.log(logging.INFO, f'INSERTED {name} into DATABASE')