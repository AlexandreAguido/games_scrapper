import scrapy
import json
import re
from game_scrapper.spiders.BaseSpyder import BaseSpyder
from game_scrapper.items import ScrappedItem


class AmericanasSpider(BaseSpyder):
    name='americanas'
    console_slugs = ["'ps4'", "'ps5'", "'xbox one'", "'xbox 360'", "'ps3'", "'nintendo switch'"]
    search_url = """
    https://www.americanas.com.br/busca/{}?content={}
    &filter={{"id":"categoria","value":"Games|{}","fixed":false}}
    &filter={{"id":"wit","value":"Game","fixed":false}}
    &filter={{"id":"wit","value":"Jogo","fixed":false}}
    &filter={{"id":"condicao","value":"novo","fixed":false}}
    &sortBy=relevance
    """.replace(' ', '')

    def __init__(self, method='discover'):
        super().__init__()
        self.method = method
        self.qnt_games = 400

    def start_requests(self):
        requests = []
        for i in self._get_game_list():
            game = i['game']
            console = i['console']
            product_id = i.get('product_id')
            # if i['console'].startswith('Play'): console = i['slug']
            url = self.search_url.format(game, game, console)
            requests.append(
                scrapy.Request(url, callback=self.parse, 
                cb_kwargs={'game_console_id': i.get('game_id'), 'product_id':product_id, 'game':game})
            )
        return requests

    def parse(self, response, game_console_id, product_id, game):
        patt = 'PRELOADED_STATE__\s*=\s*(\{.*)</script>'
        game_info = re.search(patt, response.text)
        if game_info == None:
            self.log('Wrong pattern')
            return
        data = json.loads(game_info.groups()[0].replace('undefined', 'null'))['pages']
        search_id = list(data.keys())[0]
        search_result = data[search_id]['queries']['pageSearch']['result']['search']
        if search_result['total'] == 0: return  
        product = search_result['products'][0]['product']
        price = float(product['offers']['result'][0]['bestPaymentOption']['price'])
        url = 'https://americanas.com.br/produto/' + product['id']
        yield ScrappedItem(price = price, game_console_id = game_console_id, 
        url = url, product_id = product_id)