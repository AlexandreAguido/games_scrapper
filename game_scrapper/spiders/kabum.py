import scrapy
import json
import re
from game_scrapper.spiders.BaseSpyder import BaseSpyder
from game_scrapper.items import ScrappedItem


class KabumSpider(BaseSpyder):
    name='kabum'
    console_slugs = ["'ps4'", "'ps5', 'xbox one'"]
    search_url = 'https://www.kabum.com.br/cgi-local/site/listagem/listagem.cgi?string={}'

    def __init__(self, method='discover'):
        super().__init__()
        self.method = method
        
    def start_requests(self):
        requests = []
        for i in self._get_game_list():
            console = i['console']
            product_id = i.get('product_id')
            if i['console'].startswith('Play'): console = i['slug']
            game = re.sub('\W', ' ' , i['game'])
            query = f'Game {game} {console}'
            requests.append(
                scrapy.Request(self.search_url.format(query), callback=self.parse, 
                cb_kwargs={'game_console_id': i.get('game_id'), 'product_id':product_id})
            )
        return requests

    def parse(self, response, game_console_id, product_id):
        game_info = re.search(r'listagemDados = (\[.*\])', response.text)
        game_obj = json.loads(game_info.groups()[0])
        if not game_obj: return
        price = float(game_obj[0]['preco_desconto'])
        url = 'https://kabum.com.br' + game_obj[0]['link_descricao']
        yield ScrappedItem(price = price, game_console_id = game_console_id, 
        url = url, product_id = product_id)