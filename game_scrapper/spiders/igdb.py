import scrapy
import os
import requests
import json
from datetime import datetime, timedelta
from os.path import abspath, join, split
from dotenv import load_dotenv
from game_scrapper.items import IgdbItem


class IgdbSpyder(scrapy.Spider):
    name = 'igdb'
    client_id = ''
    token = ''
    offset = 0
    limit = 100

    def __init__(self, year=2020, month=8):
        month = int(month)
        year = int(year)
        absdir = split(abspath(__file__))[0]
        env_file = (join(absdir, '../../.env'))
        load_dotenv(dotenv_path=env_file)
        self.token = self._get_token()
        if not self.token:
            return
        self.release_start = int(
            datetime(year, month, 1, 0, 0).timestamp() * 1000 ) 
        self.release_end = int(
            datetime(year + (month + 1) // 13, (month % 12) + 1, 1, 0, 0).timestamp() * 1000) 

    def _get_request_params(self):
        return {
            'url': 'https://api.igdb.com/v4/games',
            'headers': {
                'Accept': 'application/json',
                'Client-ID': self.client_id,
                'Authorization': 'Bearer ' + self.token
            },
            'body': f"""
            fields name, cover.image_id, platforms.name, first_release_date, genres.name, screenshots.image_id, storyline, summary;
            where status < 6 & platforms = (12, 48, 49, 130, 167, 169) & first_release_date >= {self.release_start} & first_release_date < {self.release_end};
            limit {self.limit}; offset {self.offset};
            """
        }

    def _get_token(self):
        self.client_id = os.environ.get('CLIENT_ID')
        secret = os.environ.get('SECRET')
        url = f'https://id.twitch.tv/oauth2/token?client_id={self.client_id}&client_secret={secret}&grant_type=client_credentials'
        resp = requests.post(url).text
        resp = json.loads(resp)
        return resp.get('access_token')

    def start_requests(self):
        request_obj = self._get_request_params()
        return [scrapy.Request(url=request_obj['url'], method="POST", headers=request_obj['headers'], body=request_obj['body'], callback=self.parse), ]

    def parse(self, response):
        resp = response.json()
        if not resp:
            return
        self.offset += self.limit
        for item in resp:
            yield IgdbItem(
                name=item.get('name'),
                cover=item.get('cover'),
                consoles=item.get('platforms'),
                first_release_date=item.get('first_release_date'),
                categories=item.get('genres'),
                screenshots=item.get('screenshots'),
                storyline=item.get('storyline'),
                summary=item.get('summary')
            )
        self.offset += self.limit
        request_obj = self._get_request_params()
        yield scrapy.Request(request_obj['url'], method="POST", headers=request_obj['headers'], body=request_obj['body'])
