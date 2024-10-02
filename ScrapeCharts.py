
from bs4 import BeautifulSoup
import requests
import asyncio
from prisma import Prisma
from datetime import datetime
import logging
import logging.config

today = datetime.today().strftime('%Y-%m-%d')
log_file = "./log/chart_scraper-{}.log".format(today)
logging.basicConfig(force=True, filename=log_file, filemode="w", level=logging.INFO, format='[%(asctime)s] - %(levelname)s - %(message)s')

async def get_charts():
  
  current = datetime.now()
  
  end_year = current.year
  end_week = current.isocalendar()[1]

  latest_chart = await prisma.chart.find_many({
    'orderBy': {
      id: 'desc',
    },
    'take': 1,
  })

  year = latest_chart[0].year
  week = latest_chart[0].week

  while year != end_year and week != end_week:
    
    if week < 10:
      temp_week = f"0${week}"
      date = f"${year}${temp_week}"
    else:
      date = f"${year}${week}" 
    
    chart_url = f"https://www.tanzmusik-online.de/charts/${date}"

    page = requests.get(chart_url)
    if page.status_code == 200:
      content = page.content
    else:
      logging.error("Chart Webseite nicht erreichbar")
      break
    
    soup = BeautifulSoup(content, 'html.parser')

    charts = []

    for song_row in soup.find_all('div', class_='row songRow visibleTrigger'):
      
      chart_data = {}

      chart_data['week'] = week
      chart_data['year'] = year

      title_tag = song_row.find('div', class_='songTitle').find('a')
      chart_data['title'] = title_tag.get_text() if title_tag else None
      artist_tag = song_row.find('span', class_='artist').find('a')
      chart_data['artist'] = artist_tag.get_text() if artist_tag else None

      # Aktuelle Position
      current_position = song_row.find('div', class_='position')
      if current_position:
        chart_data['chart_position'] = int(current_position.get_text())

      # Vorherige Position (in Klammern)
      previous_position = song_row.find('div', class_='preposition')
      if previous_position:
        # Die Zahl innerhalb der Klammern extrahieren
        chart_data['previous_position'] = int(previous_position.text.strip('()'))

      charts.append(chart_data)

    for chart in charts:
      logging.info("Chart:")
      logging.info(f"Woche: {chart['week']}")
      logging.info(f"Jahr: {chart['year']}")
      logging.info(f"Song: {chart['title']}")
      logging.info(f"Künstler: {chart['artist']}")
      logging.info(f"Position: {chart['chart_position']}")
      logging.info(f"Vorherige Position: {chart['previous_position']}")
      logging.info("------")

      db_song = await prisma.song.find_first(
          where = {
            'title': chart['title'],
            'artist': chart['artist']
          }
        )

      create_chart = await prisma.chart.create(
         data = {
            'year': chart['year'],
            'week': chart['week'],
            'song': {
              'connect': {
                'id': db_song.id
              }
            },
            'placement': chart['chart_position'],
            'previous': chart['previous_position']
         }
      )

      logging.info("Successfully added new Chart")

    if week+1 > 52:
      week = 1
      year += 1
    else:
      week += 1

prisma = Prisma()

async def main():

  await prisma.connect()

  logging.info("Adding new Charts to Database")
  await get_charts()
  logging.info("Program done!")

  await prisma.disconnect()

if __name__ == '__main__':
  asyncio.run(main())
