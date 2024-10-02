
from bs4 import BeautifulSoup
import requests
import asyncio
from prisma import Prisma
from datetime import datetime
import logging
import logging.config

today = datetime.today().strftime('%Y-%m-%d')
log_file = "./log/rec_scraper-{}.log".format(today)
logging.basicConfig(force=True, filename=log_file, filemode="w", level=logging.INFO, format='[%(asctime)s] - %(levelname)s - %(message)s')

async def get_recs():

  current = datetime.now()
  
  end_year = current.year
  end_month = current.month

  latest_rec = await prisma.recommendation.find_many({
    'orderBy': {
      id: 'desc',
    },
    'take': 1,
  })

  year = latest_rec[0].year
  month = latest_rec[0].month

  while year != end_year and month != end_month:

    if month < 10:
      temp_month = f"0${month}"
      date = f"${year}-${temp_month}"
    else:
      date = f"${year}-${month}"
      
    rec_url = f"https://www.tanzmusik-online.de/recommendation/${date}"

    page = requests.get(rec_url)
    if page.status_code == 200:
      content = page.content
    else:
      logging.error("Recommendation Webseite nicht erreichbar")
      break
    
    soup = BeautifulSoup(content, 'html.parser')

    dance_categories = soup.find_all('h2')

    recommendations = []

    # Durch jede Tanzkategorie und zugehörige Songs iterieren
    for dance_category in dance_categories:

      rec_data = {}
      
      rec_data['month'] = month
      rec_data['year'] = year
      rec_data['category_name'] = dance_category.get_text()
                
      # Suche nach den Songs unter der jeweiligen Kategorie
      song_rows = dance_category.find_next_sibling('div', class_='songlist').find_all('div', class_='songRow')

      rec_data['songs'] = []
          
      for row in song_rows:
        # Titel und Künstler extrahieren
        s_data = {}
        s_data['title'] = row.find('div', class_='songTitle').get_text()
        s_data['artist'] = row.find('span', class_='artist').get_text()

        rec_data['songs'].append(s_data)

      recommendations.append(rec_data)

    for recommendation in recommendations:
      logging.info("Recommendation:")
      logging.info(f"Monat: {recommendation['month']}")
      logging.info(f"Jahr: {recommendation['year']}")
      logging.info(f"Kategorie: {recommendation['category']}")
      for song in recommendation['songs']:
        logging.info(f"Song: {song['title']}")
        logging.info(f"Künstler: {song['artist']}")
      logging.info("------")

      for song in recommendation['songs']:
        db_song = await prisma.song.find_first(
          where = {
            'title': song['title'],
            'artist': song['artist']
          }
        )

        create_rec = await prisma.recommendation.create(
          data = {
            'year': recommendation['year'],
            'month': recommendation['month'],
            'category': recommendation['category'],
            'song': {
              'connect': {
                'id': db_song.id
              }
            }
          }
        )

        logging.info("Succesfully added Recommendation")

    if month+1 > 12:
      month = 1
      year += 1
    else:
      month += 1

prisma = Prisma()

async def main():

  await prisma.connect()

  logging.info("Adding new Recommendations to Database!")
  await get_recs()
  logging.info("Program done!")

  await prisma.disconnect()

if __name__ == '__main__':
  asyncio.run(main())
