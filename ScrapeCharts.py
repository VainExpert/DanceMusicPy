
from bs4 import BeautifulSoup
import requests
import asyncio
from prisma import Prisma
from datetime import datetime
import logging
import logging.config
import random

today = datetime.today().strftime('%Y-%m-%d')
log_file = "./log/chart_scraper-{}.log".format(today)
logging.basicConfig(force=True, filename=log_file, filemode="w", level=logging.INFO, format='[%(asctime)s] - %(levelname)s - %(message)s')

def add_previous_placement(charts_data, prev_chart):
  # Iterate over the charts, comparing each with its previous one
  for i in range(1, len(charts_data)):
      if i == 1:
        previous_chart = prev_chart
      else:
        previous_chart = charts_data[i - 1]
      current_chart = charts_data[i]
      
      # Create a lookup dictionary for quick access to previous placements by song title
      previous_placements = {song['title']: song['chart_position'] for song in previous_chart['songs']}

      # Add 'previous_placement' for each song in the current chart
      for song in current_chart['songs']:
          song_title = song['title']
          # If the song exists in the previous chart, get its placement, otherwise set to -1
          song['previous_position'] = previous_placements.get(song_title, -1)

  return charts_data

async def order_songs_by_rank(songs, avg_score=0.0, min_votes=10, alpha=0.6, beta=0.4):
    """
    Orders song objects based on the combined rank of normalized user score, votes, and dance ratings.
    
    Parameters:
        songs (list): List of song dictionaries with 'score', 'votes', and 'dances' (list of dicts with 'name' and 'rating').
        avg_score (float): The average score across all songs in the dataset (in -1 to +1 range). Default is 0.0.
        min_votes (int): The minimum number of votes required for credibility. Default is 10.
        alpha (float): Weight for the user score. Default is 0.6.
        beta (float): Weight for the average dance rating. Default is 0.4.

    Returns:
        list: List of song objects, ordered by their combined rank.
    """
    
    async def calculate_combined_rank(song):
        user_score = song['score']
        votes = song['votes']
        
        # Normalize the user score from -1 to 1 to a 0 to 1 scale
        normalized_user_score = (user_score + 1) / 2
        
        # Normalize the average score (avg_score) similarly
        normalized_avg_score = (avg_score + 1) / 2
        
        # Calculate weighted score using Bayesian average with normalized scores
        weighted_score = ((votes * normalized_user_score) + (normalized_avg_score * min_votes)) / (votes + min_votes)

        get_artist = await prisma.artist.find_first(
          where = {
            'name': song['artist']
          }
        )

        get_song = await prisma.song.find_first(
          where = {
            'title': song['title'],
            'artist': get_artist.id
          }
        )

        get_dances = await prisma.dancesong.find_many(
          where = {
            'songId': get_song.id
          }
        )
        
        # Calculate normalized average dance rating
        avg_dance_rating = sum((dance.rating - 1) / 9 for dance in get_dances) / len(get_dances)
        
        # Calculate the combined rank using the weighted formula
        combined_rank = alpha * weighted_score + beta * avg_dance_rating
        return combined_rank
    
    # Add a 'rank' key to each song object based on the calculated combined rank
    for song in songs:
        song['chart_score'] = await calculate_combined_rank(song)
    
    # Sort the songs by the 'rank' in descending order (higher rank first)
    songs_sorted = sorted(songs, key=lambda x: x['chart_score'], reverse=True)

    for idx, song in enumerate(songs_sorted):
      song['chart_position'] = idx+1

    return songs_sorted

async def get_charts():
  
  current = datetime.now()
  end_year = current.year
  end_month = current.month
  end_week = current.isocalendar()[1]

  latest_chart = await prisma.chart.find_many(
    order =  {
      'id': 'desc',
    },
    take = 1
  )

  songs = await prisma.song.find_many(
    where = {
      'votes': {
        'gt': 0
      },
      'avgScore': {
        'gt': 0
      }
    },
    order = {
      'avgScore': 'desc'
    }
  )

  year = latest_chart[0].year
  month = latest_chart[0].month
  week = datetime(latest_chart[0].year, latest_chart[0].month, 1).isocalendar[1]
  all_charts = []

  if len(songs) >= 30:
    
    while year != end_year and month != end_month:
      chart = {}
      chart['year'] = year
      chart['month'] = month

      chart_songs = []
      
      for song in songs:
        song_data = {}

        song_data['title'] = song.title
        song_data['artist'] = song.artist.name
        song_data['score'] = song.avgScore
        song_data['votes'] = song.votes

      ordered_songs = await order_songs_by_rank(chart['songs'])
      chart['songs'] = ordered_songs

      all_charts.append(chart)

      if month+1 > 12:
        month = 1
        year += 1
      else:
        month += 1

  elif len(songs) >= 15:
    
    while year != end_year and month != end_month:
      chart = {}
      chart['year'] = year
      chart['month'] = month

      chart_songs = []
      song_ids = []
      
      for song in songs:
        song_data = {}

        song_data['title'] = song.title
        song_data['artist'] = song.artist.name
        song_data['score'] = song.avgScore
        song_data['votes'] = song.votes

        chart_songs.append(song_data)
        song_ids.append(song.id)

      all_songs = await prisma.song.find_many()

      while len(chart_songs) < 30:
        song_data = {}

        id = song_ids[0]
        while id in song_ids:
          id = random.randint(0, len(all_songs))
        
        get_song = await prisma.song.find_first(
          where = {
            'id': id
          }
        )

        song_data['title'] = get_song.title
        song_data['artist'] = get_song.artist.name
        song_data['score'] = get_song.avgScore
        song_data['votes'] = get_song.votes

        chart_songs.append(song_data)
        song_ids.append(song.id)

      ordered_songs = await order_songs_by_rank(chart['songs'])
      chart['songs'] = ordered_songs

      all_charts.append(chart)

      if month+1 > 12:
        month = 1
        year += 1
      else:
        month += 1

  else:

    while year != end_year and week != end_week and month != end_month:

      chart = {}
      
      if week < 10:
        temp_week = f"0${week}"
        date = f"${year}${temp_week}"
      else:
        date = f"${year}${week}"
      
      chart['year'] = year
      chart['month'] = month

      chart_url = f"https://www.tanzmusik-online.de/charts/${date}"

      page = requests.get(chart_url)
      if page.status_code == 200:
        content = page.content
      else:
        print("Chart Webseite nicht erreichbar")
        break
      
      soup = BeautifulSoup(content, 'html.parser')

      chart_songs = []

      for song_row in soup.find_all('div', class_='row songRow visibleTrigger'):
        
        song_data = {}

        title_tag = song_row.find('div', class_='songTitle').find('a')
        song_data['title'] = title_tag.get_text() if title_tag else None
        artist_tag = song_row.find('span', class_='artist').find('a')
        song_data['artist'] = artist_tag.get_text() if artist_tag else None

        song_data['score'] = int(song_row.find('div', class_='ratyBar')['data-initial-score'])
        song_data['votes'] = int(song_row.find('div', class_='votesText').find('span', class_='number').text.strip())

        chart_songs.append(song_data)
    
      ordered_songs = await order_songs_by_rank(chart['songs'])
      chart['songs'] = ordered_songs

      all_charts.append(chart)

      if week+1 > 52:
        week = 1
        month = 1
        year += 1
      else:
        week += 1
        month += 1

  all_charts = add_previous_placement(all_charts, latest_chart)
  
  for chart in all_charts:
      print("------")
      print("Chart:")
      print(f"Monat: {chart['month']}")
      print(f"Jahr: {chart['year']}")
      for song in chart['songs']:
        print(f"Song: {song['title']}")
        print(f"Künstler: {song['artist']}")
        print(f"Position: {song['chart_position']}")
        print(f"Vorherige: {song['previous_position']}")

        db_artist = await prisma.artist.find_first(
          data = {
            'where': {
              'name': song['artist']
            }
          }
        )

        db_song = await prisma.song.find_first(
          data = {
            'where': {
              'title': song['title'],
              'artist': db_artist.id
            }
          }
        )

        create_chart = await prisma.chart.create(
          data = {
              'year': chart['year'],
              'month': chart['month'],
              'song': {
                'connect': {
                  'id': db_song.id
                }
              },
              'placement': song['chart_position'],
              'previous': song['previous_position']
          }
        )

      print("------")

      print("Successfully added Chart")

prisma = Prisma()

async def main():

  await prisma.connect()

  logging.info("Adding new Charts to Database")
  await get_charts()
  logging.info("Program done!")

  await prisma.disconnect()

if __name__ == '__main__':
  asyncio.run(main())
