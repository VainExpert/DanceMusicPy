
import asyncio
from prisma import Prisma
from datetime import datetime
import logging
import logging.config
import random

today = datetime.today().strftime('%Y-%m-%d')
log_file = "./log/rec_scraper-{}.log".format(today)
logging.basicConfig(force=True, filename=log_file, filemode="w", level=logging.INFO, format='[%(asctime)s] - %(levelname)s - %(message)s')

tag_list = ["Weihnachten", "Halloween", "Ostern", "Hochzeit"]
season_list = ["11-12", "10", "4", "5-6"]
all_types = ["Latein", "Walzer", "Swing", "Tango", "Foxtrott"]

async def get_cat_songs(categories, ratings):

  for category, idx in enumerate(categories):
    if category['cat'] in tag_list:
      get_songs = await prisma.songtag.find_many(
        where = {
          'tagName': category
        },
        include = {
          'song': {
            'include': {
              'artist': True
            }
          }
        }
      )

    else:
      if len(ratings) > 0:
        get_songs = await prisma.dancesong.find_many(
          where = {
            'danceName': category,
            'rating': {
              'gte': ratings[idx]
            }
          },
          include = {
            'song': {
              'include': {
                'artist': True
              }
            }
          }
        )
      
      while len(category['songs']) < 2:
        random_s = random.choice(get_songs)
        song = {}

        song['title'] = random_s.song.title
        song['artist'] = random_s.song.artist.name

        if song not in category['songs']:
          category['songs'].append(song)
      
  return categories

async def get_tag_cat(rec, types, date, length):
  
  get_tags = await prisma.tag.find_many()

  for tag in get_tags:
    if length == 4:
      break

    seasons = tag.season.split("-")
    for season in seasons:
      if int(season) == date.month:
        length += 1
        types.append('Seasonal')
        
        cat = {'cat': tag, 'songs': []}
        rec['categories'].append(cat)
  
  return [rec, types]

async def get_recs():

  latest_rec = await prisma.recommendation.find_many(
    order = {
      'id': 'desc',
    },
    take = 1
  )

  year = latest_rec[0].year
  week = latest_rec[0].week

  current = datetime.now()
  end_year = current.year
  end_week = current.isocalendar()[1] 

  all_recs = []

  while year != end_year and week != end_week:

    recommendation = {}
    recommendation['week'] = current.isocalendar()[1]
    recommendation['year'] = year
    recommendation['categories'] = []

    length = 3
    types = random.sample(all_types, 3)

    results = await get_tag_cat(recommendation, types, length)
    recommendation = results[0]
    types = results[1]

    for type in types:
      if type != "Seasonal":
        get_new_dances = await prisma.dance.find_many(
          where = {
            'type': type
          }
        )
        cat = {'cat': random.choice(get_new_dances), 'songs': []}
        recommendation['categories'].append(cat)
    
    recommendation['categories'] = await get_cat_songs(recommendation['categories'])

    all_recs.append(recommendation)

    if week+1 > 52:
      week = 1
      year += 1
    else:
      week += 1

  for recommendation in all_recs:
    print("------")
    print("Recommendation:")
    print(f"Woche: {recommendation['week']}")
    print(f"Jahr: {recommendation['year']}")
    for category in recommendation['categories']:
      print(f"Kategorie: {category['cat']}")
      for song in category['songs']:
        print(f"Song: {song['title']}")
        print(f"Künstler: {song['artist']}")
    print("------")

    for category in recommendation['categories']:
      for song in category['songs']:
        db_artist = await prisma.artist.find_first(
            where = {
              'name': song['artist']
            }
          )

        db_song = await prisma.song.find_first(
          where = {
            'title': song['title'],
            'artist': {
              'connect': {
                'id': db_artist.id
              }
            }
          }
        )

        if category['cat'] in tag_list:
          create_rec = await prisma.recommendation.create(
            data = {
              'year': recommendation['year'],
              'week': recommendation['week'],
              'catTag': {
                'connect': {
                  'name': category['cat']
                }
              },
              'song': {
                'connect': {
                  'id': db_song.id
                }
              }
            }
          )

        else:
          create_rec = await prisma.recommendation.create(
            data = {
              'year': recommendation['year'],
              'week': recommendation['week'],
              'catDance': {
                'connect': {
                  'name': category['cat']
                }
              },
              'song': {
                'connect': {
                  'id': db_song.id
                }
              }
            }
          )
        print("Successfully added Recommendation")

async def get_personal_recs():

  users = await prisma.user.find_many(
    where = {
      'recommend': True
    }
  )

  latest_rec = await prisma.recommendation.find_many(
    order = {
      'id': 'desc',
    },
    take = 1
  )

  year = latest_rec[0].year
  week = latest_rec[0].week

  current = datetime.now()
  end_year = current.year
  end_week = current.isocalendar()[1] 

  all_recs = []

  while year != end_year and week != end_week:
    for user in users:
      rated_dances = await prisma.dancescore.find_many(
        where = {
          'username': user.username
        },
        order = {
          'avgRating': 'desc'
        }
      )

      recommendation = {}
      recommendation['user'] = user.username
      recommendation['week'] = current.isocalendar()[1]
      recommendation['year'] = current.year
      recommendation['categories'] = []

      length = 3
      ratings = []

      if len(rated_dances) >= length:
        recommendation['categories'] = [rated_dances[len(rated_dances) // 2].danceName]
        types = [rated_dances[0].dance.type, rated_dances[-1].dance.type]
        ratings = [rated_dances[0].avgRating, rated_dances[len(rated_dances) // 2].avgRating, rated_dances[-1].avgRating]
      
      else:
        recommendation['categories'] = [dance.danceName for dance in rated_dances]
        set_types = [dance.type for dance in rated_dances]
        types = []
        while len(set_types) < length:
          ran_type = random.choice(all_types)
          if ran_type not in set_types:
            set_types.append(ran_type)
            types.append(ran_type)
        
        idx = 0
        for type in types:
          if len(rated_dances) < length:
            get_new_dances = await prisma.dance.find_many(
              where = {
                'type': type
              }
            )
            cat = {'cat': random.choice(get_new_dances), 'songs': []}
          else:
            get_new_dances = await prisma.dance.find_many(
              where = {
                'type': type,
                'rating': {
                  'gte': rated_dances[idx].avgRating
                }
              }
            )
            cat = {'cat': random.choice(get_new_dances), 'songs': []}
            idx = -1
          recommendation['categories'].append(cat)
                
      results = await get_tag_cat(recommendation, set_types, length)
      recommendation = results[0]
      
      recommendation['categories'] = await get_cat_songs(recommendation['categories'], ratings)

      all_recs.append(recommendation)

    if week+1 > 52:
      week = 1
      year += 1
    else:
      week += 1

  for recommendation in all_recs:
    logging.info("------")
    logging.info("Recommendation:")
    logging.info(f"Nutzer: {recommendation['user']}")
    logging.info(f"Woche: {recommendation['week']}")
    logging.info(f"Jahr: {recommendation['year']}")
    for category in recommendation['categories']:
      logging.info(f"Kategorie: {category['cat']}")
      for song in category['songs']:
        logging.info(f"Song: {song['title']}")
        logging.info(f"Künstler: {song['artist']}")
    logging.info("------")

    for category in recommendation['categories']:
      for song in category['songs']:
        db_artist = await prisma.artist.find_first(
            where = {
              'name': song['artist']
            }
          )

        db_song = await prisma.song.find_first(
          where = {
            'title': song['title'],
            'artist': {
              'connect': {
                'id': db_artist.id
              }
            }
          }
        )

        if category['cat'] in tag_list:
          create_rec = await prisma.recommendation.create(
            data = {
              'year': recommendation['year'],
              'week': recommendation['week'],
              'catTag': {
                'connect': {
                  'name': category['cat']
                }
              },
              'song': {
                'connect': {
                  'id': db_song.id
                }
              },
              'user': {
                'connect': {
                  'username': recommendation['user']
                }
              }
            }
          )

        else:
          create_rec = await prisma.recommendation.create(
            data = {
              'year': recommendation['year'],
              'week': recommendation['week'],
              'catDance': {
                'connect': {
                  'name': category['cat']
                }
              },
              'song': {
                'connect': {
                  'id': db_song.id
                }
              },
              'user': {
                'connect': {
                  'username': recommendation['user']
                }
              }
            }
          )
        print("Successfully added Recommendation")

prisma = Prisma()

async def main():

  await prisma.connect()

  logging.info("Adding new Recommendations to Database!")
  await get_recs()
  await get_personal_recs()
  logging.info("Program done!")

  await prisma.disconnect()

if __name__ == '__main__':
  asyncio.run(main())
