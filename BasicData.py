
from bs4 import BeautifulSoup
import requests
from shazamio import Shazam
import asyncio
from prisma import Prisma
import random
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import logging
import logging.config
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')
log_file = "./log/web_scraper-{}.log".format(today)
logging.basicConfig(force=True, filename=log_file, filemode="w", level=logging.WARNING, format='[%(asctime)s] - %(levelname)s - %(message)s')

problems = 0

christmas_songs = [
    'Jingle Bells',
    'Silent Night (Stille Nacht)',
    'O Holy Night',
    'Deck the Halls',
    'Hark! The Herald Angels Sing',
    'Joy to the World',
    'The First Noel',
    'Feliz Navidad',
    'Rudolph the Red-Nosed Reindeer',
    'Frosty the Snowman',
    'Have Yourself a Merry Little Christmas',
    'White Christmas',
    'It’s Beginning to Look a Lot Like Christmas',
    'Rockin\' Around the Christmas Tree',
    'Santa Claus Is Coming to Town',
    'Let It Snow! Let It Snow! Let It Snow!',
    'All I Want for Christmas Is You',
    'The Christmas Song (Chestnuts Roasting on an Open Fire)',
    'Winter Wonderland',
    'Do They Know It\'s Christmas?',
    'O Tannenbaum',
    'Leise rieselt der Schnee',
    'Kling, Glöckchen, klingelingeling',
    'Es ist ein Ros entsprungen',
    'Ihr Kinderlein, kommet'
]

halloween_songs = [
    'Thriller',
    'Ghostbusters',
    'Monster Mash',
    'Somebody’s Watching Me',
    'This Is Halloween',
    'Highway to Hell',
    'Disturbia',
    'Spooky, Scary Skeletons',
    'Time Warp',
    'Superstition',
    'Black Magic Woman',
    'Bury a Friend',
    'Sympathy for the Devil',
    'People Are Strange',
    'Dead Man’s Party',
    'Season of the Witch',
    'I Put a Spell on You',
    'Dragula',
    'Werewolves of London',
    'Bark at the Moon',
    'Die Hexen kommen',
    'Totentanz',
    'Mein Teil'
]

easter_songs = [
    'Christ the Lord Is Risen Today',
    'Because He Lives',
    'Up from the Grave He Arose',
    'He Lives',
    'In Christ Alone',
    'The Old Rugged Cross',
    'Were You There?',
    'Glorious Day (Living He Loved Me)',
    'My Redeemer Lives',
    'Amazing Grace',
    'Hosanna',
    'At the Cross',
    'How Great Thou Art',
    'Forever',
    'O Praise the Name (Anástasis)',
    'Via Dolorosa',
    'Lead Me to the Cross',
    'The Lion and the Lamb',
    'Easter Song',
    'Is He Worthy?',
    'Christ ist erstanden',
    'Erstanden ist der heilig Christ',
    'Wir wollen alle fröhlich sein'
]

wedding_songs = [
    'At Last',
    'All of Me',
    'A Thousand Years',
    'Marry Me',
    'Perfect',
    'Canon in D',
    'Thinking Out Loud',
    'I Will Always Love You',
    'Can’t Help Falling in Love',
    'Make You Feel My Love',
    'The Way You Look Tonight',
    'Endless Love',
    'You Are the Best Thing',
    'Here Comes the Sun',
    'Your Song',
    'I Don’t Want to Miss a Thing',
    'How Long Will I Love You',
    'From This Moment On',
    'Love on Top',
    'I Choose You',
    'Ja',
    'Dir gehört mein Herz',
    'Liebe ist',
    'Du lässt mich sein, so wie ich bin'
]


async def get_shazam_tracks(song_data):

  title = song_data['song_title'].replace("#", "")
  
  shazam_tracks = await shazam.search_track(f"{song_data['artist']} {title}", limit=5)
  return_tracks = []
  if shazam_tracks:
    for track in shazam_tracks['tracks']['hits']:
      current_track = {}
      
      current_track['artist'] = track['heading']['subtitle']
      current_track['title'] = track['heading']['title']
      
      for action in track['stores']['apple']['actions']:
         if action['type'] == 'uri':
            current_track['apple_url'] = action['uri']
            
      print(track)
      
      if track['images']['coverart'] != "":
        current_track['image'] = track['images']['coverart']
      else:
        current_track['image'] = ""

      #Add Release Date from Shazam Data
      current_track['release'] = None
      
      return_tracks.append(current_track)

  return return_tracks

async def get_spotify_tracks(song_data):

  spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id="b0c7d447c72a4fd798dd22c4ae6507f9",
                                                      client_secret="05591d3451b345bf89a79f40bfb85d21",
                                                      redirect_uri="https://example.org/callback"))

  title = song_data['song_title'].replace("#", "")
  results = spotify.search(f"{song_data['artist']} {title}", limit=5, type="track")

  track_info_list = []

  for track in results['tracks']['items']:
    track_name = track['name']
    track_url = track['external_urls']['spotify']
    
    # Extracting artist names
    artist_names = [artist['name'] for artist in track['artists']]

    # Extracting image URL
    if 'images' in track['album'] and len(track['album']['images']) > 0:
      image_url = track['album']['images'][0]['url']
    else:
      image_url = ""
    
    track_info = {
      "title": track_name,
      "spotify_url": track_url,
      "artists": artist_names,
      "image": image_url
    }

    print(track_info)
    
    track_info_list.append(track_info)

  return track_info_list

async def get_dances():

  with open('danceDB.json') as f:
    danceDB = json.load(f)
    print("Loaded all Dances")
    print()
  
  for dance in danceDB['danceDB']:
    create_dance = await prisma.dance.create(
      data = {
        "name": dance['nameDe'],
        "meter": dance['meter'],
        "bpm": dance['bpm'],
        "mpm": dance['mpm'],
        "type": dance['typeDe']
      }
    )
    print(f"Successfully added Dance {dance['nameDe']}")
  print()

async def get_tags():

  for i in range(len(tag_list)):
    create_tag = await prisma.tag.create(
      data = {
        'tag': tag_list[i],
        'season': season_list[i]
      }
    )
    print(f"Successfully added Tag {tag_list[i]}")
  print()

async def get_songs():
  
  start_url = "https://www.tanzmusik-online.de/music"

  page = requests.get(start_url)

  if page.status_code == 200:
    content = page.content
  else:
    print("Gesamt Webseite nicht erreichbar")
    exit()

  soup = BeautifulSoup(content, 'html.parser')

  interpreten_divs = soup.find_all('div', class_='col-lg-3 col-md-4 col-sm-6 col-xs-offset-1 col-xs-12')
  interpreten = [div.find('a').get_text() for div in interpreten_divs if div.find('a')]

  for interpret in interpreten:
    create_artist = await prisma.artist.create(
         data = {
            'name': interpret
         }
      )
    print(f"Successfully added Artist {interpret}")
  print()

  links = [div.find('a')['href'] for div in interpreten_divs if div.find('a')]

  for link in links:
      
    page = requests.get(link)
    if page.status_code == 200:
      content = page.content
    else:
      print("Interpreten Webseite nicht erreichbar")
      continue
    
    soup = BeautifulSoup(content, 'html.parser')

    songs = []

    # Suche nach jedem Song-Container
    for song_row in soup.find_all('div', class_='songRow'):
      song_data = {}

      # Songtitel extrahieren
      title_tag = song_row.find('div', class_='songTitle').find('a')
      song_data['song_title'] = title_tag.get_text() if title_tag else None
      song_data['song_url'] = title_tag['href'] if title_tag else None
      
      # Künstlername extrahieren
      artist_tag = song_row.find('span', class_='artist').find('a')
      song_data['artist'] = artist_tag.get_text() if artist_tag else None

      by_hand_checked = song_row.find('span', class_='byHandChecked')['title']
      song_data['expert_checked'] = True if "Experten" in by_hand_checked or "100" in by_hand_checked else False
      
      # Tanzarten und deren Bewertungen extrahieren
      dances = []
      for dance_div in song_row.find_all('div', class_='dances'):
          for dance in dance_div.find_all('div'):
              dance_name_tag = dance.find('a')
              if dance_name_tag:
                  dance_name = dance_name_tag.get_text()
                  # Extrahiere die Anzahl der aktiven Sterne
                  active_stars = dance.find_all('i', class_='fa fa-star active')
                  rating = len(active_stars) * 2
                  if rating < 8 and rating > 2 and not by_hand_checked:
                    rating = rating + random.randint(-2, 3)
                  dances.append({'dance': dance_name, 'rating': rating})

      song_data['dances'] = dances
      
      result = await get_shazam_tracks(song_data)

      song_data['shazam'] = False
      song_data['apple_url'] = ""
      song_data['image'] = ""
      song_data['release'] = None
      if result:
        for res_song in result:
          if song_data['artist'].lower() == res_song['subtitle'].lower() and song_data['song_title'].lower() == res_song['title'].lower():
            song_data['shazam'] = True
            song_data['apple_url'] = res_song['apple_url']
            song_data['image'] = res_song['image']
            song_data['release'] = res_song['release']
            
            break

      result = await get_spotify_tracks(song_data)

      song_data['spotify_url'] = ""
      if result:
        for res_song in result:
          if song_data['song_title'].lower() == res_song['title'].lower():
            for artist in res_song['artists']:
              if song_data['artist'].lower() == artist.lower():
                song_data['spotify_url'] = res_song['spotify_url']

                if song_data['image'] == "" and res_song['image'] != "":
                  song_data['image'] = res_song['image']
            
                break
      
      if song_data['spotify_url'] != "":
        exit()

      # Füge den extrahierten Song zu der Liste hinzu
      songs.append(song_data)

    # Ausgabe der extrahierten Daten
    for song in songs:
      print("------")
      print(f"Song: {song['song_title']} ({song['song_url']})")
      print(f"Künstler: {song['artist']}")
      print(f"Release: {song['release']}")
      print(f"Bild: {song['image']}")
      print(f"Geprüft: {song['expert_checked']}")
      for dance in song['dances']:
        print(f"Tanz: {dance['dance']} - Bewertung: {dance['rating']} Sterne")
      print(f"In Shazam: {song['shazam']}")
      print(f"Apple URL: {song['apple_url']}")
      print(f"Spotify URL: {song['spotify_url']}")
      print("------")

      a_artist = await prisma.artist.find_first(
        where = {
          'name': song['artist']
        }
      )

      create_song = await prisma.song.create(
         data = {
            'title': song['song_title'],
            'image': song['image'],
            'artist': {
              'connect': {
                'id': a_artist.id,
              }
            },
            'release': song['release'],
            'checked': song['expert_checked'],
            'appleMusicUrl': song['apple_url'],
            'spotifyUrl': song['spotify_url'],
            'shazam': song['shazam']
         }
      )

      if song['apple_url'] == "" and not song['shazam'] and song['spotify_url'] == "" and song['image'] == "":
        problems += 1
        logging.warning(f"Song: {song['song_title']} ({song['song_url']})")
        logging.warning(f"Künstler: {song['artist']}")
        logging.warning(f"ID: {create_song.id}")
        logging.warning("Keine Apple URL, nicht auf Shazam, kein Bild und keine Spotify URL")
      
      elif not song['shazam'] and song['apple_url'] == "":
        problems += 1
        logging.warning("-------------------------------")
        logging.warning(f"Song: {song['song_title']} ({song['song_url']})")
        logging.warning(f"Künstler: {song['artist']}")
        logging.warning(f"ID: {create_song.id}")
        logging.warning("Keine Apple URL und nicht auf Shazam")
        logging.warning("-------------------------------")

      elif not song['shazam'] and song['apple_url'] == "" and song['image'] == "":
        problems += 1
        logging.warning("-------------------------------")
        logging.warning(f"Song: {song['song_title']} ({song['song_url']})")
        logging.warning(f"Künstler: {song['artist']}")
        logging.warning(f"ID: {create_song.id}")
        logging.warning("Keine Apple URL, kein Bild und nicht auf Shazam")
        logging.warning("-------------------------------")
      
      elif song['spotify_url'] == "" and song['image'] == "":
        problems += 1
        logging.warning("-------------------------------")
        logging.warning(f"Song: {song['song_title']} ({song['song_url']})")
        logging.warning(f"Künstler: {song['artist']}")
        logging.warning(f"ID: {create_song.id}")
        logging.warning("Keine Spotify URL und kein Bild")
        logging.warning("-------------------------------")
      
      elif song['spotify_url'] == "":
        problems += 1
        logging.warning("-------------------------------")
        logging.warning(f"Song: {song['song_title']} ({song['song_url']})")
        logging.warning(f"Künstler: {song['artist']}")
        logging.warning(f"ID: {create_song.id}")
        logging.warning("Keine Spotify URL")
        logging.warning("-------------------------------")
      
      elif song['image'] == "":
        problems += 1
        logging.warning("-------------------------------")
        logging.warning(f"Song: {song['song_title']} ({song['song_url']})")
        logging.warning(f"Künstler: {song['artist']}")
        logging.warning(f"ID: {create_song.id}")
        logging.warning("Kein Bild")
        logging.warning("-------------------------------")


      print(f"Successfully added Song {song['title']} {song['artist']}")
      print("------")


      tags = []
      for title in christmas_songs:
        if song['title'] == title:
          tags.append("Weihnachten")
          break

      for title in halloween_songs:
        if song['title'] == title:
          tags.append("Halloween")
          break
      
      for title in easter_songs:
        if song['title'] == title:
          tags.append("Ostern")
          break
      
      for title in wedding_songs:
        if song['title'] == title:
          tags.append("Hochzeit")
          break

      for tag in tags:
        create_songtag = prisma.songtag.create(
            data = {
              'tag': {
                'connect': {
                  'tag': tag
                }
              },
              'song': {
                'connect': {
                  'id': create_song.id
                }
              }
            }
          )
        print(f"Added Tag {tag} to Song {create_song.title}")


      for dance in song['dances']:
        create_dancesongs = await prisma.dancesong.create(
          data = {
              'song': {
                'connect': {
                  'title': create_song.id,
                }
              },
              'dance': {
                'connect': {
                  'name': dance['dance'],
                }
              },
              'rating': dance['rating'],
          }
        )

        print("Successfully added DanceSong")
      print()

async def get_charts():
  
  year = 2020
  week = 12
  end_year = 2024
  end_week = 39

  all_charts = []

  while year != end_year and week != end_week:

    chart = {}
    
    if week < 10:
      temp_week = f"0${week}"
      date = f"${year}${temp_week}"
    else:
      date = f"${year}${week}" 

    chart['week'] = week
    chart['year'] = year
    
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

      #Get Score and Votes from Website -> ChatGPT
      song_data['score'] = 0

      song_data['votes'] = 0

      song_data['chart_score'] = 0

      chart_songs.append(song_data)
    
    chart['songs'] = chart_songs

    all_charts.append(chart)

    if week+1 > 52:
      week = 1
      year += 1
    else:
      week += 1
  
  true_charts = []

  for i in range(0, len(all_charts), 4):

    chart = all_charts[i]
    
    new_chart = {}
    month = int(chart['week'] / 4)
    new_chart['month'] = month
    new_chart['year'] = chart['year']

    for song in chart['songs']:
      get_artist = await prisma.artist.find_first(
        daat = {
          'where': {
            'name': song['artist']
          }
        }
      )

      get_song = await prisma.song.find_first(
        data = {
          'where': {
            'title': song['title'],
            'artist': get_artist.id
          }
        }
      )

      get_dances = await prisma.dancesong.find_many(
        data = {
          'where': {
            'songId': get_song.id
          }
        }
      )

      chart_score = 0
      dance_score = 0
      for dance in get_dances:
        dance_score += dance.rating

      dance_score = dance_score / len(get_dances)

      chart_score = song['score'] * song['votes'] / dance_score

      song['chart_score'] = chart_score

    #Sort songs with chart_score, new_placement and prev_position -> ChatGPT
    songs = []

    new_chart['songs'] = songs
    true_charts.append(new_chart)

  if len(true_charts) != 56:
    print(f"Fail! Nicht jeder oder zu viele Monate gemappt. {len(true_charts)}")
  else:
    for chart in true_charts:
      print("------")
      print("Chart:")
      print(f"Monat: {chart['month']}")
      print(f"Jahr: {chart['year']}")
      for song in chart['songs']:
        print(f"Song: {song['title']}")
        print(f"Künstler: {song['artist']}")
        print(f"Position: {song['chart_position']}")
        print(f"Veränderung: {song['change']}")

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
              'change': song['change']
          }
        )

      print("------")

      print("Successfully added Chart")

async def get_recs(): 
  
  year = 2020
  month = 3
  end_year = 2024
  end_month = 10

  all_recs = []

  while year != end_year and month != end_month:

    if month < 10:
      temp_month = f"0${month}"
      date = f"${year}-${temp_month}"
    else:
      date = f"${year}-${month}"

    recommendation = {}
    recommendation['month'] = month
    recommendation['year'] = year
      
    rec_url = f"https://www.tanzmusik-online.de/recommendation/${date}"

    page = requests.get(rec_url)
    if page.status_code == 200:
      content = page.content
    else:
      print("Recommendation Webseite nicht erreichbar")
      break
    
    soup = BeautifulSoup(content, 'html.parser')

    dance_categories = soup.find_all('h2')

    categories = []

    # Durch jede Tanzkategorie und zugehörige Songs iterieren
    for dance_category in dance_categories:

      cat_data = {}
      cat_data['cat'] = dance_category.get_text()
                
      # Suche nach den Songs unter der jeweiligen Kategorie
      song_rows = dance_category.find_next_sibling('div', class_='songlist').find_all('div', class_='songRow')

      cat_data['songs'] = []

      for row in song_rows:
        # Titel und Künstler extrahieren
        s_data = {}
        s_data['title'] = row.find('div', class_='songTitle').get_text()
        s_data['artist'] = row.find('span', class_='artist').get_text()

        cat_data['songs'].append(s_data)

      categories.append(cat_data)
    
    recommendation['categories'] = categories
    
    all_recs.append(recommendation)

    if month+1 > 12:
      month = 1
      year += 1
    else:
      month += 1

  new_recs = []
  for recommendation in all_recs:
    
    new_rec = {}
    
    new_rec['week'] = recommendation['month'] * 4
    new_rec['year'] = recommendation['year']
    
    all_types = []
    length = 3
    while len(recommendation['categories']) < length:
      types = []
      
      for category in recommendation['categories']:
        get_dance = await prisma.dance.find_first(
          data = {
            'where': {
              'name': category['cat']
            }
          }
        )
        types.append(get_dance.type)

      get_tags = await prisma.tag.find_many()
      all_seasons = []

      for tag in get_tags:
        seasons = tag.season.split("-")
        for season in seasons:
          all_seasons.append(season)

      while len(types) < length:
        for type in all_types:
          if type not in types:
            
            if type == "Seasonal":
              date = datetime.now()
              for season in all_seasons:
                if int(season) == date.month:
                  length += 1
                  types.append(type)
                  cat = {'cat': tag, 'songs': []}
                  recommendation['categories'].append(cat)

            get_new_dances = await prisma.dance.find_many(
              data = {
                'where': {
                  'type': type
                }
              }
            )
            types.append(type)
            
            random_num = random.randint(0, len(get_new_dances))
            new_dance = get_new_dances[random_num]
            
            cat = {'cat': new_dance.name , 'songs': []}
            recommendation['categories'].append(cat)

    new_rec['categories'] = recommendation['categories']
      
    for category in new_rec['categories']:

      if category in tag_list:
        get_songs = await prisma.songtag.find_many(
          data = {
            'where': {
              'danceName': category
            },
            'include': {
              'songs': {
                'artist': True
              }
            }
          }
        )

      else:
        get_songs = await prisma.dancesong.find_many(
          data = {
            'where': {
              'tagName': category
            },
            'include': {
              'songs': {
                'artist': True
              }
            }
          }
        )

      while len(category['songs']) < 2:
        
        random_num = random.randint(0, len(get_songs.songs))
        song = {}

        song['title'] = get_songs.songs[random_num].title
        song['artist'] = get_songs.songs[random_num].artist.name

        if song not in category['songs']:
          category['songs'].append(song)

    #Extrapolate Recommendations to every week with release date of songs and the current week and year to be added -> ask ChatGPT for advice
    

  for recommendation in new_recs:
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
            data = {
              'where': {
                'name': song['artist']
              }
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

prisma = Prisma()
shazam = Shazam()

tag_list = ["Weihnachten", "Halloween", "Ostern", "Hochzeit"]
season_list = ["11-12", "10", "4", "5-6"]

async def main():

  await prisma.connect()

  await prisma.song.delete_many()
  await prisma.dance.delete_many()
  await prisma.tag.delete_many()

  await get_tags()

  await get_dances()

  await get_songs()

  await get_charts()

  await get_recs()

  await prisma.disconnect()

  logging.warning(f"Anzahl manuell Nachzuarbeitender Songs: {problems}")

if __name__ == '__main__':
  asyncio.run(main())
