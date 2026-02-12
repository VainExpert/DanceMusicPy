
from fastapi import FastAPI, HTTPException
from prisma import Prisma
from pydantic import BaseModel
import random
import string

app = FastAPI()
prisma = Prisma()

# Pydantic Modell für den Request Body
class UserCreate(BaseModel):
  logging: bool
  recommend: bool
  recordDuration: int
  language: str

class LogCreate(BaseModel):
  title: str
  artist: str

class ScoreData(BaseModel):
  score: int
  title: str
  artist: str

@app.on_event("startup")
async def startup():
  await prisma.connect()

@app.on_event("shutdown")
async def shutdown():
  await prisma.disconnect()

@app.get("/user/{username}")
async def get_user(username: str):
  user = await prisma.user.find_unique(where={"username": username})
  if user is None:
    raise HTTPException(status_code=404, detail="User not found")
  return user

@app.post("/user")
async def create_user():
    
  characters = string.ascii_letters + string.digits + string.punctuation
  name = ''.join(random.choice(characters) for _ in range(10))

  all_users = await prisma.user.find_many()
  all_names = []
  for user in all_users:
    all_names.append(user.username)

  while name in all_names:
    name = ''.join(random.choice(characters) for _ in range(10))

  user = await prisma.user.create(data={
    'username': name
  })
  return user

@app.put("/user/{username}")
async def update_user(username: str, user_data: UserCreate):
  user = await prisma.user.update(
    where={"username": username},
    data={
      "logging": user_data.logging,
      "recommend": user_data.recommend,
      "recordDuration": user_data.recordDuration,
      "language": user_data.language
    }
  )
  if user is None:
    raise HTTPException(status_code=404, detail="User not found")
  return user

@app.delete("/user/{username}")
async def delete_user(username: str):
  user = await prisma.user.delete(where={"username": username})
  if user is None:
    raise HTTPException(status_code=404, detail="User not found")
  return user

@app.post("/score/{username}")
async def update_score(username: str, score_data: ScoreData):

  artist = await prisma.artist.find_first(
    where = {
      'name': score_data.artist
    }
  )
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")

  song = await prisma.song.find_first(
    where = {
      'title': score_data.title,
      'artistId': artist.id
    },
    include = {
      'danceSongs': True
    }
  )
  if song is None:
    raise HTTPException(status_code=404, detail="Song not found")
  
  new_votes = song.votes + 1
  
  up_song = await prisma.song.update(
    where = {
      'id': song.id
    },
    data = {
      'votes': new_votes
    }
  )
  return up_song

@app.post("/log/{username}")
async def create_log(username: str, log_data: LogCreate):
  
  artist = await prisma.artist.find_first(
    where = {
      'name': log_data.artist
    }
  )
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")

  song = await prisma.song.find_first(
    where = {
      'title': log_data.title,
      'artistId': artist.id
    }
  )
  if song is None:
    raise HTTPException(status_code=404, detail="Song not found")

  logentry = await prisma.logentry.create(data={
    'user': {
      'connect': {
        'username': username
      }
    },
    'song': {
      'connect': {
        'id': song.id
      }
    }
  })
  return logentry

@app.get("/logs/{username}")
async def get_log(username: str):
  log = await prisma.logentry.find_many(
    where={
      "username": username
      },
    include = {
      "song": {
        'include': {
          'artist': True
        }
      }
    }
    )
  if log is None:
    raise HTTPException(status_code=404, detail="Log not found")
  return log

@app.get("/dances")
async def get_dances():
  dances = await prisma.dance.find_many()
  if dances is None:
    raise HTTPException(status_code=404, detail="Dances not found")
  return dances

@app.get("/inspiration")
async def get_inspiration():
  
  all_songs = await prisma.song.find_many(
    include = {
      'danceSongs': True,
      'artist': True
    }
  )
  if all_songs is None:
    raise HTTPException(status_code=404, detail="Songs not found")
  inspirations = [random.choice(all_songs) for _ in range(10)]

  if inspirations is None:
    raise HTTPException(status_code=404, detail="Inspirations not found")
  return inspirations

@app.get("/artists")
async def get_artists():
  artists = await prisma.artist.find_many()
  if artists is None:
    raise HTTPException(status_code=404, detail="Artists not found")
  return artists

@app.get("/songs")
async def get_songs():
  songs = await prisma.song.find_many(
    include = {
      'artist': True,
      'danceSongs': True
    }
  )
  if songs is None:
    raise HTTPException(status_code=404, detail="Songs not found")
  return songs

@app.get("/songs/{artist_name}")
async def get_songs_by_artist(artist_name: str):
  
  artist = await prisma.artist.find_first(
    where = {
      'name': artist_name
    }
  )
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")
  
  songs = await prisma.song.find_many(
    where = {
      'artistId': artist.id
    },
    include = {
      'artist': True,
      'danceSongs': True
    }
  )
  if songs is None:
    raise HTTPException(status_code=404, detail=f"Songs by {artist_name} not found")
  return songs

@app.get("/song/{artist_name}/{title}")
async def get_song_by_title_artist(artist_name: str, title: str):
  
  artist = await prisma.artist.find_first(
    where = {
      'name': artist_name
    }
  )
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")
  
  song = await prisma.song.find_first(
    where = {
      'artistId': artist.id,
      'title': title
    },
    include = {
      'artist': True,
      'danceSongs': True
    }
  )
  if song is None:
    raise HTTPException(status_code=404, detail=f"Song {title} by {artist_name} not found")
  return song

@app.get("/charts")
async def get_charts():

  charts = await prisma.chart.find_many(
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if charts is None:
    raise HTTPException(status_code=404, detail=f"Charts not found")
  return charts

@app.get("/charts/{year}")
async def get_charts_by_year(year: int):

  charts = await prisma.chart.find_many(
    where = {
      'year': year
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if charts is None:
    raise HTTPException(status_code=404, detail=f"Charts in {year} not found")
  return charts

@app.get("/charts/{year}/{month}")
async def get_charts_by_year_month(year: int, month: int):

  charts = await prisma.chart.find_many(
    where = {
      'year': year,
      'month': month
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if charts is None:
    raise HTTPException(status_code=404, detail=f"Charts in {month}-{year} not found")
  return charts

@app.get("/recommendations/{username}")
async def get_recommendations_by_user(username: str):

  recommendations = await prisma.recommendation.find_many(
    where = {
      'username': username
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if recommendations is None:
    raise HTTPException(status_code=404, detail=f"Recommendations for {username} not found")
  return recommendations

@app.get("/recommendations/{username}/{year}")
async def get_recommendations_by_user_year(username: str, year: int):

  recommendations = await prisma.chart.find_many(
    where = {
      'year': year,
      'username': username
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if recommendations is None:
    raise HTTPException(status_code=404, detail=f"Recommendations for {username} in {year} not found")
  return recommendations

@app.get("/recommendations/{username}/{year}/{week}")
async def get_recommendations_by_user_year_week(username: str, year: int, week: int):

  recommendations = await prisma.chart.find_many(
    where = {
      'year': year,
      'week': week,
      'username': username
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if recommendations is None:
    raise HTTPException(status_code=404, detail=f"Recommendations for {username} in W{week}-{year} not found")
  return recommendations

@app.get("/recommendations/{year}")
async def get_recommendations_by_year(year: int):

  recommendations = await prisma.chart.find_many(
    where = {
      'year': year,
      'username': None
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if recommendations is None:
    raise HTTPException(status_code=404, detail=f"Recommendations in {year} not found")
  return recommendations

@app.get("/recommendations/{year}/{week}")
async def get_recommendations_by_year_week(year: int, week: int):

  recommendations = await prisma.chart.find_many(
    where = {
      'year': year,
      'week': week,
      'username': None
    },
    include = {
      'song': {
        'include': {
          'artist': True,
          'danceSongs': True
        }
      }
    }
  )
  if recommendations is None:
    raise HTTPException(status_code=404, detail=f"Recommendations in W{week}-{year} not found")
  return recommendations
