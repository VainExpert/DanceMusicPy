
from tkinter import *
import time

import asyncio
from shazamio import Shazam

import sounddevice as sd
import soundfile as sf

from bs4 import BeautifulSoup
import requests

from MusicBG import *
from PIL import Image

samplerate = 44100  # Hertz
duration = 12  # seconds
filename = 'recorded.wav'

#Define Window
window = Tk()
window.title("Tanz zu deiner Musik")
window.geometry("450x400")

MusicUIBG()

BackgroundImage = PhotoImage(file='MusicNote.png')
BackGroundCanvas = Canvas(window, width=450, height=400, background='white')
BGIMG = BackGroundCanvas.create_image(225, 200, image=BackgroundImage, anchor='center')

Interpret = Label(window, text="Interpret: ", background='white')
Interpret.place(height=25, x=100, y=45)

Song = Label(window, text="Songtitle: ", background='white')
Song.place(height=25, x=100, y=70)


artistIn = Entry(window)
titleIn = Entry(window)
artistIn.place(height=25, x=200, y=45)
titleIn.place(height=25, x=200, y=70)

WelcomeText = BackGroundCanvas.create_text(225, 25, font=('calibri', 25, 'bold'), text="Willkommen")
InfoText = BackGroundCanvas.create_text(225, 150, font=('calibri', 15, 'bold'), text=" ")


def handleInputBtt():

    BackGroundCanvas.itemconfigure(WelcomeText, text="Programm arbeitet")
    window.update_idletasks()

    artist = artistIn.get()
    title = titleIn.get()
    
    information = [title, artist]
    
    InputMain(information)

def handleRecordBtt():

    BackGroundCanvas.itemconfigure(WelcomeText, text="Programm arbeitet")
    window.update_idletasks()

    loop = asyncio.new_event_loop()
    loop.run_until_complete(RecordMain())
    loop.close()

def ChangeInfoText(output, number):
    
    if number == 0:
        BackGroundCanvas.itemconfigure(InfoText, text=output)
        window.update_idletasks()
        return
        
    else:
        BackGroundCanvas.itemconfigure(InfoText, text=output)
        window.update_idletasks()
        output += "."
        time.sleep(1)
        BackGroundCanvas.after(500, ChangeInfoText(output, number-1))
        


def ReformatData(information):
    
    title = information[0].replace("-", " ").title()
    artist = information[1].replace("-", " ").title()
    
    return title, artist
    
def FormatData(information):
    
    title = information[0].replace(" ", "-").lower()
    artist = information[1].replace(" ", "-").lower()
    
    return title, artist

def FormatDances(dances, rating):

    danceString = ""
    for i in range(len(dances)):
        danceString += dances[i]
        danceString += " (" + rating[i] + ")"
        danceString += " oder\n"
        
    danceString = danceString[:-6]
    danceString += "\n"
    
    return danceString



def GetDances(artist, title):

    url = "https://www.tanzmusik-online.de/music/{}/title/{}".format(artist, title)

    page = requests.get(url)

    if page.status_code == 200:
        content = page.content

    else:
        return "Error1", "Webseite nicht erreichbar", [page.status_code]

    WebDocument = BeautifulSoup(content, 'html.parser')

    try:
        title = WebDocument.find("meta", property="og:title").attrs['content'].rsplit('-', 2)[0]
        descr = WebDocument.find("meta", property="og:description").attrs['content']
    	
        descr = descr.split(" / ")

        counts = []
        for div in WebDocument.find_all("span", {'class': "danceRating"}):
            count = 0
            for element in div:
                if "star" in str(element):
                    if "red" in str(element):
                        count += 1
        
            tempcount = str(count) + "/5"
            counts.append(tempcount)

        return title, descr, counts
        
    except:
        return "Error2", "Song nicht in Datenbank", []



async def recordAudio():

    mydata = sd.rec(int(samplerate * duration), samplerate=samplerate,
                channels=2, blocking=True)
    sf.write(filename, mydata, samplerate)

async def recognize_song():

    shazam = Shazam()
    out = await shazam.recognize_song(filename)

    try:
        information = [out['track']['title'], out['track']['subtitle']]
        return information

    except KeyError:
    	return []



async def RecordMain():

    ChangeInfoText("Aufnahme", 3)
    await recordAudio()
    information = await recognize_song()

    if information == []:
        ChangeInfoText("Kein Song gefunden!", 0)
        return

    InputMain(information)


def InputMain(information):

    ChangeInfoText("Analysiere", 3)
    title, artist = FormatData(information)
    song, dances, rating = GetDances(artist, title)

    if song == "Error1":
        ChangeInfoText("Datenbank nicht erreichbar!\n mit Status-Code: {}".format(rating[0]), 0)
        return
        
    elif song == "Error2":
        ChangeInfoText("Song nicht in Datenbank enthalten!", 0)
        return

    danceString = FormatDances(dances, rating)
    title, artist = ReformatData(information)
    
    BackGroundCanvas.itemconfigure(WelcomeText, text="Fertig")
    ChangeInfoText(" ", 0)
    result = BackGroundCanvas.create_text(225, 200, font=('calibri', 15, 'bold'), justify='center', text="Zu {} von {}\n kann\n {} getanzt werden".format(title, artist, danceString))



recordBtt = Button(window, text="Aufnahme starten", command=handleRecordBtt)
recordBtt.place(height=25, x=220, y=95)

InButton = Button(window, text="Eingabe", command=handleInputBtt)
InButton.place(height=25, x=100, y=95)

BackGroundCanvas.pack(expand=True)

window.mainloop()

