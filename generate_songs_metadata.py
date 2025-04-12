import pandas as pd
import numpy as np
import uuid

# Predefine song metadata fields
titles = [f"Song {i}" for i in range(1, 101)]
artists = [f"Artist {i}" for i in range(1, 21)]
genres = ['Pop', 'Rock', 'Jazz', 'Hip-Hop', 'Classical']
moods = ['Happy', 'Sad', 'Energetic', 'Chill']

# Generate songs metadata using list comprehension
songs_metadata = [
    {
        'song_id': str(uuid.uuid4()),
        'title': title,
        'artist': np.random.choice(artists),
        'genre': np.random.choice(genres),
        'mood': np.random.choice(moods)
    }
    for title in titles
]

# Create and save DataFrame
df_songs = pd.DataFrame(songs_metadata)
df_songs.to_csv('songs_metadata.csv', index=False)
