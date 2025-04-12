import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Load song metadata
songs_df = pd.read_csv('songs_metadata.csv')
song_ids = songs_df['song_id'].values

# Generate user IDs
user_ids = [f"user_{i}" for i in range(1, 21)]

# Generate random data
num_events = 1000
random_users = np.random.choice(user_ids, num_events)
random_songs = np.random.choice(song_ids, num_events)
random_dates = [
    datetime(2025, 3, np.random.randint(20, 28), np.random.randint(0, 24), np.random.randint(0, 60))
    for _ in range(num_events)
]
random_durations = np.random.randint(30, 301, size=num_events)

# Create the DataFrame
listening_logs = pd.DataFrame({
    'user_id': random_users,
    'song_id': random_songs,
    'timestamp': [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in random_dates],
    'duration_sec': random_durations
})

# Save to CSV
listening_logs.to_csv('listening_logs.csv', index=False)
