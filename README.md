# Music Streaming Analytics with PySpark

This project analyzes user music listening behavior using PySpark. It reads in user listening logs and song metadata, then performs various aggregations and outputs the results into organized folders.

## 📁 Input Files

Place the following CSV files in the project root:

- `listening_logs.csv` – contains user listening history:
  ```
  user_id,song_id,timestamp,duration_sec
  user_14,b4da6eff-3a3f-4c02-bd20-f2f0a214f281,2025-03-24 01:58:00,88
  ```

- `songs_metadata.csv` – contains song details:
  ```
  song_id,title,artist,genre,mood
  b4da6eff-3a3f-4c02-bd20-f2f0a214f281,Song A,Artist X,Pop,Happy
  ```

## ⚙️ Tasks Performed

### 1. User's Favorite Genre
Identifies the most played genre per user.

**Sample Output:**
```
user_id     genre     count
user_1      Pop       15
user_2      Rock      9
```

---

### 2. Average Listen Time per Song
Computes the average duration each song is listened to.

**Sample Output:**
```
song_id                                avg_listen_time
b4da6eff-3a3f-4c02-bd20-f2f0a214f281   212.4
e072c1e1-a53d-42f3-8b95-be85ffb81050   189.2
```

---

### 3. Top 10 Songs This Week
Filters and lists top 10 played songs between 2025-03-20 to 2025-03-27.

**Sample Output:**
```
song_id                                count
3d281cb4-6350-4b25-87aa-caac36d0cc07   34
a372ed0a-e2b5-49fd-838c-7797e5cf3af4   28
```

---

### 4. Happy Song Recommendations
Recommends "Happy" mood songs to users who mostly listen to "Sad" songs.

**Sample Output:**
```
user_id     song_id                                title
user_9      80af7ca0-abc3-4f2e-a8d2-45d1a1f2e3f0    Song Happy 1
user_9      34be8f07-e1fa-42e6-9170-52a7d32c8992    Song Happy 2
```

---

### 5. Genre Loyalty Score
Computes the proportion of plays in their favorite genre (only >0.8 are shown).

**Sample Output:**
```
user_id     top_genre_count     count     loyalty_score
user_3      28                  30        0.933
user_7      19                  22        0.863
```

---

### 6. Night Owl Users
Extracts users active between 12 AM and 5 AM.

**Sample Output:**
```
user_id     count
user_14     4
user_9      3
```

---

## 📂 Output Structure

Each task's results are written to:

- `output/enriched_logs/`
- `output/user_favorite_genres/`
- `output/avg_listen_time_per_song/`
- `output/top_songs_this_week/`
- `output/happy_recommendations/`
- `output/genre_loyalty_scores/`
- `output/night_owl_users/`

---

## 🖥️ Requirements

- Python 3.7+
- PySpark
- Hadoop (for `winutils.exe` on Windows)

## 🚀 Run the Script

```bash
python music_analysis.py
```
