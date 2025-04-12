from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, hour, row_number, to_timestamp
from pyspark.sql.window import Window
import os

# Set HADOOP_HOME (ensure winutils.exe exists at C:\hadoop\bin\winutils.exe)
os.environ['HADOOP_HOME'] = "C:/hadoop"

spark = SparkSession.builder \
    .appName("MusicAnalytics") \
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
    .getOrCreate()

# Load datasets
logs = spark.read.option("header", True).option("inferSchema", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).option("inferSchema", True).csv("songs_metadata.csv")

logs = logs.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Enrich logs
enriched = logs.join(songs, on="song_id")
enriched.coalesce(1).write.mode("overwrite").csv("output/enriched_logs/")
print("✅ Enriched logs written.")

# 1. User's Favorite Genre
user_genre_counts = enriched.groupBy("user_id", "genre").count()
window_genre = Window.partitionBy("user_id").orderBy(desc("count"))
fav_genre = user_genre_counts.withColumn("rank", row_number().over(window_genre)).filter("rank = 1")
fav_genre.coalesce(1).write.mode("overwrite").csv("output/user_favorite_genres/")
print("✅ User favorite genres written.")

# 2. Average Listen Time per Song
avg_listen = logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_listen_time"))
avg_listen.coalesce(1).write.mode("overwrite").csv("output/avg_listen_time_per_song/")
print("✅ Average listen time written.")

# 3. Top 10 Songs This Week
week_start, week_end = "2025-03-20", "2025-03-27"
top_songs = logs.filter((col("timestamp") >= week_start) & (col("timestamp") <= week_end)) \
                .groupBy("song_id").count().orderBy(desc("count")).limit(10)
top_songs.coalesce(1).write.mode("overwrite").csv("output/top_songs_this_week/")
print("✅ Top songs this week written.")

# 4. Recommend Happy Songs to Sad Listeners
sad_listeners = enriched.filter(col("genre") == "Sad") \
    .groupBy("user_id", "genre").count() \
    .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(desc("count")))) \
    .filter("rank = 1 AND genre = 'Sad'") \
    .select("user_id")

happy_songs = songs.filter(col("mood") == "Happy").select("song_id", "title")
already_played = logs.select("user_id", "song_id").distinct()

recommend = sad_listeners.crossJoin(happy_songs) \
    .join(already_played, ["user_id", "song_id"], "left_anti") \
    .limit(3)

recommend.coalesce(1).write.mode("overwrite").csv("output/happy_recommendations/")
print("✅ Happy song recommendations written.")

# 5. Genre Loyalty Score
total_plays = enriched.groupBy("user_id").count()
top_genre_plays = user_genre_counts.withColumn("rank", row_number().over(window_genre)) \
    .filter("rank = 1") \
    .select("user_id", col("count").alias("top_genre_count"))

loyalty = total_plays.join(top_genre_plays, "user_id") \
    .withColumn("loyalty_score", col("top_genre_count") / col("count")) \
    .filter(col("loyalty_score") > 0.8)

loyalty.coalesce(1).write.mode("overwrite").csv("output/genre_loyalty_scores/")
print("✅ Genre loyalty scores written.")

# 6. Night Owl Users
night_users = logs.withColumn("hour", hour(col("timestamp"))) \
    .filter((col("hour") >= 0) & (col("hour") <= 5)) \
    .groupBy("user_id").count()

night_users.coalesce(1).write.mode("overwrite").csv("output/night_owl_users/")
print("✅ Night owl users written.")
