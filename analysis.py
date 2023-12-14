import numpy as np
import pandas as pd
import json
from pathlib import Path

data_path = Path("/home/charlesdietzel/Documents/Repos/spotify-data-analysis/data/")
library_file = data_path / Path("YourLibrary.json")
playlist_file = data_path / Path("Playlist1.json")
output_file = data_path / Path("output.xslx")
num_streaming_files = 2
selected_playlist = "GOAT songs redux"
msThreshold = 15000

library_data = []
playlist_data = []
streaming_data = []
streaming_files = []

for i in range(num_streaming_files):
    streaming_files.append(data_path / Path("StreamingHistory" + str(i) + ".json"))

# with open(library_file) as f:
#     data = json.load(f)
#     data = data["tracks"]
# library_data.extend(json.load(f))

with open(playlist_file) as f:
    data = json.load(f)
    data = data["playlists"]
    for playlist in data:
        if playlist["name"] == selected_playlist:
            items = playlist["items"]
            for item in items:
                track = item["track"]
                playlist_data.append(track)
            break

for file in streaming_files:
    with open(file) as f:
        streaming_data.extend(json.load(f))

playlist_df = pd.DataFrame(playlist_data)
del playlist_data
streaming_df = pd.DataFrame(streaming_data)
del streaming_data
del (
    data,
    data_path,
    f,
    file,
    i,
    item,
    items,
    library_data,
    library_file,
    num_streaming_files,
    playlist,
    playlist_file,
    selected_playlist,
    streaming_files,
    track,
)


df = pd.merge(
    streaming_df,
    playlist_df,
    how="inner",
    left_on=["trackName", "artistName"],
    right_on=["trackName", "artistName"],
)
df["endTime"] = pd.to_datetime(df["endTime"])
df = df.drop("albumName", axis=1)
df = df.drop("trackUri", axis=1)
df = df[df["endTime"] > "2023-01-01T00:00:00.000"]
counts = df.groupby(by=["artistName", "trackName"], as_index=False).size()
ratios = (
    df.set_index(["artistName", "trackName"])["msPlayed"]
    .rename(">= " + str(msThreshold))
    .ge(msThreshold)
    .groupby(level=[0, 1])
    .value_counts(True)
    .unstack(fill_value=0)
    .reset_index()
)
output = pd.merge(
    ratios,
    counts,
    how="left",
    left_on=["artistName", "trackName"],
    right_on=["artistName", "trackName"],
)
output.to_excel(output_file)
