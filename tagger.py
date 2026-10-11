import os
import acoustid
import mutagen
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3

ACOUSTID_API_KEY = "YOUR_ACOUSTID_API_KEY"
MUSIC_DIRECTORY = os.path.expanduser("~/storage/shared/Music")

def clean_and_tag_music(directory):
    if not os.path.exists(directory):
        print(f"Directory not found: {directory}")
        return
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp3', '.m4a', '.flac')):
                file_path = os.path.join(root, file)
                try:
                    for score, recording_id, title, artist in acoustid.match(ACOUSTID_API_KEY, file_path):
                        if score > 0.85 and title and artist:
                            audio = EasyID3(file_path)
                            audio['title'] = title
                            audio['artist'] = artist
                            audio.save()
                            print(f"Updated: {title} by {artist} (Score: {score:.2f})")
                            break
                except Exception as e:
                    print(f"Error processing {file}: {e}")

if __name__ == '__main__':
    clean_and_tag_music(MUSIC_DIRECTORY)
