# Harmonixia TUI test client

This is a small Python curses client for manual testing against a running
Harmonixia server. It prompts for the server URL when one is not supplied, then
reads the username and password from stdin before opening the TUI.

Requirements on the Linux machine running the client:

- `python3`
- `ffmpeg`
- `aplay` from ALSA utilities

Run it from the repository root:

```sh
python3 tests/client/harmonixia_tui.py http://127.0.0.1:3000
```

Controls:

- `Enter`: select artist, album, or track
- selecting an album starts playback of its first track
- `b`: go back
- `n`: next track during playback
- `p`: previous track during playback
- `q`: quit

Playback uses:

```text
GET /api/v1/media/track/{track_id}/transcode/{profile}
```

The returned AAC stream is decoded by local `ffmpeg` to 44.1 kHz stereo PCM and
piped into `aplay`, so output goes through ALSA. Use `--alsa-device` to pick a
specific ALSA device and `--profile mobile|standard|high` to choose the server
transcode profile.
