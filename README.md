### The code for V2V: Efficiently Synthesizing Video Results for Video Queries

This repo has been sanitized to remove references to unpublished work.

## Install

```
pushd ~

# ffmpeg
rm -rf ffmpeg /usr/local/bin/ffmpeg /usr/local/bin/ffprobe
curl https://ffmpeg.org/releases/ffmpeg-snapshot.tar.bz2 | bzip2 -d | tar -x
pushd ffmpeg
./configure --extra-cflags='-march=native' --enable-nonfree --enable-gpl --enable-libx264 --enable-libfdk-aac --enable-libvpx --enable-libfreetype --enable-libharfbuzz --enable-libfontconfig
make -j 16
sudo make install
popd
rm -rf ffmpeg

popd
```

## Run

```bash
cargo build --release
```

Then use the `./target/release/v2v` binary. The CLI is self-documenting so you can use `--help` on anything and it will guide you through the usage.
Example of tos benchmarking is here:

```bash
cargo run -- add-video --datastore datastore.json --name tos --video-path videos/clip.mp4 --ffprobe-json videos/clip.ffprobe.json
cargo run -- add-video --datastore datastore.json --name DJI_0009 --video-path videos/DJI_0009.mp4 --ffprobe-json videos/DJI_0009.ffprobe.json
cargo run -- add-video --datastore datastore.json --name DJI_0011 --video-path videos/DJI_0011.mp4 --ffprobe-json videos/DJI_0011.ffprobe.json
cargo run -- add-video --datastore datastore.json --name DJI_0012 --video-path videos/DJI_0012.mp4 --ffprobe-json videos/DJI_0012.ffprobe.json
cargo run -- add-video --datastore datastore.json --name DJI_0014 --video-path videos/DJI_0014.mp4 --ffprobe-json videos/DJI_0014.ffprobe.json

export RUST_LOG=debug
cargo run --release -- benchmark --datastore datastore.json --dataset tos
# see results in datalog.json
```

## Preprocess TOS to include frame metadata for frame-exact verification


```bash
ffmpeg -i tearsofsteel_4k.mov -vf "drawtext=text='Frame=%{n}':x=300:y=200:fontcolor=white:fontsize=35:box=1:boxcolor=white@0.5,drawtext=text='pts_rational=%{expr\\:n*12288}':x=300:y=250:fontcolor=white:fontsize=35:box=1:boxcolor=white@0.5,drawtext=text='PTS_HMS=%{pts \\: hms}':x=300:y=300:fontcolor=white:fontsize=35:box=1:boxcolor=white@0.5,drawtext=text='PTS_FLT=%{pts \\: flt}':x=300:y=350:fontcolor=white:fontsize=35:box=1:boxcolor=white@0.5" -c:a copy -c:v libx264 -crf 26 -preset ultrafast -vsync 0 -enc_time_base -1 tearsofsteel_4k_anot.mp4

ffprobe -i clip.mp4 -show_frames -show_streams -count_frames -print_format json -select_streams v > clip.ffprobe.json

cat clip.ffprobe.json | jq '.frames[] | select(.key_frame == 1) | .pkt_pts' | awk '{printf "clip0009_key.insert(Rational64::new(%d, 12288));\n", $1}'
```

