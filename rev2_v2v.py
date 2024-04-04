import cv2
import sys
import json

video_path = sys.argv[1]
json_path = f"{video_path}.json"
fixed_path = (
    "/".join(video_path.split("/")[:-1]) + "/fixed-" + video_path.split("/")[-1]
)
ffprobe_path = fixed_path + ".json"

clip_start = int(sys.argv[2])
clip_end = int(sys.argv[3])

with open(json_path, "r") as f:
    json_data = json.loads(f.read())
with open(ffprobe_path, "r") as f:
    ffprobe_data = json.loads(f.read())

sorted_ffprobe_frames = sorted(ffprobe_data["frames"], key=lambda x: x["pts"])

assert len(json_data["frames"]) == len(sorted_ffprobe_frames)

w, h = json_data["width"], json_data["height"]
assert (
    w == ffprobe_data["streams"][0]["width"]
    and h == ffprobe_data["streams"][0]["height"]
)

w = 1280
h = 720

gops = []
current_gop = {"start": 0, "has_anototation": False, "filter": {}}
start_of_annoted_span = None
for frame_number, frame in enumerate(json_data["frames"]):
    ffprobe_frame = sorted_ffprobe_frames[frame_number]
    if ffprobe_frame["pict_type"] == "I" and current_gop["start"] != frame_number:
        current_gop["end"] = frame_number - 1
        gops.append(current_gop)
        if not current_gop["has_anototation"]:
            start_of_annoted_span = None

        current_gop = {"start": frame_number, "has_anototation": False, "filter": {}}

    for annotation in frame["results"]["yolov5m"]:
        if frame_number not in current_gop["filter"]:
            current_gop["filter"][frame_number] = []

        if not current_gop["has_anototation"]:
            current_gop["has_anototation"] = True
            if start_of_annoted_span is None:
                start_of_annoted_span = current_gop["start"]

        xmin, ymin, xmax, ymax = (
            annotation["xmin"],
            annotation["ymin"],
            annotation["xmax"],
            annotation["ymax"],
        )

        w = 1280
        h = 720

        x = int(xmin * w)
        w = int((xmax - xmin) * w)
        y = int(ymin * h)
        h = int((ymax - ymin) * h)

        class_name = annotation["name"]
        box_filter = (
            f"drawbox=x={x}:y={y}:w={w}:h={h}:color=red@0.5:enable='eq(n\,XXX)'"
        )
        text_filter = f"drawtext=text='{class_name}':x={x}:y={y - 10}:fontcolor=white:fontsize=24:enable='eq(n\,XXX)'"
        current_gop["filter"][frame_number].append(box_filter)
        current_gop["filter"][frame_number].append(text_filter)
        break


if current_gop["start"] != len(json_data["frames"]) - 1:
    current_gop["end"] = len(json_data["frames"]) - 1
    gops.append(current_gop)

assert gops[-1]["end"] == len(json_data["frames"]) - 1


for j in range(len(gops)):
    for i in range(len(gops) - 1):
        if (
            gops[i]["has_anototation"] == gops[i + 1]["has_anototation"]
            and len(gops[i]["filter"]) + len(gops[i + 1]["filter"]) < 250
        ):
            gops[i]["end"] = gops[i + 1]["end"]
            gops[i]["filter"] = {**gops[i]["filter"], **gops[i + 1]["filter"]}
            gops.pop(i + 1)
            break

gops = [g for g in gops if g["end"] >= clip_start and g["start"] <= clip_end]
gops[0]["start"] = clip_start
gops[0]["filter"] = {k: v for k, v in gops[0]["filter"].items() if k >= clip_start}
gops[-1]["end"] = clip_end
gops[-1]["filter"] = {k: v for k, v in gops[-1]["filter"].items() if k <= clip_end}

# print(json.dumps(gops, indent=4))
# 0/0

spec = {
    "iter": {"start": [0, 1], "end": [clip_end - clip_start, 24], "step": [1, 24]},
    "render": {"MatchT": []},
    "output": "/scratch/output10.mp4",
}

fixed = False
for gop in gops:
    filters = []
    if not gop["has_anototation"]:
        fixed = True

    for frame_number, frame_filters in gop["filter"].items():
        if not fixed:
            filters.extend(
                [
                    f.replace("XXX", str(frame_number - clip_start))
                    for f in frame_filters
                ]
            )
        else:
            filters.extend([f.replace("XXX", str(frame_number)) for f in frame_filters])

    if filters:
        spec["render"]["MatchT"].append(
            [
                {
                    "start": [gop["start"] - clip_start, 24],
                    "end": [gop["end"] - clip_start + 1, 24],
                    "step": [1, 24],
                },
                {
                    "F2fFunction": {
                        "func": "Filter",
                        "sources": [
                            {
                                "SourceFunction": {
                                    "func": "ReadFrame",
                                    "source": fixed_path,
                                    "t": {"Add": ["T", [gop["start"], 24]]},
                                    "args": [],
                                }
                            }
                        ],
                        "args": [{"ConstStr": ",".join(filters)}],
                    }
                },
            ]
        )
    else:
        spec["render"]["MatchT"].append(
            [
                {
                    "start": [gop["start"] - clip_start, 24],
                    "end": [gop["end"] - clip_start + 1, 24],
                    "step": [1, 24],
                },
                {
                    "SourceFunction": {
                        "func": "ReadFrame",
                        "source": fixed_path,
                        "t": {"Add": ["T", [gop["start"], 24]]},
                        "args": [],
                    }
                },
            ]
        )

print(json.dumps(spec, indent=2))
