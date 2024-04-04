import cv2
import sys
import json

video_path = sys.argv[1]
json_path = f"{video_path}.json"
with open(json_path, "r") as f:
    json_data = json.loads(f.read())


def create_ffmpeg_filter(annotations, frame_rate, time_base):
    filters = []

    w, h = annotations["width"], annotations["height"]

    for frame_number, frame_annotations in enumerate(annotations['frames']):
        for annotation in frame_annotations["results"]["yolov5m"]:
            xmin, ymin, xmax, ymax = annotation["xmin"], annotation["ymin"], annotation["xmax"], annotation["ymax"]

            x = int(xmin * w)
            w = int((xmax - xmin) * w)
            y = int(ymin * h)
            h = int((ymax - ymin) * h)

            class_name = annotation['name']
            box_filter = f"drawbox=x={x}:y={y}:w={w}:h={h}:color=red@0.5:enable='eq(n\,{frame_number})'"
            text_filter = f"drawtext=text='{class_name}':x={x}:y={y - 10}:fontcolor=white:fontsize=24:enable='eq(n\,{frame_number})'"
            filters.append(box_filter)
            filters.append(text_filter)
    
    return ','.join(filters)


print(create_ffmpeg_filter(json_data, 24, 12288))
