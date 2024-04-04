import cv2
import sys
import json

video_path = sys.argv[1]
seconds = int(sys.argv[2])

json_path = f"{video_path}.json"
with open(json_path, "r") as f:
    json_data = json.loads(f.read())

def process_video(video_path, annotations, output_path):
    # Open the video
    cap = cv2.VideoCapture(video_path)
    cap.set(cv2.CAP_PROP_POS_FRAMES, 60*24-1)
    assert cap.get(3) > cap.get(4)

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, 24.0, (1280, 720))

    w, h = 1280, 720

    frame_n = 0
    while cap.isOpened():
        if frame_n >= seconds*24:
            break

        ret, frame = cap.read()
        if not ret:
            break
        
        frame = cv2.resize(frame, (w, h))

        # Draw bounding boxes for the current frame
        for annotation in annotations["frames"][frame_n]["results"]["yolov5m"]:
            xmin, ymin, xmax, ymax = annotation["xmin"], annotation["ymin"], annotation["xmax"], annotation["ymax"]
            name = annotation["name"]

            xmin = int(xmin * w)
            xmax = int(xmax * w)
            ymin = int(ymin * h)
            ymax = int(ymax * h)

            # cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 2)
            cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), (0, 255, 0), 2)
            cv2.putText(frame, name, (xmin, ymin), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2, cv2.LINE_AA)
            break

        # Write the frame
        out.write(frame)

        frame_n += 1
        # print(".", end="", flush=True)

        # if frame_n > 1000:
        #     break

    # Release everything
    cap.release()
    out.release()
    cv2.destroyAllWindows()

process_video(video_path, json_data, f"{video_path}.tmp.mkv")
