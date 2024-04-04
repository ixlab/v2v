set -e
export TIMEFORMAT=%R

echo "tos"
for i in {1..5}
do
  time python3 rev2_cv2.py rev2/tearsofsteel_4k_anot.mp4 5 2>&1
done

echo

echo "tos-1m"
python3 rev2_cv2.py rev2/tearsofsteel_4k_anot.mp4 60
for i in {1..5}
do
  time python3 rev2_cv2.py rev2/tearsofsteel_4k_anot.mp4 60
done

echo
echo "kabr"
python3 rev2_cv2.py rev2/DJI_0009.MP4 5
python3 rev2_cv2.py rev2/DJI_0011.MP4 5
python3 rev2_cv2.py rev2/DJI_0012.MP4 5
python3 rev2_cv2.py rev2/DJI_0014.MP4 5
for i in {1..5}
do
  time python3 rev2_cv2.py rev2/DJI_0009.MP4 5
  time python3 rev2_cv2.py rev2/DJI_0011.MP4 5
  time python3 rev2_cv2.py rev2/DJI_0012.MP4 5
  time python3 rev2_cv2.py rev2/DJI_0014.MP4 5
done

echo
echo "kabr-1m"
python3 rev2_cv2.py rev2/DJI_0009.MP4 60
python3 rev2_cv2.py rev2/DJI_0011.MP4 60
python3 rev2_cv2.py rev2/DJI_0012.MP4 60
python3 rev2_cv2.py rev2/DJI_0014.MP4 60
for i in {1..5}
do
  time python3 rev2_cv2.py rev2/DJI_0009.MP4 60
  time python3 rev2_cv2.py rev2/DJI_0011.MP4 60
  time python3 rev2_cv2.py rev2/DJI_0012.MP4 60
  time python3 rev2_cv2.py rev2/DJI_0014.MP4 60
done

