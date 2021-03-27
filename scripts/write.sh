# !/bin/bash
cat /dev/null > $(pwd)/log/t0.txt
for i in {1..1000000}
do
   echo "Welcome $i times" >> $(pwd)/../mount/t0.txt
done