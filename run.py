import subprocess
import time

# Run ff.py
print("Running eccn.py...")
subprocess.run(["python", "eccn_Grainger.py"])

# Wait for 5 seconds (optional, if you need to delay between scripts)
time.sleep(1)

# Run gg.py
print("Running eccn.py...")
subprocess.run(["python", "eccn.py"])
time.sleep(1)

# Run gg.py
print("Running eccn.py...")
subprocess.run(["python", "eccn_MCM_polo.py"])

print("Both scripts have finished running.")
