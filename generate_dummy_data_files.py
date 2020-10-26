import os
from datetime import datetime

# iterate through locations:
for location in os.listdir("/home/base-dir"):

    # treat only directories as locations:
    if os.path.isdir(f"/home/base-dir/{location}"):
        
        # set timestamp / directory structure variables:
        timestamp = datetime.now()
        YYYY = datetime.strftime(timestamp, "%Y")
        MM = datetime.strftime(timestamp, "%m")
        DD = datetime.strftime(timestamp, "%d")
        HH = datetime.strftime(timestamp, "%H")
        mm = datetime.strftime(timestamp, "%M")
        
        # check / update directory structure:
        if not os.path.isdir(f"/home/base-dir/{location}/{YYYY}"):
            os.mkdir(f"/home/base-dir/{location}/{YYYY}")
        if not os.path.isdir(f"/home/base-dir/{location}/{YYYY}/{MM}"):
            os.mkdir(f"/home/base-dir/{location}/{YYYY}/{MM}")
        if not os.path.isdir(f"/home/base-dir/{location}/{YYYY}/{MM}/{DD}"):
            os.mkdir(f"/home/base-dir/{location}/{YYYY}/{MM}/{DD}")
        if not os.path.isdir(f"/home/base-dir/{location}/{YYYY}/{MM}/{DD}"\
                             f"/{HH}"):
            os.mkdir(f"/home/base-dir/{location}/{YYYY}/{MM}/{DD}/{HH}")
        
        # set filepath / filename variables:
        filepath = f"/home/base-dir/{location}/{YYYY}/{MM}/{DD}/{HH}"
        filename = f"{mm}-datafile.dat"
        
        # write datafile to local directory:
        with open(f"{filepath}/{filename}", "x") as f:
            f.write(f"dummy data generated at "\
                    f"{datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')}")

        # output (print to terminal / write to log):
        print (f"{datetime.strftime(timestamp, '%b %d %H:%M:%S')} "\
               f"location: {location} "\
               f"directory: {filepath} "\
               f"filename: {filename}"
               )
