__author__ = 'me'

import os
from ftplib import FTP

ftp = FTP("ftp.ncdc.noaa.gov", "ftp", "j.barrett.strausser@gmail.com")
#ftp.login()
ftp.retrlines("LIST")

ftp.cwd("/pub/data/gsod/2002")
#ftp.cwd("subFolder") # or ftp.cwd("folderOne/subFolder")
for i in range(2003,2013) :
    name = "gsod_"+ str(i) +".tar"
    dir = str(i)
    ftp.cwd("/pub/data/gsod/" + dir)
    listing = []
    ftp.retrlines("LIST", listing.append)
    words = listing[0].split(None, 8)
    filename = words[-1].lstrip()
    d = ftp.dir()

# download the file
    local_filename = os.path.join(r"/home/me/Downloads/", name)
    lf = open(local_filename, "wb")
    ftp.retrbinary("RETR " + name, lf.write, 8*1024)
    lf.close()