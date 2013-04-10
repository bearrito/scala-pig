__author__ = 'me'

import os
import argparse
from ftplib import FTP


def download(download_directory,start_year, end_year) :
    ftp = FTP("ftp.ncdc.noaa.gov", "ftp", "foor@gmail.com")
    #ftp.login()
    ftp.retrlines("LIST")



    for i in range(start_year,end_year) :
        name = "gsod_"+ str(i) +".tar"
        dir = str(i)
        ftp.cwd("/pub/data/gsod/" + dir)
        listing = []
        ftp.retrlines("LIST", listing.append)
        words = listing[0].split(None, 8)
        filename = words[-1].lstrip()
        d = ftp.dir()

    # download the file
        local_filename = os.path.join(download_directory, name)
        lf = open(local_filename, "wb")
        ftp.retrbinary("RETR " + name, lf.write, 8*1024)
        lf.close()

if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--download_directory", help="where do you want to donwload the files")
    parser.add_argument("--start_year", type=int, help="year to start with",default = 2002)
    parser.add_argument("--end_year", type=int, help="year to end with", default = 2013)
    args = parser.parse_args()
    download(args.download_directory,args.start_year,args.end_year)

