import os
from os import path
import shutil

Source_Path = 'images/basic'
Destination = 'images/changed'

def main():
    for count, filename in enumerate(os.listdir(Source_Path)):
        dst =  "img-" + str(count) + ".tiff"
        os.rename(os.path.join(Source_Path, filename),  os.path.join(Destination, dst))

# Driver Code
if __name__ == '__main__':
    main()