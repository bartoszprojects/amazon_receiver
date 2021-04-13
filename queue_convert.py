import glob
import os
from PIL import Image
import boto3
import json
import asyncio

Image.MAX_IMAGE_PIXELS = None

# This line loads credentials of S3 Storage from local json file to avoid public access to keys
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

credentials = json.load(open(os.path.join(__location__, 'credentials.json')))


path = os.path.dirname(__file__)
BUCKET = 'blecho'

s3_resource = boto3.resource('s3', region_name='us-east-2', aws_access_key_id=credentials['aws_access_key_id'],
                             aws_secret_access_key=credentials['aws_secret_access_key'])

s3_client = boto3.client('s3', region_name='us-east-2', aws_access_key_id=credentials['aws_access_key_id'],
                         aws_secret_access_key=credentials['aws_secret_access_key'])

bucket = s3_resource.Bucket('blecho')

# this function receiving IMAGES from AWS S3 STORAGE (in this example: "blecho" BUCKET)
# Every Image has individual queue: 1. Receive Image > Convert Image > Upload Image
async def getImagesFromAWS():
    with open("images.txt", "r") as pathlist:
        l = pathlist.readlines()
        for path in l:
            path = path.strip()
            print("Processing [{}]".format(path))
            objlist = list(bucket.objects.filter(Prefix=path))
            print("the length of objlist" +str(len(objlist)))
            if len(objlist):
                obj = objlist[0]
                print(f"before download file {obj.key}")
                path, filename = os.path.split(obj.key)
                bucket.download_file(obj.key, filename)
                await convertImages(filename, path)
                #delete local images
                file_without_ext = os.path.splitext(filename)[0]
                os.remove(filename)
                os.remove('{0}.jpg'.format(file_without_ext))
                os.remove('{0}.pdf'.format(file_without_ext))


# After image is downloaded to 'images' folder - it is converted at the first step to RGBA MODE
async def convertImages(filename, path):
    file_without_ext = os.path.splitext(filename)[0]
    ext = os.path.splitext(filename)[1]
    print(f"before Image.open({filename})")
    im = Image.open(filename)
    rgb_im = im.convert('RGB')
    rgb_im.save('{0}.jpg'.format(file_without_ext))
    image1 = Image.open('{0}.jpg'.format(file_without_ext))
    image1.save('{0}.pdf'.format(file_without_ext))
    local_filename = '{0}.pdf'.format(file_without_ext)
    sendImagesToAWS(local_filename, path, filename)
    im.close()
    rgb_im.close()
    image1.close()

# This function upload single image in queue to S3 STORAGE.
# !! Only if this single image is downloaded > converted > uploaded - then next image is on the function
def sendImagesToAWS(filename, path, original_file_name):
    dest_bucket = BUCKET +'/'+ path
    print(f"destination {dest_bucket}")
    #upload_file('/tmp/' + filename, '<bucket-name>', 'folder/{}'.format(filename))
    #s3.upload_file(file_path,bucket_name, '%s/%s' % (bucket_folder,dest_file_name))
    s3_resource.meta.client.upload_file(filename, BUCKET, '%s/%s' % (path, filename), ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/aws/s3'})
    #s3_client.upload_file(filename, dest_bucket, filename)
    print("Saved [{}]".format(filename))


async def run():
    await getImagesFromAWS()
    print('EVERYTHING IS DONE')


#s3_resource.meta.client.upload_file("a.pdf", BUCKET,  "a.pdf", ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/aws/s3'})
asyncio.run(run())
