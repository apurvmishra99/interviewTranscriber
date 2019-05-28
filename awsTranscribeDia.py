import botocore
import os
import json
import boto3
import time
filepath = "/home/apurv/audio_wav/"
output_filepath = "/home/apurv/Transcripts/"


bucketName = "interviewaudiostore"


def upload_file_to_s3(audio_file_name):

    Key = filepath + audio_file_name
    outPutname = audio_file_name

    s3 = boto3.client('s3')
    s3.upload_file(Key, bucketName, outPutname)


def download_file_from_s3(audio_file_name):

    s3 = boto3.resource('s3')

    Key = outPutname = audio_file_name.split('.')[0] + '.json'

    try:
        s3.Bucket(bucketName).download_file(Key, outPutname)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def delete_file_from_s3(audio_file_name):

    s3 = boto3.resource('s3')
    s3.Object(bucketName, audio_file_name).delete()
    s3.Object(bucketName, audio_file_name.split('.')[0] + '.json').delete()


def transcribe(audio_file_name):

    transcripts = ''

    upload_file_to_s3(audio_file_name)

    transcribe = boto3.client('transcribe', region_name='eu-west-1')
    job_name = audio_file_name.split('.')[0]
    job_uri = "https://s3.eu-west-1.amazonaws.com/" + \
        bucketName + "/" + audio_file_name
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat='wav',
        LanguageCode='en-IN',
        Settings={'MaxSpeakerLabels': 2, 'ShowSpeakerLabels': True},
        OutputBucketName=bucketName
    )
    while True:
        status = transcribe.get_transcription_job(
            TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        time.sleep(5)

    download_file_from_s3(audio_file_name)

    transcribe.delete_transcription_job(TranscriptionJobName=job_name)

    # delete_file_from_s3(audio_file_name)

    with open(audio_file_name.split('.')[0] + '.json') as f:
        text = json.load(f)

    for i in text['results']['transcripts']:
        transcripts += i['transcript']

    #os.remove(audio_file_name.split('.')[0] + '.json')

    return transcripts


def write_transcripts(transcript_filename, transcript):
    f = open(output_filepath + transcript_filename, "w+")
    f.write(transcript)
    f.close()


if __name__ == "__main__":
    for audio_file_name in os.listdir(filepath):
        transcript = transcribe(audio_file_name)
        transcript_filename = audio_file_name.split('.')[0] + '.txt'
        write_transcripts(transcript_filename, transcript)
