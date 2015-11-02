#!/usr/bin/python

from datetime import datetime
import logging

import boto3
from botocore.client import ClientError

REGION_NAME = 'ap-northeast-1'
TRANSCODER_ROLE_NAME = 'Elastic_Transcoder_Default_Role'
PIPELINE_NAME = 'HLS Transcoder'
IN_BUCKET_NAME = 'boto3-transcoder-in'
OUT_BUCKET_NAME = 'boto3-transcoder-out'
INPUT_KEY = 'D0002022073_00000/sample.mp4'  # e.g. 'D0002021500_00000/sample.mp4'
TOPIC_NAME = 'test-complete'

logging.basicConfig()  # http://stackoverflow.com/questions/27411778/no-handlers-found-for-logger-main
logger = logging.getLogger(__name__)


def main():
    transcoder = boto3.client('elastictranscoder', REGION_NAME)
    s3 = boto3.resource('s3')
    iam = boto3.resource('iam')
    sns = boto3.resource('sns', REGION_NAME)

    def check_role(role_name):
        try:
            # If the IAM role doesn't exist, raises ClientError
            iam.meta.client.get_role(RoleName=role_name)
        except ClientError:
            raise

    # Check if role exists
    check_role(TRANSCODER_ROLE_NAME)
    role = iam.Role(TRANSCODER_ROLE_NAME)

    def check_bucket(bucket_name):
        try:
            # If the bucket doesn't exist, raises ClientError
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError:
            logger.error("No such bucket exists. bucket_name={}".format(bucket_name))
            raise

    # Check if bucket exists
    check_bucket(IN_BUCKET_NAME)
    check_bucket(OUT_BUCKET_NAME)

    # Get a topic arn
    # This action is idempotent, so if the requester already owns a topic with the specified name,
    # that topic's ARN is returned without creating a new topic.
    # http://boto3.readthedocs.org/en/latest/reference/services/sns.html#SNS.ServiceResource.create_topic
    topic_arn = sns.create_topic(Name=TOPIC_NAME).arn

    # Create a pipeline
    response = transcoder.create_pipeline(
        Name=PIPELINE_NAME,
        InputBucket=IN_BUCKET_NAME,
        OutputBucket=OUT_BUCKET_NAME,
        Role=role.arn,
        Notifications={
            'Progressing': '',
            'Completed': topic_arn,
            'Warning': '',
            'Error': ''
        },
    )
    print("response={}".format(response))
    pipeline_id = response['Pipeline']['Id']

    # Create a job
    job = transcoder.create_job(
        PipelineId=pipeline_id,
        Input={
            'Key': INPUT_KEY,
            'FrameRate': 'auto',
            'Resolution': 'auto',
            'AspectRatio': 'auto',
            'Interlaced': 'auto',
            'Container': 'auto',
        },
        Outputs=[
            {
                'Key': 'HLS/1M/{}'.format('.'.join(INPUT_KEY.split('.')[:-1])),
                'PresetId': '1351620000001-200030',  # System preset: HLS 1M
                'SegmentDuration': '10',
            },
        ],
    )
    print("start time={}".format(datetime.now().strftime("%H:%M:%S.%f")[:-3]))
    print("job={}".format(job))


if __name__ == '__main__':
    main()
