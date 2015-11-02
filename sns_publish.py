#!/usr/bin/python

import boto3
import logging

TOPIC_NAME = 'test-complete'

logging.basicConfig()  # http://stackoverflow.com/questions/27411778/no-handlers-found-for-logger-main
logger = logging.getLogger(__name__)


def main():
    sns = boto3.resource('sns', 'ap-northeast-1')

    # Get a topic arn
    # This action is idempotent, so if the requester already owns a topic with the specified name,
    # that topic's ARN is returned without creating a new topic.
    # http://boto3.readthedocs.org/en/latest/reference/services/sns.html#SNS.ServiceResource.create_topic
    topic_arn = sns.create_topic(Name=TOPIC_NAME).arn
    print("topic_arn={}".format(topic_arn))

    # Publish a message
    response = sns.Topic(topic_arn).publish(
        Subject="test",
        Message="This is a message.",
    )
    print("response={}".format(response))


if __name__ == '__main__':
    main()
