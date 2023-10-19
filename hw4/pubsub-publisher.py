#!/usr/bin/python3
import argparse
import google.cloud.pubsub as pubsub

def publish_pub_sub(message, project, topic):
    project_id = project
    topic_id = topic
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data = message.encode('utf-8')
    print('Publishing **', data, '** data on ', topic_path)
    future = publisher.publish(topic_path, data)
    future.result()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project", help="Project to use", type=str, default="cloudcomputingcourse-380619")
    parser.add_argument("-t", "--topic", help="Topic to use", type=str, default="mytopic")
    parser.add_argument("-m", "--message", help="Message to publish", type=str, default="Just a message")
    args = parser.parse_args()
    publish_pub_sub(args.message, args.project, args.topic)

if __name__ == "__main__":
    main()
