#!/usr/bin/python3

from concurrent.futures import TimeoutError
from google.cloud import pubsub
from google.cloud import pubsub_v1
import argparse

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

def consume_messages(project_id, sub_id, timeout):
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, sub_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            if timeout != 0:
                streaming_pull_future.result(timeout=timeout)
            else:
                streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--timeout", help="Timeout for message pull requests", type=int, default=0)
    parser.add_argument("-p", "--project", help="Project to use", type=str, default="cloudcomputingcourse-380619")
    parser.add_argument("-s", "--subscription", help="Subscription to use", type=str, default="mytopic-mysub")
    args = parser.parse_args()
    consume_messages(args.project, args.subscription, args.timeout) 

if __name__ == "__main__":
    main()
