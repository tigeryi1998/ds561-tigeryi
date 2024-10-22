from google.cloud import pubsub_v1

PROJECT_ID = "feisty-gasket-398719"
TOPIC_ID = "my-topic"
SUBSCRIPTION_NAME = "my-topic-sub"

# listen for banned countries
subscriber = pubsub_v1.SubscriberClient()


subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

def callback(message):
  print('Received message: {}'.format(message.data.decode('utf-8')))
  message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print('Listening for banned countries...')

with subscriber:
  try:
    streaming_pull_future.result()
  except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()