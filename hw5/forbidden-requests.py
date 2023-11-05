from google.cloud import pubsub_v1

# listen for banned countries
subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path('ds561-trial-project', 'banned-countries-sub')

def callback(message):
  print('Received message: {}'.format(message))
  message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print('Listening for banned countries...')

with subscriber:
  try:
    streaming_pull_future.result()
  except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()