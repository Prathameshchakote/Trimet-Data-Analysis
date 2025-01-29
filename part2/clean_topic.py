""" This program clean the topic. Here We are clearing and cleaning the topic and removing
 all the messages from topic.  """

from google.cloud import pubsub_v1

project_id = "cloud-kumari-kumari-400603"
topic_name = "test"
subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_name)
subscriptions = subscriber.list_subscriptions(project=f"projects/{project_id}")

for subscription in subscriptions:
    if topic_path in subscription.topic:
        subscription_path = subscription.name
        subscriber.delete_subscription(request={"subscription": subscription_path})
        print(f"All messages in subscription '{subscription_path}' have been deleted.")

print(f"All messages in topic '{topic_path}' have been deleted.")

""" This program creates the subscriptions named as test-sub-{current_date} """

import datetime

subscription_name = "test-1"
subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_name)
subscription = subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})

print(f"Subscription '{subscription_path}' has been created for topic '{topic_path}'.")
