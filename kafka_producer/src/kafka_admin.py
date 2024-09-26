from kafka import KafkaAdminClient
import argparse


def create_topic():
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:29092')
    topics_to_delete = ["cgm_readings"]
    admin_client.create_topics(new_topics=topics_to_delete, validate_only=False)
    admin_client.close()

def delete_topic():
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:29092')
    topics_to_delete = ["cgm_readings"]
    admin_client.delete_topics(topics_to_delete)
    admin_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Admin")
    parser.add_argument("--action", type=str, default="create", help="create|delete")
    args = parser.parse_args()
    if(args.action=="create"):
        create_topic()
    else:
        delete_topic()




