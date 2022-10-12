import redis

#db_endpoint = "redis-18296.c3.eu-west-1-2.ec2.cloud.redislabs.com"
db_endpoint = "redis-15651.c81.us-east-1-2.ec2.cloud.redislabs.com"
db_port = "15651"
db_no = "0"
db_user = "default"
db_auth = "dsnd7ACtTEN6GuNGGRhqyAdN3pLhXLQ3"

db_backup_endpoint = "redis-15651.c81.us-east-1-2.ec2.cloud.redislabs.com"
db_backup_port = "15651"
db_backup_auth = "dsnd7ACtTEN6GuNGGRhqyAdN3pLhXLQ3"




def connect_to_db():
    r = redis.StrictRedis.from_url(f"redis://{db_user}:{db_auth}@{db_endpoint}:{db_port}/{db_no}")

    try:
        r.ping()
        print("Successfully connected to redis")
    except (redis.exceptions.ConnectionError, ConnectionRefusedError):
        r = redis.StrictRedis.from_url(f"redis://{db_user}:{db_backup_auth}@{db_backup_endpoint}:{db_backup_port}/{db_no}")
        print("Redis connection error!")

    return r
