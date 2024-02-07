# Data Modeling I

For Mac users:

brew install postgresql
Running Postgres
docker-compose up
To shutdown, press Ctrl+C and run:

docker-compose down
Running ETL Scripts
python create_tables.py
python etl.py
Steps
1) docker-compose up
2) python create_tables.py
3) python etl.py
Tables
Actors
Attribute	Data Type	Note
id	int	
login	text	username
url	text	link of the actor’s profile
Events
Attribute	Data Type	Note
id	text	
type	text	type of event
actor_id	int	(foreign key)
created_at	text	time event created
Repos
Attribute	Data Type	Note
id	int	
name	text	
event_id	text	(foreign key)

## H2

### H3