import click
import time
import pandas as pd
from dotenv import dotenv_values
from pymongo import MongoClient
import spacy
from textblob import TextBlob
# Load the spaCy language model
nlp = spacy.load("en_core_web_sm")


config = dotenv_values(".env")

# Connect to the local MongoDB instance
connection_string = "mongodb://localhost:27017/"
mongodb_client = MongoClient(connection_string)

# Choose or create a database
db = mongodb_client['mydatabase']

# Choose or create a collection
collection = db['headlinesV2']

@click.group()
def cli():
    pass

@click.command()
@click.option('--csv_file_path', prompt='Enter a Csv file path', help="Give csv file path")
def import_headlines(csv_file_path):
    start_time = time.time()

    # Read the CSV file into a Pandas DataFrame
    csv_file = csv_file_path
    df = pd.read_csv(csv_file)

    # Convert DataFrame to a list of dictionaries
    data = df.to_dict(orient='records')

    # Insert data into MongoDB
    collection.insert_many(data)

    mongodb_client.close()

    end_time = time.time()
    execution_time = end_time - start_time
    click.echo(f"Execution time: {execution_time:.4f} seconds")

@click.command()
def extract_entities():
    start_time = time.time()
    for row in collection.find():
            doc = nlp(row['headline_text'])
            blob = TextBlob(row['headline_text'])

            sentiment = ''
            # Get sentiment polarity (-1 to 1, where -1 is negative, 1 is positive, and 0 is neutral)
            polarity = blob.sentiment.polarity

            # Get sentiment subjectivity (0 to 1, where 0 is very objective, and 1 is very subjective)
            subjectivity = blob.sentiment.subjectivity

            # Determine sentiment based on polarity
            if polarity > 0:
                sentiment = "positive"
            elif polarity < 0:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            entities = []
            for ent in doc.ents:
                newEntities = (ent.text, ent.label_) 
                if newEntities[1] == 'PERSON' or newEntities[1] == 'ORG' or newEntities[1] == 'LOC':
                    entitygroup = ent.text
                    entitygroup = entitygroup.split(' ')
                    for entity in entitygroup:
                        entities.append({'type': ent.label_,'value':entity})
            update_data = {
                    "$set": {
                        "sentimententities": entities,
                        "sentiment":sentiment
                    }
                        }
            result = collection.update_one({"_id": row['_id']},update_data )
            print(result)

     
    end_time = time.time()
    execution_time = end_time - start_time
    click.echo(f"Execution time for Sentiment Entities: {execution_time:.4f} seconds")

@click.command()
def top100entitieswithtype():
    start_time = time.time()
    # Define the aggregation pipeline
    pipeline = [
        {
            "$unwind": "$sentimententities"  # Unwind the 'sentimententities' array
        },
        {
            "$group": {
                "_id": "$sentimententities",  # Group by the sentimententities array
                "count": {"$sum": 1}  # Count the occurrences of each entity
            }
        },
        {
            "$sort": {"count": -1}  # Sort by count in descending order
        },
        {
            "$limit": 100  # Limit to the top 100 entities
        }
    ]

    # Execute the aggregation pipeline and retrieve the top 100 entities
    result = list(collection.aggregate(pipeline))

    # Print the top 100 entities and their counts
    for item in result:
        print(f"Entity: {item['_id']}, Count: {item['count']}")

    end_time = time.time()
    execution_time = end_time - start_time
    click.echo(f"Execution time for Top 100 Entities with Type: {execution_time:.4f} seconds")

    end_time = time.time()
    execution_time = end_time - start_time
    click.echo(f"Execution time for Top 100 Entities with Type: {execution_time:.4f} seconds")

@click.command()
@click.option('--entity-name', prompt='Enter search entity type', help="search entity type")
def allheadlinesfor(entity_name):
    start_time = time.time()
    result = collection.find({
    "sentimententities": {
        "$elemMatch": {
            "type": "ORG"
        }
    }
})
    for row in result:
        print(row)

    end_time = time.time()
    execution_time = end_time - start_time
    click.echo(f"Execution time for Top 100 Entities with Type: {execution_time:.4f} seconds")


cli.add_command(import_headlines)
cli.add_command(extract_entities)
cli.add_command(extract_entities)
cli.add_command(top100entitieswithtype)
cli.add_command(allheadlinesfor)

if __name__ == "__main__":
    cli()

