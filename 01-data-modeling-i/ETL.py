import glob
import json
import os
from typing import List

import psycopg2


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["actor"]["url"],
                        each["actor"]["display_login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["repo"]["url"],
                        each["created_at"],
                        # each["payload"]["issue"]["url"],
                        # each["payload"]["action"],
                        # each["labels"]["id"],
                        # each["labels"]["node_id"],
                        # each["labels"]["url"],
                        # each["labels"]["name"],
                        # each["labels"]["color"],
                        # each["labels"]["is_default"],
                        # each["labels"]["description"],

                    )
                else:
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["actor"]["url"],
                        each["actor"]["display_login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["repo"]["url"],
                        each["created_at"],
                        # each["payload"]["issue"]["url"],
                        # each["payload"]["action"]
                        # each["labels"]["id"],
                        # each["labels"]["node_id"],
                        # each["labels"]["url"],
                        # each["labels"]["name"],
                        # each["labels"]["color"],
                        # each["labels"]["is_default"],
                        # each["labels"]["description"],
                        
                    )

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO actors (
                        id,
                        login,
                        url,
                        display_login
                    ) VALUES ('{each["actor"]["id"]}', '{each["actor"]["login"]}', '({each["actor"]["url"]}', '{each["actor"]["display_login"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO events (
                        id,
                        type,
                        actor_id
                    ) VALUES ('{each["id"]}', '{each["type"]}', '{each["actor"]["id"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO repo (
                        id,
                        name,
                        url,
                        actor_id
                    ) VALUES ('{each["repo"]["id"]}', '{each["repo"]["name"]}', '{each["repo"]["url"]}', '{each["actor"]["id"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                # insert_statement = f"""
                #     INSERT INTO payload (
                #         action,
                #         issue
                #     ) VALUES ('{each["payload"]["action"]}', '{each["payload"]["issue"]}')
                #     ON CONFLICT (id) DO NOTHING
                # """

                # print(insert_statement)
                cur.execute(insert_statement)

                conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()