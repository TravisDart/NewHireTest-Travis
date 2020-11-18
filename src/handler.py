import json
import os
import re
import csv
from datetime import datetime
from pymongo import MongoClient
from pymongo import ReturnDocument

db_uri = os.environ.get("MONGO_DB_URI", "localhost")
db_name = os.environ.get("MONGO_DB_NAME", "new_hire_test")

db = MongoClient(db_uri)[db_name]


def handle_csv_upload(event, context):
    response_body = {
        "numCreated": 0,
        "numUpdated": 0,
        "errors": [],
    }

    # YOUR LOGIC HERE
    lines = event.splitlines()
    reader = csv.DictReader(lines, delimiter=',')
    for i, row in enumerate(reader):
        # Part 1: Clean the data
        # Name
        row["Name"] = row["Name"].strip()

        # Email
        row["Email"] = row["Email"].lower().strip()
        try:
            assert(
                re.match(
                    r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
                    row["Email"]
                )
            )
        except AssertionError:
            response_body["errors"] += [
                f"Row {i} is missing a valid Email."
            ]
            continue  # Unrecoverable. Email is required.

        # Manager
        row["Manager"] = row.get("Manager", "").lower().strip()
        if row["Manager"]:
            try:
                assert(
                    re.match(
                        r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
                        row["Manager"]
                    )
                )

                # Create the Manager User if it doesn't already exist.
                # The CSV may not be ordered in hierarchy order.
                manager_object = db.user.find_one(
                    {"normalized_email": row["Manager"]},
                )
                if not manager_object:
                    return_object = db.user.insert_one({
                        "normalized_email": row["Manager"]
                    })
                    row["Manager"] = return_object.inserted_id
                else:
                    row["Manager"] = manager_object["_id"]
            except AssertionError:
                response_body["errors"] += [
                    f"Row {i} has an invalid manager's email."
                ]
                del row["Manager"]
        else:
            del row["Manager"]

        # Salary
        if "Salary" in row:
            try:
                row["Salary"] = int(row["Salary"])
            except ValueError:
                response_body["errors"] += [
                    f"Row {i} is missing a valid Salary."
                ]
                del row["Salary"]

        # Hire Date
        if "Hire Date" in row:
            try:
                row["Hire Date"] = datetime.strptime(
                    row["Hire Date"],
                    '%m/%d/%Y'
                )
                row["is_active"] = row["Hire Date"] < datetime.now()
            except ValueError:
                response_body["errors"] += [
                    f"Row {i} is missing a valid Hire Date."
                ]
                row["is_active"] = True
                del row["Hire Date"]
        else:
            row["is_active"] = True

        # Part 2: Update the database

        # First, find if the user exists in the databse already.
        existing_user = db.user.find_one(
            {"normalized_email": row["Email"]}
        ) or {}

        # If a component of the CSV row is invalid, replace the value with
        # the value currently in the database (if there is one).
        row_for_database = {
            "name": row.get("Name", existing_user.get("name")),
            "normalized_email": row.get(
                "Email",
                existing_user.get("normalized_email")
            ),
            "manager_id": row.get("Manager", existing_user.get("manager_id")),
            "salary": row.get("Salary", existing_user.get("salary")),
            "hire_date": row.get("Hire Date", existing_user.get("Hire Date")),
            "is_active": row.get("is_active", existing_user.get("is_active")),
            "hashed_password": existing_user.get("hashed_password"),
        }

        if not row_for_database["name"]:
            response_body["errors"] += [
                f"Row {i} is missing Name."
            ]
            continue  # Sorry, name is required.

        # Update the tally.
        if existing_user:
            # If the record is missing name, either the document is empty (the
            # user doesn't exist), or it is a manager that hasn't appeared yet
            # as a user in any uploaded CSV.
            if existing_user.get("name"):
                response_body["numUpdated"] += 1
        else:
            response_body["numCreated"] += 1

        # Upsert the user.
        user_document = db.user.find_one_and_replace(
            {"normalized_email": row["Email"]},
            row_for_database,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        # Create the chain of command.
        if existing_user:
            db.chain_of_command.delete_one({"user_id": existing_user["_id"]})

        managers = []
        manager = user_document["manager_id"]
        while manager:
            managers += [manager]
            manager_document = db.user.find_one({"_id": manager}) or {}
            manager = manager_document.get("manager_id")

        db.chain_of_command.insert_one({
             "user_id": user_document["_id"],
             "chain_of_command": managers
        })

        print("user_document", user_document)

    print("chain of command")
    x = db.chain_of_command.find()
    for xx in x:
        print(xx)

    # Count how many users don't have a name.
    # These are managers who don't appear in the file as users.
    missing_managers = db.user.count_documents({"name": None})
    if missing_managers:
        response_body["errors"] += [
            (
                f"There are {missing_managers} Managers that do not appear in "
                "the file as users."
            )
        ]

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }
    return response
