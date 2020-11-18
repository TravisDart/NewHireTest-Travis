from handler import db, handle_csv_upload
import json
import datetime
import pymongo
import bcrypt
from bson import ObjectId


def dummy_data_decorator(test_function):
    def f():
        '''
        Drop any existing data and fill in some dummy test data,
        as well as creating indexes; the data will be dropped after
        the test as well
        '''

        db.user.drop()
        db.user.create_index([
            ("normalized_email", pymongo.ASCENDING),
        ], unique=True)

        dummy_users = [
            {
                "_id": ObjectId(),
                "name": "Brad Jones",
                "normalized_email": "bjones@performyard.com",
                "manager_id": None,
                "salary": 90000,
                "hire_date": datetime.datetime(2010, 2, 10),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(
                    b"password", bcrypt.gensalt()
                ),
            },
            {
                "_id": ObjectId(),
                "name": "Ted Harrison",
                "normalized_email": "tharrison@performyard.com",
                "manager_id": None,
                "salary": 50000,
                "hire_date": datetime.datetime(2012, 10, 20),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(
                    b"correct horse battery staple", bcrypt.gensalt()
                ),
            }
        ]

        # Give Ted a manager
        dummy_users[1]["manager_id"] = dummy_users[0]["_id"]

        for user in dummy_users:
            db.user.insert(user)

        db.chain_of_command.drop()
        db.chain_of_command.create_index([
            ("user_id", pymongo.ASCENDING),
        ], unique=True)

        dummy_chain_of_commands = [
            {"user_id": dummy_users[0]["_id"], "chain_of_command":[]},
            {
                "user_id": dummy_users[1]["_id"],
                "chain_of_command":[dummy_users[0]["_id"]]
            },
        ]

        for chain_of_command in dummy_chain_of_commands:
            db.chain_of_command.insert(chain_of_command)

        test_function()
        db.user.drop()
        db.chain_of_command.drop()
    return f


@dummy_data_decorator
def test_setup():
    '''
    This test should always pass if your environment is set up correctly
    '''
    assert(True)


@dummy_data_decorator
def test_simple_csv():
    '''
    This should successfully update one user and create one user,
    also updating their chain of commands appropriately
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Brad Jones,bjones@performyard.com,,100000,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 100000)

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_number():
    '''
    This test should still update Brad and create John, but should return
    a single error because the salary field for Brad isn't a number
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,NOT A NUMBER,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 1)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 90000)
    assert(brad["name"] == "Bradley Jones")

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_manager_that_is_not_a_user():
    '''If a manager's email does not match a row that has been encountered
    so far, we should create a user for that manager. However, if we get to the
    end of the CSV and we still haven't encountered the manager, return
    an error.
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
John Smith,jsmith@performyard.com,th@performyard.com,80000,07/16/2020
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    j = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    manager = db.user.find_one({"normalized_email": "th@performyard.com"})
    assert(j["manager_id"] == manager["_id"])

    j_chain_of_command = db.chain_of_command.find_one(
        {"user_id": j["_id"]})
    assert(len(j_chain_of_command["chain_of_command"]) == 1)
    assert(j_chain_of_command["chain_of_command"][0] == manager["_id"])


@dummy_data_decorator
def test_updated_row():
    '''If a row has the same email address as a previous row, then update that
    user's information with the data on the most recently encountered row.
    '''

    two_years_from_now = datetime.datetime.now().year + 2
    body = '''Name,Email,Manager,Salary,Hire Date
John Smith,jsmith@performyard.com,tharrison@performyard.com,80000,07/16/{year}
'''.format(year=two_years_from_now)

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 0)

    ted = db.user.find_one({"normalized_email": "tharrison@performyard.com"})
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})

    # Check the first row.
    j = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(j["name"] == "John Smith")
    assert(j["manager_id"] == ted["_id"])
    assert(j["salary"] == 80000)
    assert(j["hire_date"] == datetime.datetime(two_years_from_now, 7, 16))
    assert(not(j["is_active"]))

    # Ted is Johnny's manager, and Brad is Ted's manager.
    j_chain_of_command = db.chain_of_command.find_one(
        {"user_id": j["_id"]})
    assert(len(j_chain_of_command["chain_of_command"]) == 2)
    ted_and_brad = set([ted["_id"], brad["_id"]])
    assert(set(j_chain_of_command["chain_of_command"]) == ted_and_brad)

    # Check the second row.
    body = '''Name,Email,Manager,Salary,Hire Date
Johnny Smith,jsmith@performyard.com,bjones@performyard.com,80001,07/17/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 0)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    j = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(j["name"] == "Johnny Smith")
    assert(j["manager_id"] == brad["_id"])
    assert(j["salary"] == 80001)
    assert(j["hire_date"] == datetime.datetime(2018, 7, 17))
    assert(j["is_active"])

    j_chain_of_command = db.chain_of_command.find_one(
        {"user_id": j["_id"]})
    assert(len(j_chain_of_command["chain_of_command"]) == 1)
    assert(j_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_name():
    '''Name is required, so discard all rows without a valid name.'''

    body = '''Name,Email,Manager,Salary,Hire Date
,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 0)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)


@dummy_data_decorator
def test_invalid_email():
    body = '''Name,Email,Manager,Salary,Hire Date
John Smith,jsmithperformyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 0)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john is None)


@dummy_data_decorator
def test_invalid_manager_email():
    body = '''Name,Email,Manager,Salary,Hire Date
John Smith,jsmith@performyard.com,bjonesperformyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["manager_id"] is None)

    # Check that John doesn't have a manager.
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 0)


@dummy_data_decorator
def test_missing_hire_date():
    '''If hire date is missing from the CSV, assume they are active.'''

    body = '''Name,Email,Manager,Salary
John Smith,jsmith@performyard.com,tharrison@performyard.com,80000
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 0)

    j = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(j["is_active"])


@dummy_data_decorator
def test_invalid_hire_date():
    body = '''Name,Email,Manager,Salary,Hire Date
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,99/99/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["is_active"])
    assert(john["hire_date"] is None)
