
import requests

# Replace with your Airflow instance URL and credentials
AIRFLOW_BASE_URL = 'http://localhost:8080/api/v1'
USERNAME = 'admin'
PASSWORD = 'admin'

# New user details
new_user = {
    "username": "new_user",
    "email": "new_user@example.com",
    "first_name": "New",
    "last_name": "User",
    "roles": ["User"],
    "password": "secure_password"
}

response = requests.post(
    f"{AIRFLOW_BASE_URL}/users",
    auth=(USERNAME, PASSWORD),
    json=new_user
)

if response.status_code == 200:
    print("User created successfully.")
else:
    print(f"Failed to create user: {response.text}")
