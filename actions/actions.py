import psycopg2
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from datetime import datetime

# Function to establish the database connection
def connect_to_db():
    return psycopg2.connect(
        host="postgres",          # Docker service name for PostgreSQL
        database="taxi_data",       # Database name
        user="airflow",           # Database user
        password="airflow"        # Database password
    )

# Action to get the average trip duration
class ActionGetAvgTripDuration(Action):
    def name(self):
        return "action_get_avg_trip_duration"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute('SELECT avg_trip_duration FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;')
        result = cursor.fetchone()

        if result:
            response = f"The average trip duration is {result[0]} minutes."
        else:
            response = "No data found for trip duration."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the average trip distance
class ActionGetAvgTripDistance(Action):
    def name(self):
        return "action_get_avg_trip_distance"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT avg_trip_distance FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The average trip distance is {result[0]} miles."
        else:
            response = "No data found for trip distance."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the maximum number of passengers
class ActionGetMaxPassengers(Action):
    def name(self):
        return "action_get_max_passengers"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT max_passengers FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The maximum number of passengers in a trip is {result[0]}."
        else:
            response = "No data found for the maximum number of passengers."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the total revenue
class ActionGetTotalRevenue(Action):
    def name(self):
        return "action_get_total_revenue"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT total_revenue FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The total revenue is ${result[0]:,.2f}."
        else:
            response = "No data found for total revenue."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the most common payment method
class ActionGetMostCommonPaymentType(Action):
    def name(self):
        return "action_get_most_common_payment_type"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT most_common_payment_type FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The most common payment method is {result[0]}."
        else:
            response = "No data found for payment methods."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the average tip amount
class ActionGetAvgTipAmount(Action):
    def name(self):
        return "action_get_avg_tip_amount"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT avg_tip_amount FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The average tip amount is ${result[0]:,.2f}."
        else:
            response = "No data found for tip amounts."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the average tolls per trip
class ActionGetAvgTollsPerTrip(Action):
    def name(self):
        return "action_get_avg_tolls_per_trip"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT avg_tolls_per_trip FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The average toll per trip is ${result[0]:,.2f}."
        else:
            response = "No data found for tolls."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the most frequent driver
class ActionGetMostFrequentDriver(Action):
    def name(self):
        return "action_get_most_frequent_driver"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT most_frequent_driver FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The most frequent driver is {result[0]}."
        else:
            response = "No data found for drivers."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []

# Action to get the revenue per mile
class ActionGetRevenuePerMile(Action):
    def name(self):
        return "action_get_revenue_per_mile"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain):
        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SELECT revenue_per_mile FROM analysis_results ORDER BY analysis_date DESC LIMIT 1;")
        result = cursor.fetchone()

        if result:
            response = f"The average revenue per mile is ${result[0]:,.2f}."
        else:
            response = "No data found for revenue per mile."

        cursor.close()
        connection.close()
        dispatcher.utter_message(text=response)
        return []
