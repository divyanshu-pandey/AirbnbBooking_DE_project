import random
import string
import datetime
import time
import boto3
import json

sqs_url='https://sqs.ap-south-1.amazonaws.com/992382508283/AirbnbBookingQueue'
sqs_client=boto3.client('sqs')

def booking_details():
    # Define the characters to use
    characters = string.ascii_letters + string.digits

    # Generate a random string of length 6 for bookinng id, user id and property id 
    booking_id = ''.join(random.choice(characters) for _ in range(6))
    #print('booking_id->',booking_id)

    user_id=''.join(random.choice(characters)for _ in range (6))
    #print('user_id->', user_id)

    property_id=''.join(random.choice(characters) for _ in range(6))
    #print('property_id->',property_id)

    #city list to select random city
    city_list=['Banaglore, India', 'Pune, India','Lucknow, India','Indore, India','Jaipur, India','Mumbai, India','Delhi, India','Manali, India']

    location=random.choice(city_list)
    #print('Location->',location)

    # to select random start booking date 
    start_date=datetime.date(2023, 1, 1)
    end_date=datetime.date(2024, 12, 31)
    total_date=(end_date-start_date).days
    random_day=random.randint(0,total_date)
    start_booking_date1=start_date+datetime.timedelta(days=random_day)
    start_booking_date = start_booking_date1.strftime('%Y-%m-%d')

    #print('Start_booking_date->',start_booking_date)

    #to select random end_booking date
    random_end_day=random.randint(0,total_date//50)
    end_booking_date1=start_booking_date1+ datetime.timedelta(days=random_end_day)
    end_booking_date = end_booking_date1.strftime('%Y-%m-%d')
    #print('end_booking_date->',end_booking_date)

    # Generate a random price b/w 0 to 1000
    random_price = 500.0 + random.random() * 500.0
    price=round(random_price,2)
    #print('random_price->',price)
    
    booking_for_days=(end_booking_date1-start_booking_date1).days
    booking_for_days=int(booking_for_days)

    d1={'booking_id': booking_id,
        'user_id':user_id,
        'property_id':property_id,
        'location':location,
        'start_booking_date':start_booking_date,
        'end_booking_date':end_booking_date,
        'price':price,
        'booking_for_days':booking_for_days
    }
    return d1

def lambda_handler(event, context):
    i=0
    while(i<200):
        bookings=booking_details()
        bookings_json=json.dumps(bookings)
        print(bookings_json)
        sqs_client.send_message(
            QueueUrl=sqs_url,
            MessageBody=json.dumps(bookings)
            
        )
        
        i+=1
    return {
        'statusCode': 200,
        'body': json.dumps('Bookings are published into sqs..')
    }


