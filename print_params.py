import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('--firstname')
parser.add_argument('--lastname')
parser.add_argument('--cars')  # will be JSON string

args = parser.parse_args()

cars = json.loads(args.cars)

print(f"First Name: {args.firstname}")
print(f"Last Name: {args.lastname}")
print("Cars:")
for car in cars:
    print(f"  - {car['name']} ({car['model']})")
