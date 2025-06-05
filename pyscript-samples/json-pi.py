import sys
import json

def main():
    # sys.argv[1], sys.argv[2], etc.
    customer_id = None
    input_json = None

    for i in range(len(sys.argv)):
        if sys.argv[i] == "--customer-id":
            customer_id = sys.argv[i+1]
        if sys.argv[i] == "--input-json":
            input_json = sys.argv[i+1]

    print(f"Customer ID: {customer_id}")
    print(f"Input JSON: {input_json}")

    if input_json:
        data = json.loads(input_json)
        print(f"Parsed JSON: {data}")

if __name__ == "__main__":
    main()
