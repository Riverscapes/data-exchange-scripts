import boto3
import csv

s3 = boto3.client('s3')
obj = s3.get_object(Bucket='riverscapes-athena', Key='adhoc/yct.csv')
body = obj['Body']

# If the file is text, wrap the body in a TextIOWrapper
import io
reader = csv.DictReader(io.TextIOWrapper(body, encoding='utf-8'))

with open('eggs.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)

    # Write header row
    spamwriter.writerow(reader.fieldnames)

    i = 0
    for row in reader:
        print(row)
        # Write the row values in the same order as the headers
        spamwriter.writerow([row[field] for field in reader.fieldnames])
        i += 1
        if i == 4:
            break
