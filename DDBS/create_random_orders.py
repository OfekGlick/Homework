import os
import csv
import random
import string

curdir = os.getcwd()
os.mkdir(os.path.join(curdir, 'orders'))
os.chdir(os.path.join(curdir, 'orders'))

def create_file(filename, site_range, product_range, amount_range):
    records = random.randint(5, 41)
    with open(filename, 'w+', newline='') as myfile:
        wr = csv.writer(myfile)
        for i in range(records):
            wr.writerow((random.choice(site_range), random.choice(product_range), random.choice(amount_range)))


# Modify parameters according to the orders you want to preform
orders_no = 50
site_range = list(range(4, 39))
product_range = list(range(1, 6))
amount_range = list(range(1, 52))
filenames = [''.join(random.choices(string.ascii_uppercase, k=8)) + ".csv" for _ in range(orders_no)]

for filename in filenames:
    create_file(filename, site_range, product_range, amount_range)
