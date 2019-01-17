# Imports

import pandas
from tqdm import tqdm
import numpy


# Script input parameters

output_file_dir = "./"
output_file_name = "result"
output_format = "csv"

num_customers = 10000
transaction_params = {
    'trn_type':{'distribution':'discrete', 'parameters':{'names':['view','click','buy'], 'p':[80,15,5]}},
    'campaign':{'distribution':'discrete', 'parameters':{'names':['campaign1','campaign2','campaign3','campaign4','campaign5'], 'p':[35,25,20,10,10]}},
    'trn_amt': {'distribution':'lognormal', 'parameters':(6,1)},
    'product': {'distribution':'discrete','parameters':{'names':['product1','product2','product3','product4','product5'], 'p':[30,20,20,15,15]}}
}


# Create data dictionary
result = {"cust_id":[], "trn_id":[], "trn_type":[], "trn_dt":[]}
for transaction_param in transaction_params:
    result[transaction_param] = []

# Normalize probabilites for discrete distributions
for param in transaction_params:    
    if transaction_params[param]['distribution']=="discrete":
        transaction_params[param]['parameters']['p'] = [x / sum(transaction_params[param]['parameters']['p']) for x in transaction_params[param]['parameters']['p']]


def generate_sample(param_map, param_name):
    distribution_type = param_map[param_name]['distribution']
    distribution_parameters = param_map[param_name]['parameters']
    if distribution_type=="discrete":
        return (numpy.random.choice(distribution_parameters['names'], p=distribution_parameters['p']))
    if distribution_type=="lognormal":
        return (round(numpy.random.lognormal(mean=distribution_parameters[0],sigma=distribution_parameters[1])))



# Generation loop

trn_id = 0
c = 0
for cust_num in tqdm(range(num_customers), total=num_customers, unit="customers"):
  for activity in range(round(numpy.random.lognormal(mean=3,sigma=1))):
    trn_id += 1
    dt_mean = numpy.random.uniform(0,1000)
    dt_sigma = numpy.random.uniform(50,100)
    dt = max(min(round(numpy.random.normal(dt_mean, dt_sigma)),1000),0)
    if dt < 0 or dt > 1000:
        c += 1
        continue
    for transaction_param in transaction_params:
        result[transaction_param].append(generate_sample(transaction_params, transaction_param))
    result['cust_id'].append(cust_num)
    result['trn_id'].append(trn_id)
    result['trn_dt'].append(dt)

df = pandas.DataFrame(result)


# Convert to string
df['start_date'] = "2016-01-01"
df['trn_dt'] = pandas.to_datetime(df['start_date']) + pandas.to_timedelta(df['trn_dt'],"d")
df = df.drop(columns=["start_date"])

output = output_file_dir + output_file_name
if output_format == "csv":
    df.to_csv(output+".csv", sep=";")
if output_format == "parquet":
    df.to_parquet(output+".parquet")
if output_format == "pickle":
    df.to_csv("result.csv", sep=";")

transaction_params = """{
    'trn_type':{'distribution':'discrete', 'parameters':{'names':['view','click','buy'], 'p':[80,15,5]}},
    'campaign':{'distribution':'discrete', 'parameters':{'names':['campaign1','campaign2','campaign3','campaign4','campaign5'], 'p':[35,25,20,10,10]}},
    'trn_amt': {'distribution':'lognormal', 'parameters':(6,1)},
    'product': {'distribution':'discrete','parameters':{'names':['product1','product2','product3','product4','product5'], 'p':[30,20,20,15,15]}}
}"""

transaction_params = '''{
    "trn_type":{"distribution":"discrete", "parameters":{"names":["view","click","buy"], "p":[80,15,5]}},
    "campaign":{"distribution":"discrete", "parameters":{"names":["campaign1","campaign2","campaign3","campaign4","campaign5"], "p":[35,25,20,10,10]}},
    "trn_amt": {"distribution":"lognormal", "parameters":[6,1]},
    "product": {"distribution":"discrete","parameters":{"names":["product1","product2","product3","product4","product5"], "p":[30,20,20,15,15]}}
}'''