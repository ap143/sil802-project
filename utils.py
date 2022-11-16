import csv
import subprocess
import pandas as pd


def parse_output(result):
    """
    Parse the output of the program and save it to a csv file
    """

    table = []
    plus_Count = 0

    for line in result.split("\n"):
        if line.startswith("+"):
            plus_Count += 1
        elif plus_Count == 1:
            raw_row = line.split("|")[1:-1]
            #print(raw_row)
            headers = [x.strip() for x in raw_row]
            #print(headers)
        else:
            raw_row = line.split("|")[1:-1]
            row = [x.strip() for x in raw_row]
            
            if len(row) > 0:
                table.append(row)

    # print(headers, table)
    return pd.DataFrame(table, columns=headers)


def basic_query(req):
    """
        Gets query params as dictionary
    """

    # Build query
    query = "SELECT "
    query += req["group_by"] + ", "
    query += req["aggr_func"] + "(" + req["aggr_column"] + ") " + "AS " + req["aggr_func"] + " "
    
    query += "FROM data "
    
    if "where" in req:
        query += "WHERE " + req["where"] + " "
    
    query += "GROUP BY " + ", ".join([col.split()[-1] for col in req["group_by"].split(",")]) + " "
    
    if "order_by" in req:
        query += "ORDER BY " + req["order_by"] + " "
    
    if "limit" in req and req["limit"] != -1:
        query += "LIMIT " + str(req["limit"]) + " "
    
    if "offset" in req and req["offset"] != -1:
        query += "OFFSET " + str(req["offset"]) + " "
    
    query += ";"

    return query


def private_query(req):
    """
        Gets query params as dictionary
    """

    df = pd.read_csv("data.csv")

    if req["aggr_func"].lower() == "count":
        max_val = 2
    else:
        vals = df[req["aggr_column"]]
        ls = []
        for val in vals:
            try:
                ls.append(float(val))
            except:
                pass
        
        if len(ls) == 0:
            max_val = 1
        else:
            max_val = max(ls)
        req["aggr_column"] = f"CAST(" + req["aggr_column"] + " AS FLOAT32)"

    clamping = f"CLAMPED BETWEEN 0 AND {max_val}"

    # Build query
    query = f"SELECT WITH ANONYMIZATION OPTIONS(epsilon = {req['epsilon']}, delta = {req['delta']}, kappa = 3) "
    query += req["group_by"] + ", "
    query += "ANON_" + req["aggr_func"] + "(" + req["aggr_column"] + " " + clamping + ") " + "AS " + req["aggr_func"] + " "
    
    query += "FROM data "
    
    if "where" in req:
        query += "WHERE " + req["where"] + " "
    
    query += "GROUP BY " + ", ".join([col.split()[-1] for col in req["group_by"].split(",")]) + " "
    
    if "order_by" in req:
        query += "ORDER BY " + req["order_by"] + " "
    
    if "limit" in req and req["limit"] != -1:
        query += "LIMIT " + str(req["limit"]) + " "
    
    if "offset" in req and req["offset"] != -1:
        query += "OFFSET " + str(req["offset"]) + " "
    
    query += ";"
    return query


def run_query(query):
    print(query)

    query = query.replace("`", "\\`")

    # Build command
    command = "./execute_query  --data_set=$(pwd)/data.csv --userid_col=incident_number " + f'"{query}"'

    # Run command
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    output = process.stdout.read()

    # Parse output
    df = parse_output(output.decode("utf-8"))
    return df


def filter(req):
    # Add more filters with time

    if req["group_by"] == "incident_number":
        return False

    return True
