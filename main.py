import utils
import json

from fastapi import FastAPI, Request

        
# Start API server on port 80
app = FastAPI()

@app.get("/basic")
async def basic(request: Request):
    """
        Run queries without privacy
    """

    print("Running basic queries")
    
    req = await request.json()

    if not utils.filter(req):
        return "Invalid Request"

    df = utils.run_query(utils.basic_query(req))

    df.iloc[:, -1] = df.iloc[:, -1].astype("float64")
    
    response = df.to_dict('list')
    return response


@app.get("/private")
async def private(request: Request):
    """
        Run queries with privacy
    """
    
    print("Running private queries")
    
    req = await request.json()

    if not utils.filter(req):
        return "Invalid Request"
    
    if "epsilon" not in req:
        req["epsilon"] = 1
    if "delta" not in req:
        req["delta"] = 1e-2

    df = utils.run_query(utils.private_query(req))

    df.iloc[:, -1] = df.iloc[:, -1].astype("float64")
    
    response = df.to_dict('list')
    return response