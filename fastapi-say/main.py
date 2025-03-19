from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess

app = FastAPI()

class Person(BaseModel):
    firstname: str
    lastname: str
    country: str

@app.post("/greet")
async def greet_person(person: Person):
    try:
        # Create the greeting message
        greeting = f"hello {person.firstname} from {person.country}"
        
        # Execute the say command
        subprocess.run(["say", greeting])
        
        return {"message": "Greeting spoken successfully", "greeting": greeting}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=14271) 
