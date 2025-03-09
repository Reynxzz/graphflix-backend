from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Union
import os
import json
import networkx as nx
import nx_arangodb as nxadb
from arango import ArangoClient

# Import custom modules
from graph_analytics import (
    calculate_pagerank, detect_communities, find_shortest_path,
    calculate_centrality, get_graph_data
)
from recommendation_engine import (
    find_similar_movies, get_recommendations_by_genre,
    get_recommendations_by_year, get_personalized_recommendations
)
from chat_agent import initialize_movie_recommender_agent

# Initialize FastAPI app
app = FastAPI(title="Movie Recommender API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Update with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect to ArangoDB
ARANGO_DB_URL = os.getenv("ARANGO_DB_URL", "https://your-arangodb-instance.cloud:8529")
ARANGO_DB_NAME = os.getenv("ARANGO_DB_NAME", "MovieLens")
ARANGO_DB_USERNAME = os.getenv("ARANGO_DB_USERNAME", "root")
ARANGO_DB_PASSWORD = os.getenv("ARANGO_DB_PASSWORD", "")

# Initialize database connection
db_client = ArangoClient(hosts=ARANGO_DB_URL).db(
    name=ARANGO_DB_NAME,
    username=ARANGO_DB_USERNAME,
    password=ARANGO_DB_PASSWORD,
    verify=True
)

# Initialize graph
G_adb = nxadb.Graph(name="MovieLens", db=db_client)

# Initialize chat agent
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
query_movie_graph = initialize_movie_recommender_agent(db_client, ANTHROPIC_API_KEY)

# Define request/response models
class GraphRequest(BaseModel):
    movieId: str
    depth: Optional[int] = 2

class RecommendationRequest(BaseModel):
    movieId: str
    limit: Optional[int] = 10
    threshold: Optional[float] = 0.3

class AnalyticsRequest(BaseModel):
    query: str
    context: Optional[Dict[str, Any]] = None

class ChatRequest(BaseModel):
    message: str
    context: Optional[Dict[str, Any]] = None

# API Routes
@app.get("/")
async def root():
    return {"message": "Movie Recommender API is running"}

@app.get("/graph/{movie_id}")
async def get_movie_graph(movie_id: str, depth: int = Query(2, ge=1, le=3)):
    """Get graph data for visualization centered on a specific movie"""
    try:
        graph_data = get_graph_data(G_adb, movie_id, depth)
        return graph_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching graph data: {str(e)}")

@app.get("/recommendations")
async def get_recommendations(
    movieId: str = Query(..., description="Movie ID to get recommendations for"),
    limit: int = Query(10, ge=1, le=50),
    threshold: float = Query(0.3, ge=0.1, le=1.0)
):
    """Get movie recommendations based on a source movie"""
    try:
        similar_movies = find_similar_movies(G_adb, movieId, threshold, limit)
        return {"recommendations": similar_movies}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching recommendations: {str(e)}")

@app.post("/analytics")
async def run_analytics(request: AnalyticsRequest):
    """Run graph analytics queries"""
    try:
        if "pagerank" in request.query.lower():
            result = calculate_pagerank(G_adb, request.context.get("limit", 10))
            return {"type": "pagerank", "result": result}
        elif "communit" in request.query.lower():
            result = detect_communities(G_adb, request.context.get("algorithm", "louvain"))
            return {"type": "communities", "result": result}
        elif "path" in request.query.lower() or "route" in request.query.lower():
            source = request.context.get("source")
            target = request.context.get("target")
            if not source or not target:
                raise HTTPException(status_code=400, detail="Source and target nodes are required for path analysis")
            result = find_shortest_path(G_adb, source, target)
            return {"type": "path", "result": result}
        elif "central" in request.query.lower():
            result = calculate_centrality(G_adb, request.context.get("limit", 10))
            return {"type": "centrality", "result": result}
        else:
            # Use the chat agent for general analytics queries
            response = query_movie_graph(request.query)
            return {"type": "general", "result": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running analytics: {str(e)}")

@app.post("/chat")
async def chat(request: ChatRequest):
    """Process natural language queries about movies"""
    try:
        response = query_movie_graph(request.message)
        
        # Check if the response is a dict with visualization
        if isinstance(response, dict) and "text" in response and "visualization" in response:
            return response
        else:
            return {"text": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing chat message: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

