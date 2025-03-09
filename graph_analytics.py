import networkx as nx
import nx_arangodb as nxadb
from typing import Dict, List, Any, Optional, Union
import json

def get_graph_data(G_adb, movie_id: str, depth: int = 2) -> Dict[str, Any]:
    """
    Get graph data for visualization centered on a specific movie
    
    Args:
        G_adb: ArangoDB graph connection
        movie_id: ID of the central movie
        depth: Traversal depth from central movie
        
    Returns:
        Dict with nodes and links for visualization
    """
    # Create node ID with proper prefix if needed
    if not movie_id.startswith("movie_"):
        movie_id = f"movie_{movie_id}"
    
    # Find the movie node
    movie_query = f"""
    FOR movie IN MovieLens_node
    FILTER movie._id == "{movie_id}" OR movie.original_id == "{movie_id.replace('movie_', '')}"
    LIMIT 1
    RETURN movie
    """
    
    movie_result = list(G_adb.query(movie_query))
    if not movie_result:
        return {"nodes": [], "links": []}
    
    central_movie = movie_result[0]
    central_id = central_movie["_id"]
    
    # Get subgraph with traversal
    subgraph_query = f"""
    LET central = (
        FOR movie IN MovieLens_node
        FILTER movie._id == "{central_id}"
        LIMIT 1
        RETURN movie
    )[0]
    
    LET nodes = (
        FOR v, e, p IN 1..{depth} ANY central MovieLens_node_to_MovieLens_node
        RETURN DISTINCT {{
            node: v,
            distance: LENGTH(p.edges)
        }}
    )
    
    LET links = (
        FOR v, e, p IN 1..{depth} ANY central MovieLens_node_to_MovieLens_node
        RETURN DISTINCT e
    )
    
    RETURN {{
        central: central,
        nodes: nodes,
        links: links
    }}
    """
    
    result = list(G_adb.query(subgraph_query))
    if not result:
        return {"nodes": [], "links": []}
    
    graph_data = result[0]
    
    # Format nodes for visualization
    nodes = [
        {
            "id": graph_data["central"]["_id"],
            "label": graph_data["central"].get("title", graph_data["central"]["_id"]),
            "type": graph_data["central"].get("type", "unknown"),
            "year": graph_data["central"].get("year"),
            "group": "central"
        }
    ]
    
    for node_data in graph_data["nodes"]:
        node = node_data["node"]
        node_type = node.get("type", "unknown")
        
        # Format label based on node type
        if node_type == "movie":
            label = node.get("title", node["_id"])
        elif node_type == "genre":
            label = node.get("name", node["_id"])
        elif node_type == "tag":
            label = node.get("name", node["_id"])
        else:
            label = node["_id"]
        
        nodes.append({
            "id": node["_id"],
            "label": label,
            "type": node_type,
            "year": node.get("year") if node_type == "movie" else None,
            "group": node_type,
            "distance": node_data["distance"]
        })
    
    # Format links for visualization
    links = []
    for link in graph_data["links"]:
        link_type = link.get("type", "unknown")
        
        # Add weight/value based on link type
        if link_type == "similar_to":
            weight = link.get("similarity", 0.5)
        elif link_type == "rated":
            weight = link.get("rating", 3) / 5
        else:
            weight = 1.0
        
        links.append({
            "source": link["_from"],
            "target": link["_to"],
            "type": link_type,
            "weight": weight
        })
    
    return {
        "nodes": nodes,
        "links": links
    }

def calculate_pagerank(G_adb, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Calculate PageRank for movies in the graph
    
    Args:
        G_adb: ArangoDB graph connection
        limit: Number of top results to return
        
    Returns:
        List of movies with their PageRank scores
    """
    # Convert to NetworkX for algorithm
    G_nx = nx.DiGraph()
    
    # Get all movie nodes
    movie_query = """
    FOR movie IN MovieLens_node
    FILTER movie.type == 'movie'
    RETURN movie
    """
    
    movies = list(G_adb.query(movie_query))
    
    # Get all relevant edges
    edge_query = """
    FOR e IN MovieLens_node_to_MovieLens_node
    FILTER e.type IN ['similar_to', 'belongs_to', 'has_tag']
    RETURN e
    """
    
    edges = list(G_adb.query(edge_query))
    
    # Build NetworkX graph
    for movie in movies:
        G_nx.add_node(movie["_id"], **movie)
    
    for edge in edges:
        G_nx.add_edge(edge["_from"], edge["_to"], **edge)
    
    # Calculate PageRank
    pagerank = nx.pagerank(G_nx, alpha=0.85)
    
    # Sort and format results
    movie_pageranks = []
    for movie_id, score in pagerank.items():
        node_data = G_nx.nodes.get(movie_id)
        if node_data and node_data.get("type") == "movie":
            movie_pageranks.append({
                "id": movie_id,
                "title": node_data.get("title", movie_id),
                "year": node_data.get("year"),
                "pagerank": score
            })
    
    # Sort by PageRank score (descending)
    movie_pageranks.sort(key=lambda x: x["pagerank"], reverse=True)
    
    return movie_pageranks[:limit]

def detect_communities(G_adb, algorithm: str = "louvain") -> Dict[str, List[Dict[str, Any]]]:
    """
    Detect communities in the movie graph
    
    Args:
        G_adb: ArangoDB graph connection
        algorithm: Community detection algorithm to use
        
    Returns:
        Dict with communities and their members
    """
    # Convert to NetworkX for algorithm
    G_nx = nx.Graph()
    
    # Get all movie nodes
    movie_query = """
    FOR movie IN MovieLens_node
    FILTER movie.type == 'movie'
    RETURN movie
    """
    
    movies = list(G_adb.query(movie_query))
    
    # Get similarity edges
    edge_query = """
    FOR e IN MovieLens_node_to_MovieLens_node
    FILTER e.type == 'similar_to'
    RETURN e
    """
    
    edges = list(G_adb.query(edge_query))
    
    # Build NetworkX graph
    for movie in movies:
        G_nx.add_node(movie["_id"], **movie)
    
    for edge in edges:
        weight = edge.get("similarity", 0.5)
        G_nx.add_edge(edge["_from"], edge["_to"], weight=weight)
    
    # Detect communities
    if algorithm.lower() == "louvain":
        from community import best_partition
        partition = best_partition(G_nx)
    elif algorithm.lower() == "label_propagation":
        partition = {node: i for i, comm in enumerate(nx.algorithms.community.label_propagation_communities(G_nx)) 
                    for node in comm}
    else:
        # Default to Girvan-Newman
        communities_generator = nx.algorithms.community.girvan_newman(G_nx)
        partition = {}
        for i, comm in enumerate(next(communities_generator)):
            for node in comm:
                partition[node] = i
    
    # Group movies by community
    communities = {}
    for movie_id, community_id in partition.items():
        if community_id not in communities:
            communities[community_id] = []
        
        node_data = G_nx.nodes.get(movie_id)
        if node_data and node_data.get("type") == "movie":
            communities[community_id].append({
                "id": movie_id,
                "title": node_data.get("title", movie_id),
                "year": node_data.get("year")
            })
    
    # Sort communities by size
    sorted_communities = {
        f"community_{i}": members for i, (_, members) in 
        enumerate(sorted(communities.items(), key=lambda x: len(x[1]), reverse=True))
    }
    
    return sorted_communities

def find_shortest_path(G_adb, source_id: str, target_id: str) -> Dict[str, Any]:
    """
    Find shortest path between two movies
    
    Args:
        G_adb: ArangoDB graph connection
        source_id: ID of the source movie
        target_id: ID of the target movie
        
    Returns:
        Dict with path information
    """
    # Create node IDs with proper prefix if needed
    if not source_id.startswith("movie_"):
        source_id = f"movie_{source_id}"
    if not target_id.startswith("movie_"):
        target_id = f"movie_{target_id}"
    
    # Find the shortest path using AQL
    path_query = f"""
    FOR source IN MovieLens_node
    FILTER source._id == "{source_id}" OR source.original_id == "{source_id.replace('movie_', '')}"
    
    FOR target IN MovieLens_node
    FILTER target._id == "{target_id}" OR target.original_id == "{target_id.replace('movie_', '')}"
    
    FOR path IN OUTBOUND SHORTEST_PATH source TO target MovieLens_node_to_MovieLens_node
    RETURN {{
        vertices: path.vertices,
        edges: path.edges
    }}
    """
    
    path_result = list(G_adb.query(path_query))
    
    if not path_result:
        return {
            "source": source_id,
            "target": target_id,
            "path_exists": False,
            "path": []
        }
    
    path_data = path_result[0]
    
    # Format path for response
    formatted_path = []
    for i in range(len(path_data["vertices"])):
        node = path_data["vertices"][i]
        node_type = node.get("type", "unknown")
        
        # Format label based on node type
        if node_type == "movie":
            label = node.get("title", node["_id"])
        elif node_type == "genre":
            label = node.get("name", node["_id"])
        elif node_type == "tag":
            label = node.get("name", node["_id"])
        else:
            label = node["_id"]
        
        path_item = {
            "id": node["_id"],
            "label": label,
            "type": node_type
        }
        
        # Add edge information if not the last node
        if i < len(path_data["vertices"]) - 1:
            edge = path_data["edges"][i]
            path_item["edge"] = {
                "type": edge.get("type", "unknown"),
                "weight": edge.get("similarity", 1.0) if edge.get("type") == "similar_to" else 1.0
            }
        
        formatted_path.append(path_item)
    
    return {
        "source": source_id,
        "target": target_id,
        "path_exists": True,
        "path_length": len(formatted_path) - 1,
        "path": formatted_path
    }

def calculate_centrality(G_adb, limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    Calculate various centrality measures for movies
    
    Args:
        G_adb: ArangoDB graph connection
        limit: Number of top results to return
        
    Returns:
        Dict with different centrality measures
    """
    # Convert to NetworkX for algorithm
    G_nx = nx.Graph()
    
    # Get all movie nodes
    movie_query = """
    FOR movie IN MovieLens_node
    FILTER movie.type == 'movie'
    RETURN movie
    """
    
    movies = list(G_adb.query(movie_query))
    
    # Get all relevant edges
    edge_query = """
    FOR e IN MovieLens_node_to_MovieLens_node
    FILTER e.type IN ['similar_to', 'belongs_to', 'has_tag']
    RETURN e
    """
    
    edges = list(G_adb.query(edge_query))
    
    # Build NetworkX graph
    for movie in movies:
        G_nx.add_node(movie["_id"], **movie)
    
    for edge in edges:
        weight = edge.get("similarity", 0.5) if edge.get("type") == "similar_to" else 1.0
        G_nx.add_edge(edge["_from"], edge["_to"], weight=weight)
    
    # Calculate centrality measures
    degree_centrality = nx.degree_centrality(G_nx)
    betweenness_centrality = nx.betweenness_centrality(G_nx)
    closeness_centrality = nx.closeness_centrality(G_nx)
    
    # Format results
    centrality_results = {
        "degree": [],
        "betweenness": [],
        "closeness": []
    }
    
    for movie_id in G_nx.nodes():
        node_data = G_nx.nodes.get(movie_id)
        if node_data and node_data.get("type") == "movie":
            movie_info = {
                "id": movie_id,
                "title": node_data.get("title", movie_id),
                "year": node_data.get("year")
            }
            
            # Add to degree centrality
            degree = degree_centrality.get(movie_id, 0)
            centrality_results["degree"].append({
                **movie_info,
                "centrality": degree
            })
            
            # Add to betweenness centrality
            betweenness = betweenness_centrality.get(movie_id, 0)
            centrality_results["betweenness"].append({
                **movie_info,
                "centrality": betweenness
            })
            
            # Add to closeness centrality
            closeness = closeness_centrality.get(movie_id, 0)
            centrality_results["closeness"].append({
                **movie_info,
                "centrality": closeness
            })
    
    # Sort each list by centrality (descending) and limit results
    for measure in centrality_results:
        centrality_results[measure].sort(key=lambda x: x["centrality"], reverse=True)
        centrality_results[measure] = centrality_results[measure][:limit]
    
    return centrality_results

