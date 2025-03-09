from typing import Dict, List, Any, Optional, Union
import json

def find_similar_movies(G_adb, movie_id: str, threshold: float = 0.3, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Find movies similar to the specified movie
    
    Args:
        G_adb: ArangoDB graph connection
        movie_id: ID of the source movie
        threshold: Minimum similarity threshold
        limit: Maximum number of recommendations to return
        
    Returns:
        List of similar movies with similarity scores
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
        return []
    
    source_movie = movie_result[0]
    source_id = source_movie["_id"]
    
    # Find similar movies using direct similarity edges
    similar_query = f"""
    FOR movie IN MovieLens_node
    FILTER movie._id == "{source_id}"
    LET similar = (
        FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
        FILTER e.type == 'similar_to' AND e.similarity >= {threshold}
        SORT e.similarity DESC
        LIMIT {limit}
        RETURN {{
            movie: v,
            similarity: e.similarity
        }}
    )
    RETURN {{
        source_movie: movie,
        similar_movies: similar
    }}
    """
    
    similar_result = list(G_adb.query(similar_query))
    
    if similar_result and similar_result[0].get("similar_movies"):
        # Format the results
        similar_movies = []
        for item in similar_result[0]["similar_movies"]:
            movie = item["movie"]
            similar_movies.append({
                "id": movie["_id"],
                "original_id": movie.get("original_id"),
                "title": movie.get("title", "Unknown"),
                "year": movie.get("year"),
                "similarity": item["similarity"],
                "reason": "Direct similarity"
            })
        
        return similar_movies
    
    # If no direct similarities, try genre-based recommendations
    return get_recommendations_by_genre(G_adb, source_id, limit)

def get_recommendations_by_genre(G_adb, movie_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get movie recommendations based on shared genres
    
    Args:
        G_adb: ArangoDB graph connection
        movie_id: ID of the source movie
        limit: Maximum number of recommendations to return
        
    Returns:
        List of recommended movies
    """
    # Get genres for the source movie
    genre_query = f"""
    FOR movie IN MovieLens_node
    FILTER movie._id == "{movie_id}"
    LET genres = (
        FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
        FILTER e.type == 'belongs_to' AND v.type == 'genre'
        RETURN v._id
    )
    RETURN {{
        movie: movie,
        genres: genres
    }}
    """
    
    genre_result = list(G_adb.query(genre_query))
    
    if not genre_result or not genre_result[0].get("genres"):
        return []
    
    source_genres = genre_result[0]["genres"]
    
    # Find movies with similar genres
    similar_query = f"""
    FOR movie IN MovieLens_node
    FILTER movie.type == 'movie' AND movie._id != "{movie_id}"
    LET movie_genres = (
        FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
        FILTER e.type == 'belongs_to' AND v.type == 'genre'
        RETURN v._id
    )
    LET common_genres = LENGTH(
        FOR genre IN movie_genres
        FILTER genre IN {json.dumps(source_genres)}
        RETURN genre
    )
    FILTER common_genres > 0
    LET similarity = common_genres / LENGTH(UNION(movie_genres, {json.dumps(source_genres)}))
    FILTER similarity >= 0.3
    SORT similarity DESC, movie.popularity DESC
    LIMIT {limit}
    RETURN {{
        movie: movie,
        similarity: similarity,
        common_genre_count: common_genres
    }}
    """
    
    similar_result = list(G_adb.query(similar_query))
    
    # Format the results
    recommendations = []
    for item in similar_result:
        movie = item["movie"]
        recommendations.append({
            "id": movie["_id"],
            "original_id": movie.get("original_id"),
            "title": movie.get("title", "Unknown"),
            "year": movie.get("year"),
            "similarity": item["similarity"],
            "common_genre_count": item["common_genre_count"],
            "reason": f"Shares {item['common_genre_count']} genres"
        })
    
    return recommendations

def get_recommendations_by_year(G_adb, year: int, genre: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get movie recommendations based on year and optionally genre
    
    Args:
        G_adb: ArangoDB graph connection
        year: Target year
        genre: Optional genre filter
        limit: Maximum number of recommendations to return
        
    Returns:
        List of recommended movies
    """
    # Build the query based on parameters
    if genre:
        year_query = f"""
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER HAS(movie, "year") AND movie.year == {year}
        
        LET genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        
        FILTER POSITION(genres, g => LOWER(g) == LOWER("{genre}")) != -1
        
        LET ratings = (
            FOR r IN MovieLens_node_to_MovieLens_node
            FILTER r.type == 'rated' AND r._to == movie._id
            RETURN r.rating
        )
        
        LET avg_rating = LENGTH(ratings) > 0 ? AVG(ratings) : null
        LET rating_count = LENGTH(ratings)
        
        SORT avg_rating DESC NULLS LAST, movie.popularity DESC
        LIMIT {limit}
        
        RETURN {{
            movie: movie,
            genres: genres,
            avg_rating: avg_rating,
            rating_count: rating_count
        }}
        """
    else:
        year_query = f"""
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER HAS(movie, "year") AND movie.year == {year}
        
        LET genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        
        LET ratings = (
            FOR r IN MovieLens_node_to_MovieLens_node
            FILTER r.type == 'rated' AND r._to == movie._id
            RETURN r.rating
        )
        
        LET avg_rating = LENGTH(ratings) > 0 ? AVG(ratings) : null
        LET rating_count = LENGTH(ratings)
        
        SORT avg_rating DESC NULLS LAST, movie.popularity DESC
        LIMIT {limit}
        
        RETURN {{
            movie: movie,
            genres: genres,
            avg_rating: avg_rating,
            rating_count: rating_count
        }}
        """
    
    year_result = list(G_adb.query(year_query))
    
    # Format the results
    recommendations = []
    for item in year_result:
        movie = item["movie"]
        recommendations.append({
            "id": movie["_id"],
            "original_id": movie.get("original_id"),
            "title": movie.get("title", "Unknown"),
            "year": movie.get("year"),
            "genres": item["genres"],
            "avg_rating": item["avg_rating"],
            "rating_count": item["rating_count"],
            "reason": f"Released in {year}" + (f" in genre {genre}" if genre else "")
        })
    
    return recommendations

def get_personalized_recommendations(G_adb, user_preferences: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get personalized movie recommendations based on user preferences
    
    Args:
        G_adb: ArangoDB graph connection
        user_preferences: Dict with user preferences (liked_movies, liked_genres, etc.)
        limit: Maximum number of recommendations to return
        
    Returns:
        List of personalized movie recommendations
    """
    liked_movies = user_preferences.get("liked_movies", [])
    liked_genres = user_preferences.get("liked_genres", [])
    min_year = user_preferences.get("min_year")
    max_year = user_preferences.get("max_year")
    
    # Build filters based on preferences
    filters = []
    
    if min_year is not None:
        filters.append(f"HAS(movie, 'year') AND movie.year >= {min_year}")
    
    if max_year is not None:
        filters.append(f"HAS(movie, 'year') AND movie.year <= {max_year}")
    
    # Combine filters
    filter_clause = " AND ".join(filters) if filters else "true"
    
    # Build the query
    if liked_movies and liked_genres:
        # Combine movie and genre preferences
        movie_ids = [f"movie_{m}" if not m.startswith("movie_") else m for m in liked_movies]
        
        query = f"""
        LET liked_movies = {json.dumps(movie_ids)}
        LET liked_genres = {json.dumps(liked_genres)}
        
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER movie._id NOT IN liked_movies
        FILTER {filter_clause}
        
        LET movie_genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        
        LET genre_match = LENGTH(
            FOR genre IN movie_genres
            FILTER LOWER(genre) IN liked_genres
            RETURN genre
        )
        
        LET similar_to_liked = (
            FOR liked IN liked_movies
            FOR v, e IN 1..1 OUTBOUND liked MovieLens_node_to_MovieLens_node
            FILTER e.type == 'similar_to' AND v._id == movie._id
            RETURN e.similarity
        )
        
        LET similarity_score = LENGTH(similar_to_liked) > 0 ? AVG(similar_to_liked) : 0
        
        LET combined_score = (genre_match * 0.5) + (similarity_score * 5)
        
        FILTER combined_score > 0
        
        SORT combined_score DESC, movie.popularity DESC
        LIMIT {limit}
        
        RETURN {{
            movie: movie,
            genres: movie_genres,
            genre_match: genre_match,
            similarity_score: similarity_score,
            combined_score: combined_score
        }}
        """
    elif liked_movies:
        # Only movie preferences
        movie_ids = [f"movie_{m}" if not m.startswith("movie_") else m for m in liked_movies]
        
        query = f"""
        LET liked_movies = {json.dumps(movie_ids)}
        
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER movie._id NOT IN liked_movies
        FILTER {filter_clause}
        
        LET movie_genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        
        LET similar_to_liked = (
            FOR liked IN liked_movies
            FOR v, e IN 1..1 OUTBOUND liked MovieLens_node_to_MovieLens_node
            FILTER e.type == 'similar_to' AND v._id == movie._id
            RETURN e.similarity
        )
        
        LET similarity_score = LENGTH(similar_to_liked) > 0 ? AVG(similar_to_liked) : 0
        
        FILTER similarity_score > 0
        
        SORT similarity_score DESC, movie.popularity DESC
        LIMIT {limit}
        
        RETURN {{
            movie: movie,
            genres: movie_genres,
            similarity_score: similarity_score
        }}
        """
    elif liked_genres:
        # Only genre preferences
        query = f"""
        LET liked_genres = {json.dumps(liked_genres)}
        
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER {filter_clause}
        
        LET movie_genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        
        LET genre_match = LENGTH(
            FOR genre IN movie_genres
            FILTER LOWER(genre) IN liked_genres
            RETURN genre
        )
        
        FILTER genre_match > 0
        
        SORT genre_match DESC, movie.popularity DESC
        LIMIT {limit}
        
        RETURN {{
            movie: movie,
            genres: movie_genres,
            genre_match: genre_match
        }}
        """
    else:
        # No specific preferences, return popular movies
        query = f"""
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER {filter_clause}
        
        LET movie_genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        
        SORT movie.popularity DESC
        LIMIT {limit}
        
        RETURN {{
            movie: movie,
            genres: movie_genres
        }}
        """
    
    result = list(G_adb.query(query))
    
    # Format the results
    recommendations = []
    for item in result:
        movie = item["movie"]
        
        # Determine recommendation reason
        if "combined_score" in item:
            reason = f"Matches {item['genre_match']} of your preferred genres and is similar to movies you like"
        elif "similarity_score" in item:
            reason = "Similar to movies you like"
        elif "genre_match" in item:
            reason = f"Matches {item['genre_match']} of your preferred genres"
        else:
            reason = "Popular movie that matches your filters"
        
        recommendations.append({
            "id": movie["_id"],
            "original_id": movie.get("original_id"),
            "title": movie.get("title", "Unknown"),
            "year": movie.get("year"),
            "genres": item["genres"],
            "reason": reason
        })
    
    return recommendations

