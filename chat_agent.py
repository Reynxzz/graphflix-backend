import os
from typing import Dict, List, Any, Optional, Union
import json
import re
import random

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, AIMessage

def initialize_movie_recommender_agent(db_client, api_key, model_name="claude-3-5-sonnet-20241022"):
    """Initialize the GraphRAG agent with ArangoDB and LLM setup"""
    os.environ["ANTHROPIC_API_KEY"] = api_key
    
    llm = ChatAnthropic(temperature=0, model_name=model_name)
    
    def text_to_aql_to_text(query: str):
        """
        Translates a natural language query into AQL, executes it against the MovieLens graph,
        and returns the results in natural language.
        """
        # Extract potential movie titles from the query
        movie_titles = extract_movie_titles(query)
        
        if any(word in query.lower() for word in ["similar", "like", "related"]) and movie_titles:
            primary_movie = movie_titles[0]  # Take the first movie title
            return handle_similar_movies_query(db_client, primary_movie, llm)
        elif any(genre in query.lower() for genre in ["comedy", "action", "drama", "horror", "sci-fi"]):
            genres = []
            for genre in ["comedy", "action", "drama", "horror", "sci-fi", "thriller", "romance", "animation"]:
                if genre in query.lower():
                    genres.append(genre)
            if genres:
                return handle_genre_query(db_client, genres[0], "top" in query.lower() or "best" in query.lower(), llm)
        elif any(period in query.lower() for period in ["90s", "80s", "2000s", "1990"]):
            years = re.findall(r'\b(19\d0s|20\d0s|\d{4})\b', query)
            if years:
                return handle_year_query(db_client, years[0], llm)
        
        return standard_query_processing(db_client, query, llm)
    
    def create_graphrag_input(query: str):
        return {
            "messages": [
                {"role": "system", "content": """You are an expert MovieLens GraphRAG agent that helps users discover movies and understand the MovieLens dataset.
                You have access to a graph database containing movies, users, ratings, genres, and tags.
                
                When responding:
                1. Ensure you understand exactly what the user is asking for
                2. Explain your findings in clear, natural language
                3. Be specific about movies, ratings, or relationships you discover
                4. If you can't find information, suggest alternatives
                """}, 
                {"role": "user", "content": query}
            ]
        }
    
    def query_movie_graph(query: str):
        try:
            # Process the query
            response = text_to_aql_to_text(query)
            
            # If the response is already formatted (e.g., with visualization)
            if isinstance(response, dict):
                return response
            
            # Otherwise, format it with the LLM for better natural language
            input_data = {
                "messages": [
                    {"role": "system", "content": """You are an expert movie recommendation assistant.
                    Format the following information into a helpful, conversational response.
                    Keep your tone friendly but informative."""},
                    {"role": "user", "content": f"Query: {query}\n\nRaw response: {response}"}
                ]
            }
            
            final_response = llm.invoke(input_data["messages"])
            return final_response.content
        except Exception as e:
            return f"I encountered an error while processing your request: {str(e)}. Could you try rephrasing your question?"
    
    return query_movie_graph

def extract_movie_titles(query):
    """Extract potential movie titles from a query using common patterns"""
    common_titles = {
        "toy story": ["toy story", "toy stories", "toystory", "toy storey"],
        "star wars": ["star wars", "starwars", "star war"],
        "the matrix": ["matrix", "the matrix"],
        "jurassic park": ["jurassic park", "jurassic"],
        "pulp fiction": ["pulp fiction", "pulpfiction"],
        "the godfather": ["godfather", "the godfather"],
        "titanic": ["titanic"],
        "inception": ["inception"],
        "avatar": ["avatar"],
        "the dark knight": ["dark knight", "the dark knight", "batman dark knight"]
    }
    
    # Check for common titles with fuzzy matching
    found_titles = []
    query_lower = query.lower()
    
    for standard_title, variations in common_titles.items():
        if any(variation in query_lower for variation in variations):
            found_titles.append(standard_title)
    
    return found_titles

def handle_similar_movies_query(db_client, movie_title, llm):
    """Handle a query about movies similar to a specific movie"""
    try:
        find_movie_query = f"""
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie' AND LOWER(movie.title) LIKE LOWER("%{movie_title}%")
        LIMIT 1
        RETURN movie
        """
        
        movie_result = list(db_client.aql.execute(find_movie_query))
        
        if not movie_result:
            find_movie_query = f"""
            FOR movie IN MovieLens_node
            FILTER movie.type == 'movie' 
            FILTER CONTAINS(LOWER(movie.title), SPLIT(LOWER("{movie_title}"), " ")[0])
            SORT movie.popularity DESC
            LIMIT 1
            RETURN movie
            """
            movie_result = list(db_client.aql.execute(find_movie_query))
        
        if movie_result:
            source_movie = movie_result[0]
            movie_id = source_movie["_id"]            
            similar_movies_query = f"""
            FOR movie IN MovieLens_node
            FILTER movie._id == "{movie_id}"
            LET similar = (
                FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
                FILTER e.type == 'similar_to'
                SORT e.similarity DESC
                LIMIT 10
                RETURN {{
                    movie: v,
                    similarity: e.similarity
                }}
            )
            RETURN {{
                source_movie: movie.title,
                similar_movies: similar
            }}
            """
            
            similar_result = list(db_client.aql.execute(similar_movies_query))
            
            if similar_result and similar_result[0].get("similar_movies"):
                # Format the results
                source_title = similar_result[0]["source_movie"]
                similar_movies = similar_result[0]["similar_movies"]
                
                response_text = f"Here are movies similar to {source_title}:\n\n"
                for movie in similar_movies:
                    title = movie["movie"].get("title", "Unnamed movie")
                    similarity = movie["similarity"]
                    response_text += f"- {title} (similarity: {similarity:.2f})\n"
                
                return response_text
            else:
                return handle_similar_movies_by_genre(db_client, source_movie)
        else:
            return f"I couldn't find information about '{movie_title}'. Could you check the spelling or try another movie title?"
    
    except Exception as e:
        print(f"Error in similar movies query: {e}")
        return f"I had trouble finding movies similar to '{movie_title}'. The database might not have information about this movie or its relationships."

def handle_similar_movies_by_genre(db_client, source_movie):
    """Fallback method to find similar movies by matching genres"""
    try:
        movie_id = source_movie["_id"]
        movie_title = source_movie.get("title", "this movie")        
        genre_query = f"""
        FOR movie IN MovieLens_node
        FILTER movie._id == "{movie_id}"
        LET genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        RETURN {{
            title: movie.title,
            genres: genres
        }}
        """
        
        genre_result = list(db_client.aql.execute(genre_query))
        
        if genre_result and genre_result[0].get("genres"):
            genres = genre_result[0]["genres"]            
            similar_query = f"""
            FOR movie IN MovieLens_node
            FILTER movie.type == 'movie' AND movie._id != "{movie_id}"
            LET movie_genres = (
                FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
                FILTER e.type == 'belongs_to' AND v.type == 'genre'
                RETURN v.name
            )
            LET common_genres = LENGTH(
                FOR genre IN movie_genres
                FILTER genre IN {json.dumps(genres)}
                RETURN genre
            )
            FILTER common_genres > 0
            SORT common_genres DESC, movie.popularity DESC
            LIMIT 10
            RETURN {{
                title: movie.title,
                year: movie.year,
                common_genre_count: common_genres,
                movie_genres: movie_genres
            }}
            """
            
            similar_result = list(db_client.aql.execute(similar_query))
            
            if similar_result:
                response_text = f"Based on genres similar to {movie_title} ({', '.join(genres)}), you might enjoy:\n\n"
                
                for movie in similar_result:
                    title = movie.get("title", "Unnamed movie")
                    year = movie.get("year", "")
                    common_count = movie.get("common_genre_count", 0)
                    movie_genres = movie.get("movie_genres", [])
                    
                    year_str = f" ({year})" if year else ""
                    response_text += f"- {title}{year_str} - Shares {common_count} genres: {', '.join(movie_genres[:3])}\n"
                
                return response_text
            
        return f"I couldn't find movies similar to {movie_title} based on available data."
    
    except Exception as e:
        print(f"Error in genre-based similarity: {e}")
        return f"I had trouble finding movies similar to {source_movie.get('title', 'this movie')} by genre matching."

def handle_genre_query(db_client, genre, is_top_rated=False, llm=None):
    """Handle a query about movies in a specific genre"""
    try:
        if is_top_rated:
            genre_query = f"""
            FOR movie IN MovieLens_node
            FILTER movie.type == 'movie'
            LET genres = (
                FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
                FILTER e.type == 'belongs_to' AND v.type == 'genre'
                RETURN v.name
            )
            LET is_target_genre = (
                FOR g IN genres
                FILTER LOWER(g) == LOWER("{genre}")
                RETURN 1
            )
            LET ratings = (
                FOR r IN MovieLens_node_to_MovieLens_node
                FILTER r.type == 'rated' AND r._to == movie._id
                RETURN r.rating
            )
            LET avg_rating = LENGTH(ratings) > 0 ? AVG(ratings) : null
            LET rating_count = LENGTH(ratings)
            
            FILTER LENGTH(is_target_genre) > 0
            FILTER rating_count >= 5
            FILTER avg_rating != null
            
            SORT avg_rating DESC
            LIMIT 10
            
            RETURN {{
                title: movie.title,
                year: movie.year,
                average_rating: avg_rating,
                number_of_ratings: rating_count
            }}
            """
        else:
            genre_query = f"""
            FOR movie IN MovieLens_node
            FILTER movie.type == 'movie'
            LET genres = (
                FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
                FILTER e.type == 'belongs_to' AND v.type == 'genre'
                RETURN v.name
            )
            LET is_target_genre = (
                FOR g IN genres
                FILTER LOWER(g) == LOWER("{genre}")
                RETURN 1
            )
            
            FILTER LENGTH(is_target_genre) > 0
            SORT movie.popularity DESC
            LIMIT 10
            
            RETURN {{
                title: movie.title,
                year: movie.year,
                genres: genres
            }}
            """
        
        genre_results = list(db_client.aql.execute(genre_query))
        
        if genre_results:
            response_text = f"{'Top rated' if is_top_rated else 'Popular'} {genre.title()} movies:\n\n"
            
            for movie in genre_results:
                title = movie.get("title", "Unnamed movie")
                year = movie.get("year", "")
                year_str = f" ({year})" if year else ""
                
                if is_top_rated:
                    avg_rating = movie.get("average_rating", 0)
                    num_ratings = movie.get("number_of_ratings", 0)
                    response_text += f"- {title}{year_str} - Rating: {avg_rating:.1f} ({num_ratings} ratings)\n"
                else:
                    genres = movie.get("genres", [])
                    genre_str = f" - Genres: {', '.join(genres[:3])}" if genres else ""
                    response_text += f"- {title}{year_str}{genre_str}\n"
            
            return response_text
        else:
            return f"I couldn't find any {genre} movies in the database. Would you like to try a different genre?"
    
    except Exception as e:
        print(f"Error in genre query: {e}")
        return f"I had trouble finding information about {genre} movies. The database might not have information about this genre."

def handle_year_query(db_client, year_str, llm=None):
    """Handle a query about movies from a specific year or decade"""
    try:
        if len(year_str) == 4:
            year = int(year_str)
            start_year = year
            end_year = year
            period_name = str(year)
        else:  # Decade (e.g., "1990s")
            start_year = int(year_str[:4])
            end_year = start_year + 9
            period_name = f"{year_str[:4]}s"
        
        year_query = f"""
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        FILTER HAS(movie, "year") AND movie.year >= {start_year} AND movie.year <= {end_year}
        
        LET ratings = (
            FOR r IN MovieLens_node_to_MovieLens_node
            FILTER r.type == 'rated' AND r._to == movie._id
            RETURN r.rating
        )
        LET avg_rating = LENGTH(ratings) > 0 ? AVG(ratings) : null
        LET rating_count = LENGTH(ratings)
        
        FILTER rating_count > 0
        
        SORT movie.popularity DESC
        LIMIT 15
        
        RETURN {{
            title: movie.title,
            year: movie.year,
            popularity: movie.popularity,
            average_rating: avg_rating,
            rating_count: rating_count
        }}
        """
        
        year_results = list(db_client.aql.execute(year_query))
        
        if year_results:
            response_text = f"Popular movies from the {period_name}:\n\n"
            
            for movie in year_results:
                title = movie.get("title", "Unnamed movie")
                year = movie.get("year", "")
                avg_rating = movie.get("average_rating", 0)
                rating_str = f" - Rating: {avg_rating:.1f}" if avg_rating else ""
                
                response_text += f"- {title} ({year}){rating_str}\n"
            
            return response_text
        else:
            relaxed_query = f"""
            FOR movie IN MovieLens_node
            FILTER movie.type == 'movie'
            FILTER HAS(movie, "year") AND movie.year >= {start_year} AND movie.year <= {end_year}
            SORT movie.popularity DESC
            LIMIT 10
            RETURN {{
                title: movie.title,
                year: movie.year
            }}
            """
            
            relaxed_results = list(db_client.aql.execute(relaxed_query))
            
            if relaxed_results:
                response_text = f"Some movies from the {period_name}:\n\n"
                
                for movie in relaxed_results:
                    title = movie.get("title", "Unnamed movie")
                    year = movie.get("year", "")
                    
                    response_text += f"- {title} ({year})\n"
                
                return response_text
            else:
                return f"I couldn't find any movies from the {period_name} in the database."
    
    except Exception as e:
        print(f"Error in year query: {e}")
        return f"I had trouble finding movies from {year_str}. The database might not have comprehensive information for this time period."

def standard_query_processing(db_client, query, llm):
    """Process other types of queries using the standard AQL generation approach"""
    aql_system_prompt = f"""
    You are an expert in translating natural language to ArangoDB AQL queries for a MovieLens graph database.
    
    The MovieLens graph has:
    - Nodes of type 'movie', 'user', 'genre', and 'tag' in collection 'MovieLens_node'
    - Edges in collection 'MovieLens_node_to_MovieLens_node' with types 'rated', 'belongs_to', 'has_tag', 'similar_to'
    
    CRITICAL REQUIREMENTS:
    1. Use HAS(node, "attribute") before accessing attributes
    2. Use LOWER() for case-insensitive string comparisons
    3. Use proper traversal with FOR v, e IN 1..1 OUTBOUND/INBOUND
    4. Put SORT before RETURN and after FILTER
    5. ROUND() takes only one argument
    
    Your task is to return ONLY a valid AQL query that will work with this schema.
    """

    aql_messages = [
        {"role": "system", "content": aql_system_prompt},
        {"role": "user", "content": f"Create an AQL query for: {query}"}
    ]
    aql_response = llm.invoke(aql_messages)

    aql_query = aql_response.content.strip()
    if "```aql" in aql_query:
        aql_query = aql_query.split("```aql")[1].split("```")[0].strip()
    elif "```" in aql_query:
        aql_query = aql_query.split("```")[1].split("```")[0].strip()
    
    try:
        print(f"Executing AQL query: {aql_query}")
        result = db_client.aql.execute(aql_query)
        result_data = list(result)
        
        # If empty results, try a simpler query
        if not result_data:
            print("No results found, trying simplified query...")
            return handle_empty_results(db_client, query)
        
        nl_format_prompt = f"""
        You are an expert in explaining database query results in natural language.
        
        The following data is the result of a query about movies from the MovieLens database.
        Convert these results into a clear, natural language response.
        
        Original query: {query}
        
        Results: {json.dumps(result_data, default=str)}
        
        Respond as if directly to the user, keeping your response concise but informative.
        """

        format_messages = [
            {"role": "system", "content": nl_format_prompt},
            {"role": "user", "content": "Format these results as a natural language response"}
        ]

        final_response = llm.invoke(format_messages)
        return final_response.content
        
    except Exception as e:
        print(f"Error executing AQL query: {str(e)}")
        
        if "syntax error" in str(e).lower():
            return handle_empty_results(db_client, query)
        
        return f"I couldn't find the information you requested about {query}. Could you try rephrasing your question?"

def handle_empty_results(db_client, query):
    """Handle queries that returned no results by using a more general approach"""
    movie_terms = ["movie", "film", "watch", "cinema"]
    genre_terms = ["genre", "category", "type", "comedy", "action", "drama", "horror"]
    rating_terms = ["rate", "rating", "score", "popular", "best", "top", "highest"]
    
    has_movie_terms = any(term in query.lower() for term in movie_terms)
    has_genre_terms = any(term in query.lower() for term in genre_terms)
    has_rating_terms = any(term in query.lower() for term in rating_terms)
    
    if has_movie_terms and has_rating_terms:
        simple_query = """
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        SORT movie.popularity DESC
        LIMIT 10
        RETURN {
            title: movie.title,
            year: HAS(movie, "year") ? movie.year : null
        }
        """
    elif has_movie_terms and has_genre_terms:
        simple_query = """
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        LET genres = (
            FOR v, e IN 1..1 OUTBOUND movie MovieLens_node_to_MovieLens_node
            FILTER e.type == 'belongs_to' AND v.type == 'genre'
            RETURN v.name
        )
        FILTER LENGTH(genres) > 0
        SORT movie.popularity DESC
        LIMIT 10
        RETURN {
            title: movie.title,
            year: HAS(movie, "year") ? movie.year : null,
            genres: genres
        }
        """
    else:
        simple_query = """
        FOR movie IN MovieLens_node
        FILTER movie.type == 'movie'
        SORT movie.popularity DESC
        LIMIT 10
        RETURN {
            title: movie.title,
            year: HAS(movie, "year") ? movie.year : null
        }
        """
    
    try:
        print(f"Executing simplified query: {simple_query}")
        result = db_client.aql.execute(simple_query)
        result_data = list(result)
        
        if result_data:
            if has_rating_terms:
                response = "Here are some popular movies you might be interested in:\n\n"
            elif has_genre_terms:
                response = "Here are some movies with their genres:\n\n"
            else:
                response = "Here are some popular movies from the database:\n\n"
            
            for movie in result_data:
                title = movie.get("title", "Unnamed movie")
                year = movie.get("year", "")
                year_str = f" ({year})" if year else ""
                
                if has_genre_terms and "genres" in movie:
                    genres = movie.get("genres", [])
                    genre_str = f" - Genres: {', '.join(genres[:3])}" if genres else ""
                    response += f"- {title}{year_str}{genre_str}\n"
                else:
                    response += f"- {title}{year_str}\n"
            
            response += "\nI couldn't find exact matches for your query, so I've shown some popular movies instead."
            return response
        else:
            return "I couldn't find any movies matching your query in the database. Could you try a different search?"
    
    except Exception as e:
        print(f"Error executing simplified query: {str(e)}")
        return "I'm having trouble retrieving movie information from the database right now. Could you try a simpler query or check back later?"

