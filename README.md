## Running the GrapFlix Movie Recommender System Locally

Now that you have both the frontend and backend code, here's how to set up and run the complete system locally:

### Step 1: Set Up the Backend

1. **Create a Python environment**

```shellscript
mkdir movie-recommender-backend
cd movie-recommender-backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```


2. **Copy the backend files**

Copy all the Python files from the `backend` directory in the code project above into your `movie-recommender-backend` directory.


3. **Install dependencies**

```shellscript
pip install -r requirements.txt
```


4. **Set up environment variables**

Create a `.env` file with your ArangoDB and Anthropic API credentials:

```plaintext
ARANGO_DB_URL=https://your-arangodb-instance.cloud:8529
ARANGO_DB_NAME=MovieLens
ARANGO_DB_USERNAME=root
ARANGO_DB_PASSWORD=your_password
ANTHROPIC_API_KEY=your_anthropic_api_key
```


5. **Run the backend server**

```shellscript
uvicorn app:app --reload
```

Your backend API should now be running at [http://localhost:8000](http://localhost:8000)




### Step 2: Set Up the Frontend

Frontend Repository: https://github.com/Reynxzz/graphflix/tree/main

1. **Create a new Next.js project** (if you haven't already)

```shellscript
npx create-next-app@latest movie-recommender-frontend
cd movie-recommender-frontend
```


2. **Install required dependencies**

```shellscript
npm install lucide-react framer-motion d3 @radix-ui/react-dialog @radix-ui/react-tabs @radix-ui/react-scroll-area @radix-ui/react-avatar @radix-ui/react-slider
```


3. **Install shadcn/ui components**

```shellscript
npx shadcn@latest init
npx shadcn@latest add button badge dialog tabs scroll-area avatar input slider
```


4. **Copy the frontend files**

Copy all the frontend files from your previous conversation into the appropriate directories in your Next.js project.


5. **Create API route handlers**

Copy the API route handlers from the code project above into your `app/api` directory.


6. **Set up environment variables**

Create a `.env.local` file with:

```plaintext
PYTHON_API_URL=http://localhost:8000
```


7. **Run the frontend development server**

```shellscript
npm run dev
```

Your frontend should now be running at [http://localhost:3000](http://localhost:3000)




### Step 3: Connect to ArangoDB

1. **Ensure your ArangoDB instance is running**

If you're using the cloud version, make sure your credentials are correct in the `.env` file.

If you're running locally, start ArangoDB:

```shellscript
arangod --server.endpoint tcp://127.0.0.1:8529
```


2. **Import the MovieLens dataset**

You can use the Python script from your reference code to import the MovieLens dataset into ArangoDB. Make sure to run this script before using the application.




### Step 4: Test the Complete System

1. Open your browser and navigate to [http://localhost:3000](http://localhost:3000)
2. Try searching for movies, exploring the graph visualization, and using the recommendation chat
3. Test the graph analytics features by asking questions about movie connections


## Troubleshooting Common Issues

1. **CORS errors**: If you see CORS errors in the browser console, make sure the backend has CORS properly configured to allow requests from your frontend.
2. **ArangoDB connection issues**: Verify your ArangoDB credentials and make sure the database and collections exist.
3. **API endpoint errors**: Check that the `PYTHON_API_URL` environment variable is correctly set in your frontend.
4. **Missing dependencies**: If you encounter import errors, make sure all required packages are installed.
5. **Graph visualization not working**: The graph visualization requires proper data formatting. Check the console for errors and verify that the graph data is being returned correctly from the backend.