# Customer Behavior Analysis Dashboard - Backend

This directory contains the backend business logic and analysis functions for the Customer Behavior Analysis Dashboard. The backend follows a clean architecture pattern with separation of concerns.

## Architecture

### Core Modules

- **`analysis.py`** - Contains all analysis functions and business logic
- **`data_loader.py`** - Handles data loading, preprocessing, and data management
- **`main.py`** - Backend API server (if needed for future expansion)

## Analysis Functions

### Product Affinity Analysis
- `create_product_affinity_simple()` - Analyzes product pairs frequently purchased together
- Identifies top products by frequency
- Creates association rules for product recommendations

### Customer Journey Analysis
- `create_customer_journey_flow()` - Analyzes customer behavior patterns
- Distinguishes between first-time and repeat customers
- Analyzes order size distributions and reorder behavior

### Lifetime Value Analysis
- `create_lifetime_value_analysis()` - Customer segmentation and value analysis
- Calculates customer metrics (total orders, items, reorder rates)
- Creates visualizations for customer value segments

### Churn Analysis
- `create_churn_analysis()` - Customer churn prediction and risk assessment
- Analyzes ordering patterns over time
- Identifies customers at risk of churning

## Data Management

### Data Loading
- `load_data()` - Loads and preprocesses all CSV files
- Handles data merging and initial cleaning
- Supports data sampling for performance optimization

## Benefits of Backend Architecture

### 🔧 **Maintainability**
- Centralized business logic
- Easy to modify analysis algorithms
- Clear separation from UI concerns

### 🚀 **Scalability**
- Functions can be reused across different frontends
- Easy to add new analysis features
- Can be extended to support API endpoints

### 🧪 **Testability**
- Pure functions with clear inputs/outputs
- Easy to unit test individual functions
- Can be tested independently of UI

### 🔄 **Reusability**
- Analysis functions can be imported by other projects
- Can be used with different data sources
- Easy to create multiple interfaces

## Usage

### Direct Import
```python
import sys
import os
sys.path.append('path/to/backend')
from analysis import create_product_affinity_simple
from data_loader import load_data

# Load data
df, orders, products, departments, aisles = load_data()

# Run analysis
pair_counts, product_names, product_counts = create_product_affinity_simple(df)
```

### From Frontend
The frontend automatically imports backend functions through the modular structure.

## Dependencies

- pandas
- numpy
- plotly
- collections (Counter)

## Data Requirements

The backend expects the following CSV files:
- orders.csv
- products.csv
- departments.csv
- aisles.csv
- order_products__prior.csv
- order_products__train.csv

## Future Enhancements

1. **API Layer**: Convert to REST API with FastAPI or Flask
2. **Database Integration**: Replace CSV files with database connections
3. **Caching**: Add Redis caching for improved performance
4. **Async Processing**: Add async support for large datasets
5. **Machine Learning**: Integrate ML models for predictions
6. **Real-time Analytics**: Add streaming data processing capabilities

## Testing

Each analysis function can be tested independently:

```python
import pytest
from analysis import create_product_affinity_simple

def test_product_affinity():
    # Create test data
    test_df = create_test_data()
    
    # Run analysis
    pair_counts, product_names, product_counts = create_product_affinity_simple(test_df)
    
    # Assert expected results
    assert len(pair_counts) > 0
    assert len(product_counts) > 0
```

## Performance Considerations

- Analysis functions are optimized for large datasets
- Data sampling options available for faster processing
- Memory-efficient data structures used
- Caching implemented where appropriate 

# Product Recommendation System - Backend

This directory contains the modular backend for the Product Recommendation System, featuring AI-powered product recommendations, vector search, and conversational agents.

## 🏗️ Architecture

### Core Modules

- **`main_new.py`** - Main application entry point that orchestrates all components
- **`routes.py`** - FastAPI routes and HTTP endpoint handlers
- **`ml_model.py`** - ALS (Alternating Least Squares) recommendation model
- **`vector_store.py`** - Product embeddings and similarity search
- **`tools.py`** - LangChain tools for product recommendations and search
- **`ai_agent.py`** - Conversational AI agent with memory management
- **`analysis.py`** - Customer behavior analysis functions (for dashboard)
- **`data_loader.py`** - Data loading and preprocessing functions

### Legacy Files

- **`main.py`** - Original monolithic backend file (kept for reference)

## 🚀 Features

### 🤖 AI-Powered Recommendations
- **ALS Model**: Collaborative filtering for personalized recommendations
- **Vector Search**: Semantic product search using embeddings
- **Conversational Agent**: Natural language interaction with product database

### 📊 Analytics & Insights
- **Product Affinity**: Discover products frequently purchased together
- **Customer Journey**: Analyze customer behavior patterns
- **Lifetime Value**: Customer segmentation and value analysis
- **Churn Analysis**: Identify customers at risk of churning

### 🔧 System Management
- **Health Checks**: System status monitoring
- **Model Retraining**: Manual trigger for model updates
- **Session Management**: Conversation history and memory
- **Error Handling**: Robust error management and fallbacks

## 📁 Module Breakdown

### `main_new.py`
- **Purpose**: Application entry point and system orchestration
- **Responsibilities**: 
  - Initialize all system components
  - Handle startup events
  - Configure logging
  - Start FastAPI server

### `routes.py`
- **Purpose**: HTTP endpoint definitions and request handling
- **Endpoints**:
  - `GET /` - Serve frontend HTML
  - `GET /health` - System health check
  - `POST /chat` - Process chat queries
  - `POST /retrain` - Retrain ML models
  - `POST /clear_history` - Clear conversation history

### `ml_model.py`
- **Purpose**: Machine learning model management
- **Features**:
  - ALS model training and loading
  - User-product recommendation generation
  - Similar user identification
  - Model persistence and caching

### `vector_store.py`
- **Purpose**: Product embeddings and semantic search
- **Features**:
  - Product data loading and preprocessing
  - Vector store initialization
  - Qdrant collection management
  - Semantic similarity search

### `tools.py`
- **Purpose**: LangChain tools for AI agent
- **Tools**:
  - Product recommendations
  - Product search
  - Product details retrieval
  - Similar user analysis

### `ai_agent.py`
- **Purpose**: Conversational AI agent management
- **Features**:
  - Agent creation and configuration
  - Conversation memory management
  - Session handling
  - Query processing and response generation

### `analysis.py`
- **Purpose**: Customer behavior analysis (for dashboard)
- **Analyses**:
  - Product affinity analysis
  - Customer journey mapping
  - Lifetime value calculation
  - Churn prediction

### `data_loader.py`
- **Purpose**: Data loading and preprocessing
- **Features**:
  - CSV file loading
  - Data merging and cleaning
  - Data sampling for performance

## 🔄 Data Flow

```
User Query → routes.py → ai_agent.py → tools.py → ml_model.py/vector_store.py → Response
```

1. **User sends query** via `/chat` endpoint
2. **Routes** validate and forward to AI agent
3. **AI Agent** processes query using appropriate tools
4. **Tools** call ML model or vector store as needed
5. **Response** is formatted and returned to user

## 🛠️ Usage

### Running the Backend

```bash
# Run the new modular backend
python main_new.py

# Or run the original monolithic version
python main.py
```

### API Endpoints

```bash
# Health check
curl http://localhost:8000/health

# Chat with AI agent
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "What should I buy?", "user_id": "123"}'


# Clear conversation history
curl -X POST http://localhost:8000/clear_history \
  -H "Content-Type: application/json" \
  -d '{"session_id": "uuid-here"}'
```

## 📊 Dependencies

### Core Dependencies
- **FastAPI**: Web framework
- **Uvicorn**: ASGI server
- **Pandas**: Data manipulation
- **NumPy**: Numerical computing
- **Scipy**: Sparse matrix operations

### AI/ML Dependencies
- **LangChain**: AI agent framework
- **OpenAI**: LLM integration
- **Qdrant**: Vector database
- **Implicit**: ALS recommendation model

### Data Processing
- **Pickle**: Model serialization
- **UUID**: Session management

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the `backend/` directory using the template:

```bash
# Copy template and configure
cp env_template.txt .env
```

Required variables:
```bash
# OpenAI API Configuration (REQUIRED)
OPENAI_API_KEY=your_openai_api_key_here

# Vector Store Configuration
QDRANT_HOST=qdrant
QDRANT_PORT=6333

# Server Configuration
HOST=0.0.0.0
PORT=8000

# Logging Configuration
LOG_LEVEL=INFO

# Model Configuration
ALS_MODEL_PATH=als_model.pkl
VECTOR_STORE_COLLECTION=products_collection
```

See `SETUP.md` for detailed setup instructions.

### Data Requirements
The system expects the following CSV files in the `data/` directory:
- `orders.csv`
- `products.csv`
- `departments.csv`
- `aisles.csv`
- `order_products__prior.csv.gz`

## 🧪 Testing

### Unit Tests
Each module can be tested independently:

```python
# Test ML model
from ml_model import recommend_for_user
recommendations = recommend_for_user(123, N=5)

# Test vector store
from vector_store import get_vector_store
vectorstore = get_vector_store()
results = vectorstore.similarity_search("organic fruits", k=5)

# Test AI agent
from ai_agent import process_chat_query
response = process_chat_query("What should I buy?", user_id="123")
```

## 🚀 Benefits of Modular Architecture

### 🔧 **Maintainability**
- Clear separation of concerns
- Easy to modify individual components
- Reduced code complexity

### 🧪 **Testability**
- Each module can be tested independently
- Mock dependencies easily
- Unit test coverage for all components

### 🚀 **Scalability**
- Easy to add new features
- Components can be scaled independently
- Clear interfaces between modules

### 🔄 **Reusability**
- Modules can be used in other projects
- Easy to create different frontends
- API endpoints can be consumed by multiple clients

## 🔮 Future Enhancements

1. **Database Integration**: Replace CSV files with PostgreSQL/MongoDB
2. **Redis Caching**: Add caching for improved performance
3. **Async Processing**: Add async support for large datasets
4. **Microservices**: Split into separate microservices
5. **Docker Support**: Containerize each module
6. **Monitoring**: Add Prometheus/Grafana monitoring
7. **Authentication**: Add JWT authentication
8. **Rate Limiting**: Add API rate limiting

## 📈 Performance Considerations

- **Model Caching**: ALS model is cached as pickle file
- **Vector Store**: Qdrant provides fast similarity search
- **Memory Management**: Efficient sparse matrix operations
- **Session Management**: Conversation memory is optimized
- **Error Handling**: Graceful fallbacks for all operations 