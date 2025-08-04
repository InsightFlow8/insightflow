# Customer Behavior Analysis Dashboard - Frontend

This directory contains the frontend components of a modular Streamlit dashboard for customer behavior analysis. The application follows a clean separation of concerns with analysis functions in the backend and UI components in the frontend.

## Architecture

### Frontend (UI Layer)
- **`main.py`** - Main Streamlit application entry point
- **`data_loader.py`** - Frontend data loading wrapper (imports from backend)
- **`sidebar.py`** - Dashboard filters and sidebar functionality
- **`tab1_overview.py`** - Overview tab with data summary
- **`tab2_product_affinity.py`** - Product affinity analysis tab
- **`tab3_customer_journey.py`** - Customer journey and behavior analysis tab
- **`tab4_lifetime_value.py`** - Customer lifetime value analysis tab
- **`tab5_churn_analysis.py`** - Customer churn analysis tab
- **`tab6_chat_interface.py`** - AI-powered chat interface tab

### Backend (Business Logic Layer)
- **`../backend/analysis.py`** - All analysis functions (product affinity, customer journey, lifetime value, churn)
- **`../backend/data_loader.py`** - Core data loading and preprocessing functions

## Benefits of Backend/Frontend Separation

### 🎯 **Separation of Concerns**
- **Frontend**: Handles UI, user interactions, and data presentation
- **Backend**: Handles business logic, data processing, and analysis algorithms

### 🔧 **Maintainability**
- Analysis functions can be modified without touching UI code
- UI components can be updated without affecting business logic
- Clear boundaries between presentation and computation layers

### 🚀 **Scalability**
- Backend analysis functions can be reused by other applications
- Easy to add new analysis features without UI changes
- Can be extended to support API endpoints in the future

### 🧪 **Testability**
- Backend functions can be unit tested independently
- UI components can be tested separately from business logic
- Easier to mock dependencies for testing

### 🔄 **Reusability**
- Analysis functions can be imported by other projects
- Backend can be used with different frontend frameworks
- Easy to create multiple UI interfaces for the same analysis

## Features

### 📊 Overview Tab
- Data summary statistics
- Department distribution visualization
- Key metrics display

### 🕸️ Product Affinity Tab
- Product pair analysis
- Top products by frequency
- Association rule mining

### 🛤️ Customer Journey Tab
- Customer types analysis (first-time vs repeat)
- Order size distribution
- Item types (new vs reordered)
- Customer order frequency

### 💰 Lifetime Value Tab
- Customer revenue distribution
- Frequency vs order size analysis
- Reorder rate distribution
- Customer value segments

### ⚠️ Churn Analysis Tab
- Days between orders distribution
- Churn risk indicators
- High-risk customer identification

### 🤖 AI Chat Tab
- Interactive chat interface with AI assistant
- Personalized product recommendations
- Product search and information
- Natural language conversation
- Session management and memory

## Usage

To run the dashboard:

```bash
streamlit run main.py
```

## File Structure

```
imba_dashboard/
├── frontend/
│   ├── main.py                 # Main Streamlit app
│   ├── data_loader.py          # Frontend data loading wrapper
│   ├── sidebar.py              # Dashboard filters
│   ├── tab1_overview.py        # Overview tab
│   ├── tab2_product_affinity.py # Product affinity tab
│   ├── tab3_customer_journey.py # Customer journey tab
│   ├── tab4_lifetime_value.py  # Lifetime value tab
│   ├── tab5_churn_analysis.py  # Churn analysis tab
│   └── README.md               # This file
└── backend/
    ├── analysis.py             # All analysis functions
    ├── data_loader.py          # Core data loading
    └── main.py                 # Backend API (if needed)
```

## Dependencies

- streamlit
- pandas
- numpy
- plotly
- collections (Counter)
- requests

## Data Requirements

The dashboard expects the following CSV files in the working directory:
- orders.csv
- products.csv
- departments.csv
- aisles.csv
- order_products__prior.csv
- order_products__train.csv

## Future Enhancements

1. **API Layer**: Convert backend to REST API endpoints
2. **Database Integration**: Replace CSV files with database connections
3. **Real-time Updates**: Add WebSocket support for live data updates
4. **Authentication**: Add user authentication and role-based access
5. **Export Features**: Add data export capabilities
6. **Advanced Analytics**: Add machine learning models for predictions
7. **Enhanced Chat**: Add voice input/output, file uploads, and rich media support
8. **Multi-language Support**: Add support for multiple languages in chat interface 