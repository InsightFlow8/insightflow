# Frontend Structure

This directory contains the Streamlit frontend application with a simplified structure for better maintainability.

## 📁 Directory Structure

```
frontend/
├── main.py                    # Main application with tabs
├── run_analysis.py           # Dedicated analysis launcher
├── data_loader.py            # Shared data loading utilities
├── README.md                 # Original README
├── README_STRUCTURE.md       # This file
│
├── analysis/                 # 📊 Analysis Dashboard Components
│   ├── main_analysis.py     # Main analysis dashboard
│   ├── sidebar.py           # Sidebar filters
│   ├── tab1_overview.py     # Overview tab
│   ├── tab2_product_affinity.py
│   ├── tab3_customer_journey.py
│   ├── tab4_lifetime_value.py
│   └── tab5_churn_analysis.py
│
└── chat/                    # 🤖 Chat Interface Components
    └── chat_interface.py    # Chat interface functionality
```

## 🚀 How to Run

### Option 1: Combined Application (Recommended)
```bash
streamlit run main.py
```
This creates a single application with two main tabs:
- **📊 Analysis Dashboard**: All analytics and insights
- **🤖 AI Chat Assistant**: Chat interface for product recommendations

### Option 2: Analysis Dashboard Only
```bash
streamlit run run_analysis.py
```
This runs only the analysis dashboard.

## 🏗️ Architecture Benefits

1. **Simplified Structure**: No complex multi-page setup
2. **Single Application**: Everything in one place with tabs
3. **Easy Navigation**: Switch between analysis and chat using tabs
4. **Modularity**: Analysis and chat components are still separated
5. **Maintainability**: Clear organization with related functionality grouped

## 📊 Analysis Dashboard

The analysis dashboard includes:
- **Overview**: Key metrics and insights
- **Product Affinity**: Product relationship analysis
- **Customer Journey**: User behavior patterns
- **Lifetime Value**: Customer value analysis
- **Churn Analysis**: Customer retention insights

## 🤖 Chat Interface

The chat interface provides:
- **AI-powered recommendations**: Personalized product suggestions
- **Full-width layout**: Optimized for chat experience
- **Scrollable history**: Fixed input with scrollable chat area
- **Quick actions**: Pre-defined example queries
- **Settings panel**: User ID and connection status

## 🔧 Development

To add new features:
1. **New analysis tab**: Add to `analysis/` directory
2. **New chat feature**: Add to `chat/` directory
3. **New main tab**: Add to `main.py` tabs

The structure makes it easy to maintain and extend the application while keeping related functionality grouped together in a single, user-friendly interface. 