import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from collections import Counter
import warnings
warnings.filterwarnings('ignore')
from cache_manager import analysis_cache

def create_product_affinity_simple(df, top_products=20):
    """Create simplified product affinity analysis with caching"""
    
    # Check cache first
    cached_result = analysis_cache.get_cached_result(
        "product_affinity", df, {"top_products": top_products}
    )
    if cached_result:
        return cached_result
    
    # Get top products by frequency
    product_counts = df['product_id'].value_counts()
    top_product_ids = product_counts.head(top_products).index
    
    # Filter data to top products
    df_top = df[df['product_id'].isin(top_product_ids)]
    
    # Create product pairs within same orders
    product_pairs = []
    for order_id in df_top['order_id'].unique():
        order_products = df_top[df_top['order_id'] == order_id]['product_id'].tolist()
        for i in range(len(order_products)):
            for j in range(i+1, len(order_products)):
                product_pairs.append(tuple(sorted([order_products[i], order_products[j]])))
    
    # Count pair frequencies
    pair_counts = Counter(product_pairs)
    
    # Get product names
    product_names = df[['product_id', 'product_name']].drop_duplicates().set_index('product_id')['product_name']
    
    result = (pair_counts, product_names, product_counts)
    
    # Save to cache
    analysis_cache.save_result(
        "product_affinity", df, result, {"top_products": top_products}
    )
    
    return result

def create_customer_journey_flow(df):
    """Create meaningful customer journey flow diagram with caching"""
    
    # Check cache first
    cached_result = analysis_cache.get_cached_result("customer_journey", df)
    if cached_result:
        return cached_result
    
    # Calculate meaningful customer journey metrics
    total_orders = df['order_id'].nunique()
    total_customers = df['user_id'].nunique()
    
    # Calculate order frequency distribution
    customer_order_counts = df.groupby('user_id')['order_id'].nunique()
    first_time_customers = (customer_order_counts == 1).sum()
    repeat_customers = (customer_order_counts > 1).sum()
    
    # Calculate reorder behavior
    total_items = len(df)
    reordered_items = df['reordered'].sum()
    new_items = total_items - reordered_items
    
    # Calculate order size distribution
    order_sizes = df.groupby('order_id')['add_to_cart_order'].max()
    small_orders = (order_sizes <= 5).sum()  # 1-5 items
    medium_orders = ((order_sizes > 5) & (order_sizes <= 15)).sum()  # 6-15 items
    large_orders = (order_sizes > 15).sum()  # 16+ items
    
    # Create meaningful journey visualization
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Customer Types', 'Order Size Distribution', 
                       'Item Types', 'Customer Order Frequency'),
        specs=[[{"type": "pie"}, {"type": "pie"}],
               [{"type": "bar"}, {"type": "histogram"}]]
    )
    
    # Customer Types (First-time vs Repeat)
    fig.add_trace(go.Pie(
        labels=['Repeat Customers', 'First-time Customers'],
        values=[repeat_customers, first_time_customers],
        name="Customer Types",
        marker_colors=['#66B2FF', '#FF9999'],
        sort=False,
        rotation=180,
    ), row=1, col=1)
    
    # Order Size Distribution
    fig.add_trace(go.Pie(
        labels=['Small Orders (1-5 items)', 'Medium Orders (6-15 items)', 'Large Orders (16+ items)'],
        values=[small_orders, medium_orders, large_orders],
        name="Order Sizes",
        marker_colors=['#FFCC99', '#99FF99', '#FF99CC']
    ), row=1, col=2)
    
    # Item Types (New vs Reordered)
    fig.add_trace(go.Bar(
        x=['New Items', 'Reordered Items'],
        y=[new_items, reordered_items],
        text=[f'{new_items:,}', f'{reordered_items:,}'],
        textposition='auto',
        marker_color=['#FFB366', '#66B2FF'],
        name="Item Types"
    ), row=2, col=1)
    
    # Customer Order Frequency Distribution
    fig.add_trace(go.Histogram(
        x=customer_order_counts,
        nbinsx=20,
        name="Order Frequency",
        marker_color='#99CCFF'
    ), row=2, col=2)
    
    fig.update_layout(
        height=600,
        title_text="Customer Journey & Behavior Analysis",
        showlegend=False
    )
    
    # Update axes labels
    fig.update_xaxes(title_text="Item Type", row=2, col=1)
    fig.update_yaxes(title_text="Number of Items", row=2, col=1)
    fig.update_xaxes(title_text="Number of Orders per Customer", row=2, col=2)
    fig.update_yaxes(title_text="Number of Customers", row=2, col=2)
    
    # Save result to cache
    analysis_cache.save_result("customer_journey", df, fig)
    
    return fig

def create_lifetime_value_analysis(df):
    """Create customer lifetime value analysis with caching"""
    
    # Check cache first
    cached_result = analysis_cache.get_cached_result("lifetime_value", df)
    if cached_result:
        return cached_result
    
    # Calculate customer metrics
    customer_metrics = df.groupby('user_id').agg({
        'order_id': 'nunique',
        'product_id': 'count',
        'reordered': 'sum',
        'add_to_cart_order': 'sum'
    }).reset_index()
    
    customer_metrics.columns = ['user_id', 'total_orders', 'total_items', 'total_reorders', 'total_cart_additions']
    customer_metrics['avg_order_size'] = customer_metrics['total_items'] / customer_metrics['total_orders']
    customer_metrics['reorder_rate'] = customer_metrics['total_reorders'] / customer_metrics['total_items']
    
    # Handle NaN values
    customer_metrics = customer_metrics.fillna(0)
    
    # Create LTV visualization
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Customer Revenue Distribution', 'Frequency vs Order Size', 
                       'Reorder Rate Distribution', 'Customer Value Segments'),
        specs=[[{"type": "histogram"}, {"type": "scatter"}],
               [{"type": "histogram"}, {"type": "scatter"}]]
    )
    
    # Customer revenue distribution (estimated by total items purchased)
    fig.add_trace(go.Histogram(x=customer_metrics['total_items'], name='Total Items Purchased'), row=1, col=1)
    
    # Customer frequency vs order size relationship
    fig.add_trace(go.Scatter(
        x=customer_metrics['total_orders'],
        y=customer_metrics['avg_order_size'],
        mode='markers',
        marker=dict(
            size=8,
            color=customer_metrics['reorder_rate'],
            colorscale='Plasma',
            showscale=True,
            colorbar=dict(
                title="Reorder Rate",
                x=1.02,
                xanchor="left",
                thickness=15,
                len=0.4
            )
        ),
        text=customer_metrics['user_id'],
        name='Frequency vs Order Size'
    ), row=1, col=2)
    
    # Reorder rate
    fig.add_trace(go.Histogram(x=customer_metrics['reorder_rate'], name='Reorder Rate'), row=2, col=1)
    
    # Customer segments scatter - handle NaN values
    marker_sizes = customer_metrics['reorder_rate'] * 50
    marker_sizes = marker_sizes.fillna(5)  # Default size for NaN values
    
    fig.add_trace(go.Scatter(
        x=customer_metrics['total_orders'],
        y=customer_metrics['avg_order_size'],
        mode='markers',
        marker=dict(
            size=marker_sizes,
            color=customer_metrics['reorder_rate'].fillna(0),
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(
                title="Reorder Rate",
                x=1.02,
                xanchor="left",
                thickness=15,
                len=0.4
            )
        ),
        text=customer_metrics['user_id'],
        name='Customer Segments'
    ), row=2, col=2)
    
    fig.update_layout(height=600, title_text="Customer Lifetime Value Analysis")
    
    # Update axes labels for better clarity
    fig.update_xaxes(title_text="Total Items Purchased", row=1, col=1)
    fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
    fig.update_xaxes(title_text="Total Orders", row=1, col=2)
    fig.update_yaxes(title_text="Average Order Size (items)", row=1, col=2)
    fig.update_xaxes(title_text="Reorder Rate", row=2, col=1)
    fig.update_yaxes(title_text="Number of Customers", row=2, col=1)
    
    result = (fig, customer_metrics)
    
    # Save to cache
    analysis_cache.save_result("lifetime_value", df, result)
    
    return result

def create_churn_analysis(df):
    """Create churn prediction indicators with caching"""
    
    # Check cache first
    cached_result = analysis_cache.get_cached_result("churn", df)
    if cached_result:
        return cached_result
    
    # Analyze customer ordering patterns over time
    customer_timeline = df.groupby(['user_id', 'order_number']).agg({
        'order_id': 'first',
        'days_since_prior_order': 'first'
    }).reset_index()
    
    # Calculate churn indicators
    churn_indicators = customer_timeline.groupby('user_id').agg({
        'order_number': 'max',
        'days_since_prior_order': ['mean', 'max', 'std']
    }).reset_index()
    
    churn_indicators.columns = ['user_id', 'total_orders', 'avg_days_between', 'max_days_between', 'std_days_between']
    
    # Handle NaN values
    churn_indicators = churn_indicators.fillna(0)
    
    # Create churn risk visualization
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Days Between Orders Distribution', 'Churn Risk Indicators'),
        specs=[[{"type": "histogram"}, {"type": "scatter"}]]
    )
    
    # Days between orders
    fig.add_trace(go.Histogram(x=churn_indicators['avg_days_between'], name='Days Between Orders'), row=1, col=1)
    
    # Churn risk scatter - handle NaN values
    marker_sizes = churn_indicators['std_days_between'] * 10
    marker_sizes = marker_sizes.fillna(5)  # Default size for NaN values
    
    fig.add_trace(go.Scatter(
        x=churn_indicators['total_orders'],
        y=churn_indicators['max_days_between'],
        mode='markers',
        marker=dict(
            size=marker_sizes,
            color=churn_indicators['avg_days_between'].fillna(0),
            colorscale='RdYlGn_r',
            showscale=True,
            colorbar=dict(
                title="Avg Days Between Orders",
                x=1.02,
                xanchor="left",
                thickness=15,
                len=0.4
            )
        ),
        text=churn_indicators['user_id'],
        name='Churn Risk'
    ), row=1, col=2)
    
    fig.update_layout(height=400, title_text="Customer Churn Analysis")
    
    # Add axis labels for better clarity
    fig.update_xaxes(title_text="Days Between Orders", row=1, col=1)
    fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
    fig.update_xaxes(title_text="Total Orders", row=1, col=2)
    fig.update_yaxes(title_text="Max Days Between Orders", row=1, col=2)
    
    result = (fig, churn_indicators)
    
    # Save to cache
    analysis_cache.save_result("churn", df, result)
    
    return result 