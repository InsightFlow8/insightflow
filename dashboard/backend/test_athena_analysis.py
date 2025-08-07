import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import logging
from typing import Dict, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AthenaAnalyzer:
    def __init__(self, region='ap-southeast-2', database='insightflow_imba_raw_data_catalog', profile_name='insightflow'):
        # Create boto3 session with the specified profile
        session = boto3.Session(profile_name=profile_name, region_name=region)
        self.athena_client = session.client('athena')
        self.s3_client = session.client('s3')
        self.database = database
        self.output_location = 's3://insightflow-dev-raw-bucket/athena-results/'
        
        # Test the connection
        try:
            # Test if we can access the S3 bucket
            self.s3_client.head_bucket(Bucket='insightflow-dev-raw-bucket')
            logger.info(f"✅ Successfully connected to S3 bucket: insightflow-dev-raw-bucket")
        except Exception as e:
            logger.warning(f"⚠️ Could not access S3 bucket: {e}")
            # Try to create the athena-results directory if it doesn't exist
            try:
                self.s3_client.put_object(
                    Bucket='insightflow-dev-raw-bucket',
                    Key='athena-results/'
                )
                logger.info("✅ Created athena-results directory in S3 bucket")
            except Exception as create_error:
                logger.error(f"❌ Could not create athena-results directory: {create_error}")
        
    def execute_query(self, query: str, query_name: str = "analysis") -> pd.DataFrame:
        """Execute Athena query and return results as DataFrame"""
        try:
            logger.info(f"Executing Athena query: {query_name}")
            
            # Start query execution using default workgroup
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"Query execution ID: {query_execution_id}")
            
            # Wait for query to complete
            while True:
                query_status = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )['QueryExecution']['Status']['State']
                
                if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                    
                time.sleep(2)
            
            if query_status == 'SUCCEEDED':
                # Get results
                results = self.athena_client.get_query_results(
                    QueryExecutionId=query_execution_id
                )
                
                # Convert to DataFrame
                df = self._convert_results_to_dataframe(results)
                logger.info(f"Query completed successfully. Returned {len(df)} rows")
                return df
            else:
                error_msg = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Query failed: {error_msg}")
                
        except Exception as e:
            logger.error(f"Error executing Athena query: {e}")
            raise
    
    def _convert_results_to_dataframe(self, results: Dict[str, Any]) -> pd.DataFrame:
        """Convert Athena results to pandas DataFrame"""
        if 'ResultSet' not in results or 'Rows' not in results['ResultSet']:
            return pd.DataFrame()
        
        rows = results['ResultSet']['Rows']
        if len(rows) < 2:  # No data rows
            return pd.DataFrame()
        
        # Get column names from header row
        columns = [col['VarCharValue'] for col in rows[0]['Data']]
        
        # Get data rows
        data_rows = []
        for row in rows[1:]:
            data_row = []
            for col in row['Data']:
                value = col.get('VarCharValue', '')
                data_row.append(value)
            data_rows.append(data_row)
        
        df = pd.DataFrame(data_rows, columns=columns)
        
        # Convert numeric columns
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        return df
    
    def create_product_affinity_analysis(self, top_products: int = 20) -> Tuple[go.Figure, pd.DataFrame]:
        """Create product affinity analysis using Athena"""
        query = f"""
        WITH product_pairs AS (
          SELECT 
            op1.product_id as product1_id,
            op2.product_id as product2_id,
            COUNT(*) as pair_count
          FROM {self.database}.raw_order_products op1
          JOIN {self.database}.raw_order_products op2 
            ON op1.order_id = op2.order_id 
            AND op1.product_id < op2.product_id
          GROUP BY op1.product_id, op2.product_id
          HAVING COUNT(*) > 1
        )
        SELECT 
          pp.product1_id,
          p1.product_name as product1_name,
          pp.product2_id,
          p2.product_name as product2_name,
          pp.pair_count
        FROM product_pairs pp
        JOIN {self.database}.raw_products p1 ON pp.product1_id = p1.product_id
        JOIN {self.database}.raw_products p2 ON pp.product2_id = p2.product_id
        ORDER BY pp.pair_count DESC
        LIMIT {top_products}
        """
        
        df = self.execute_query(query, "product_affinity")
        
        # Create visualization
        fig = px.scatter(
            df.head(50), 
            x='product1_name', 
            y='product2_name', 
            size='pair_count',
            title='Product Affinity Analysis',
            hover_data=['pair_count']
        )
        fig.update_layout(height=600, showlegend=False)
        
        return fig, df
    
    def create_customer_journey_analysis(self) -> Tuple[go.Figure, pd.DataFrame]:
        """Create customer journey analysis using Athena"""
        query = f"""
        SELECT 
          user_id,
          COUNT(DISTINCT order_id) as total_orders,
          COUNT(*) as total_items,
          SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) as reordered_items,
          AVG(add_to_cart_order) as avg_cart_position,
          MAX(order_number) as max_order_number
        FROM {self.database}.raw_order_products op
        JOIN {self.database}.raw_orders o ON op.order_id = o.order_id
        GROUP BY user_id
        ORDER BY total_orders DESC
        LIMIT 1000
        """
        
        df = self.execute_query(query, "customer_journey")
        
        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Customer Order Distribution', 'Order Size vs Items', 
                           'Reorder Rate', 'Customer Segments'),
            specs=[[{"type": "histogram"}, {"type": "scatter"}],
                   [{"type": "histogram"}, {"type": "scatter"}]]
        )
        
        # Customer order distribution
        fig.add_trace(go.Histogram(x=df['total_orders'], name='Orders'), row=1, col=1)
        
        # Order size vs items
        fig.add_trace(go.Scatter(
            x=df['total_orders'], 
            y=df['total_items'], 
            mode='markers',
            name='Order Size vs Items'
        ), row=1, col=2)
        
        # Reorder rate
        df['reorder_rate'] = df['reordered_items'] / df['total_items']
        fig.add_trace(go.Histogram(x=df['reorder_rate'], name='Reorder Rate'), row=2, col=1)
        
        # Customer segments
        fig.add_trace(go.Scatter(
            x=df['total_orders'], 
            y=df['avg_cart_position'], 
            mode='markers',
            marker=dict(color=df['reorder_rate'], colorscale='Viridis'),
            name='Customer Segments'
        ), row=2, col=2)
        
        fig.update_layout(height=600, title_text="Customer Journey Analysis")
        
        return fig, df
    
    def create_lifetime_value_analysis(self) -> Tuple[go.Figure, pd.DataFrame]:
        """Create customer lifetime value analysis using Athena"""
        query = f"""
        WITH customer_metrics AS (
          SELECT 
            user_id,
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(*) as total_items,
            SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) as total_reorders,
            AVG(add_to_cart_order) as avg_order_size,
            MAX(order_number) as max_order_number
          FROM {self.database}.raw_order_products op
          JOIN {self.database}.raw_orders o ON op.order_id = o.order_id
          GROUP BY user_id
        )
        SELECT 
          user_id,
          total_orders,
          total_items,
          total_reorders,
          avg_order_size,
          max_order_number,
          CASE 
            WHEN total_orders >= 10 THEN 'High Value'
            WHEN total_orders >= 5 THEN 'Medium Value'
            ELSE 'Low Value'
          END as customer_segment
        FROM customer_metrics
        ORDER BY total_items DESC
        LIMIT 1000
        """
        
        df = self.execute_query(query, "lifetime_value")
        
        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Customer Value Distribution', 'Orders vs Items', 
                           'Customer Segments', 'Lifetime Value Analysis'),
            specs=[[{"type": "histogram"}, {"type": "scatter"}],
                   [{"type": "pie"}, {"type": "scatter"}]]
        )
        
        # Customer value distribution
        fig.add_trace(go.Histogram(x=df['total_items'], name='Total Items'), row=1, col=1)
        
        # Orders vs items
        fig.add_trace(go.Scatter(
            x=df['total_orders'], 
            y=df['total_items'], 
            mode='markers',
            marker=dict(color=df['avg_order_size'], colorscale='Plasma'),
            name='Orders vs Items'
        ), row=1, col=2)
        
        # Customer segments
        segment_counts = df['customer_segment'].value_counts()
        fig.add_trace(go.Pie(
            labels=segment_counts.index,
            values=segment_counts.values,
            name='Customer Segments'
        ), row=2, col=1)
        
        # Lifetime value analysis
        fig.add_trace(go.Scatter(
            x=df['total_orders'], 
            y=df['avg_order_size'], 
            mode='markers',
            marker=dict(
                size=df['total_reorders'] * 2,
                color=df['total_items'],
                colorscale='Viridis'
            ),
            name='Lifetime Value'
        ), row=2, col=2)
        
        fig.update_layout(height=600, title_text="Customer Lifetime Value Analysis")
        
        return fig, df
    
    def create_churn_analysis(self) -> Tuple[go.Figure, pd.DataFrame]:
        """Create churn prediction indicators using Athena"""
        query = f"""
        SELECT 
          user_id,
          COUNT(DISTINCT order_id) as total_orders,
          AVG(days_since_prior_order) as avg_days_between,
          MAX(days_since_prior_order) as max_days_between,
          STDDEV(days_since_prior_order) as std_days_between
        FROM {self.database}.raw_orders
        WHERE days_since_prior_order IS NOT NULL
        GROUP BY user_id
        ORDER BY total_orders DESC
        LIMIT 1000
        """
        
        df = self.execute_query(query, "churn_analysis")
        
        # Create visualization
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Days Between Orders Distribution', 'Churn Risk Indicators'),
            specs=[[{"type": "histogram"}, {"type": "scatter"}]]
        )
        
        # Days between orders
        fig.add_trace(go.Histogram(x=df['avg_days_between'], name='Days Between Orders'), row=1, col=1)
        
        # Churn risk scatter
        fig.add_trace(go.Scatter(
            x=df['total_orders'],
            y=df['max_days_between'],
            mode='markers',
            marker=dict(
                size=df['std_days_between'] * 10,
                color=df['avg_days_between'],
                colorscale='RdYlGn_r',
                showscale=True
            ),
            name='Churn Risk'
        ), row=1, col=2)
        
        fig.update_layout(height=400, title_text="Customer Churn Analysis")
        
        return fig, df

# Test script for Jupyter notebook
def test_athena_analysis():
    """
    Comprehensive test script for Athena analysis functions.
    Run this in your Jupyter notebook to see all the graphs.
    """
    
    print("🚀 Starting Athena Analysis Test")
    print("=" * 50)
    
    try:
        # Initialize Athena analyzer
        print("📊 Initializing Athena Analyzer...")
        athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
        print("✅ Athena Analyzer initialized successfully")
        
        # Test 1: Product Affinity Analysis
        print("\n🔗 Testing Product Affinity Analysis...")
        try:
            fig1, df1 = athena_analyzer.create_product_affinity_analysis(top_products=20)
            print(f"✅ Product Affinity Analysis completed - {len(df1)} product pairs found")
            print("📊 Displaying Product Affinity Graph:")
            fig1.show()
            
            # Show top product pairs
            print("\n🏆 Top 10 Product Pairs:")
            print(df1.head(10)[['product1_name', 'product2_name', 'pair_count']])
            
        except Exception as e:
            print(f"❌ Product Affinity Analysis failed: {e}")
        
        # Test 2: Customer Journey Analysis
        print("\n👥 Testing Customer Journey Analysis...")
        try:
            fig2, df2 = athena_analyzer.create_customer_journey_analysis()
            print(f"✅ Customer Journey Analysis completed - {len(df2)} customers analyzed")
            print("📊 Displaying Customer Journey Graph:")
            fig2.show()
            
            # Show customer statistics
            print("\n📈 Customer Statistics:")
            print(f"Total customers: {len(df2)}")
            print(f"Average orders per customer: {df2['total_orders'].mean():.2f}")
            print(f"Average items per customer: {df2['total_items'].mean():.2f}")
            
        except Exception as e:
            print(f"❌ Customer Journey Analysis failed: {e}")
        
        # Test 3: Lifetime Value Analysis
        print("\n💰 Testing Lifetime Value Analysis...")
        try:
            fig3, df3 = athena_analyzer.create_lifetime_value_analysis()
            print(f"✅ Lifetime Value Analysis completed - {len(df3)} customers analyzed")
            print("📊 Displaying Lifetime Value Graph:")
            fig3.show()
            
            # Show customer segments
            print("\n🎯 Customer Segments:")
            segment_counts = df3['customer_segment'].value_counts()
            for segment, count in segment_counts.items():
                percentage = (count / len(df3)) * 100
                print(f"  {segment}: {count} customers ({percentage:.1f}%)")
            
        except Exception as e:
            print(f"❌ Lifetime Value Analysis failed: {e}")
        
        # Test 4: Churn Analysis
        print("\n📉 Testing Churn Analysis...")
        try:
            fig4, df4 = athena_analyzer.create_churn_analysis()
            print(f"✅ Churn Analysis completed - {len(df4)} customers analyzed")
            print("📊 Displaying Churn Analysis Graph:")
            fig4.show()
            
            # Show churn statistics
            print("\n⚠️ Churn Risk Statistics:")
            print(f"Average days between orders: {df4['avg_days_between'].mean():.1f} days")
            print(f"Max days between orders: {df4['max_days_between'].max():.1f} days")
            print(f"Customers with high churn risk (>30 days avg): {(df4['avg_days_between'] > 30).sum()}")
            
        except Exception as e:
            print(f"❌ Churn Analysis failed: {e}")
        
        print("\n🎉 All tests completed!")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Failed to initialize Athena Analyzer: {e}")
        print("Please check your AWS credentials and S3 bucket access.")

# Individual test functions for specific analysis
def test_product_affinity():
    """Test only product affinity analysis"""
    print("🔗 Testing Product Affinity Analysis...")
    athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
    fig, df = athena_analyzer.create_product_affinity_analysis(top_products=20)
    print(f"✅ Found {len(df)} product pairs")
    fig.show()
    return fig, df

def test_customer_journey():
    """Test only customer journey analysis"""
    print("👥 Testing Customer Journey Analysis...")
    athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
    fig, df = athena_analyzer.create_customer_journey_analysis()
    print(f"✅ Analyzed {len(df)} customers")
    fig.show()
    return fig, df

def test_lifetime_value():
    """Test only lifetime value analysis"""
    print("💰 Testing Lifetime Value Analysis...")
    athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
    fig, df = athena_analyzer.create_lifetime_value_analysis()
    print(f"✅ Analyzed {len(df)} customers")
    fig.show()
    return fig, df

def test_churn_analysis():
    """Test only churn analysis"""
    print("📉 Testing Churn Analysis...")
    athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
    fig, df = athena_analyzer.create_churn_analysis()
    print(f"✅ Analyzed {len(df)} customers")
    fig.show()
    return fig, df

# Quick connection test
def test_connection():
    """Test Athena connection and basic functionality"""
    print("🔌 Testing Athena Connection...")
    try:
        athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
        
        # Test a simple query
        test_query = "SELECT COUNT(*) as total FROM insightflow_imba_raw_data_catalog.raw_products LIMIT 1"
        result = athena_analyzer.execute_query(test_query, "test_connection")
        
        print("✅ Connection successful!")
        print(f"📊 Test query result: {result}")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    # Run the full test suite
    test_athena_analysis() 