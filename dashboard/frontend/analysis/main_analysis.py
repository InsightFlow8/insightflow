import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

# Add backend directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

from analysis_athena import AthenaAnalyzer
from tab1_overview import render_overview_tab
from tab2_product_affinity import render_product_affinity_tab
from tab3_customer_journey import render_customer_journey_tab
from tab4_lifetime_value import render_lifetime_value_tab
from tab5_churn_analysis import render_churn_analysis_tab

def set_aws_credentials():
    """Helper function to set AWS credentials from environment variables"""
    import os
    import boto3
    
    # Check if credentials are already set
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')
    
    # Debug: Show all environment variables that start with AWS
    st.write("🔍 **Debug: AWS Environment Variables**")
    aws_env_vars = {k: v for k, v in os.environ.items() if k.startswith('AWS')}
    if aws_env_vars:
        for key, value in aws_env_vars.items():
            # Mask sensitive values
            if 'SECRET' in key or 'KEY' in key:
                masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 8 else '***'
                st.write(f"  - {key}: {masked_value}")
            else:
                st.write(f"  - {key}: {value}")
    else:
        st.write("  - No AWS environment variables found")
    
    if aws_access_key and aws_secret_key:
        # Set credentials in boto3 session
        try:
            session_kwargs = {
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_key
            }
            
            if aws_session_token:
                session_kwargs['aws_session_token'] = aws_session_token
            
            # Test the credentials
            session = boto3.Session(**session_kwargs)
            sts_client = session.client('sts')
            identity = sts_client.get_caller_identity()
            return True
        except Exception as e:
            st.error(f"❌ Failed to validate AWS credentials: {e}")
            return False
    else:
        st.warning("⚠️ AWS credentials not found in environment variables")
        st.info("""
        **To set AWS credentials, you can:**
        
        1. **Set environment variables in your shell:**
           ```bash
           export AWS_ACCESS_KEY_ID="your_access_key"
           export AWS_SECRET_ACCESS_KEY="your_secret_key"
           ```
        
        2. **Set them in your Docker container:**
           ```bash
           docker run -e AWS_ACCESS_KEY_ID=your_key -e AWS_SECRET_ACCESS_KEY=your_secret ...
           ```
        
        3. **Use AWS CLI to configure:**
           ```bash
           aws configure
           ```
        """)
        return False

def test_aws_connection():
    """Test AWS connection and credentials"""
    import boto3
    import os
    
    st.write("🔍 Testing AWS Connection...")
    
    # Check environment variables
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')
    
    # Show all AWS environment variables
    st.write("**Environment Variables Check:**")
    aws_env_vars = {k: v for k, v in os.environ.items() if k.startswith('AWS')}
    if aws_env_vars:
        for key, value in aws_env_vars.items():
            # Mask sensitive values
            if 'SECRET' in key or 'KEY' in key:
                masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 8 else '***'
                st.write(f"  - {key}: {masked_value}")
            else:
                st.write(f"  - {key}: {value}")
    else:
        st.write("  - No AWS environment variables found")
    
    if not aws_access_key or not aws_secret_key:
        st.error("❌ AWS credentials not found in environment variables")
        st.info("""
        **To fix this, you can:**
        
        1. **Set environment variables in your shell before running the app:**
           ```bash
           export AWS_ACCESS_KEY_ID="your_access_key"
           export AWS_SECRET_ACCESS_KEY="your_secret_key"
           ```
        
        2. **Use the manual credential input in the sidebar**
        
        3. **Check if your Docker container has the environment variables set**
        """)
        return False
    
    try:
        # Create session with credentials
        session_kwargs = {
            'aws_access_key_id': aws_access_key,
            'aws_secret_access_key': aws_secret_key
        }
        
        if aws_session_token:
            session_kwargs['aws_session_token'] = aws_session_token
        
        session = boto3.Session(**session_kwargs)
        
        # Test STS
        sts_client = session.client('sts')
        identity = sts_client.get_caller_identity()
        
        # Test S3 access
        s3_client = session.client('s3')
        try:
            s3_client.head_bucket(Bucket='insightflow-dev-raw-bucket')
        except Exception as s3_error:
            st.warning(f"⚠️ S3 bucket access failed: {s3_error}")
        
        # Test Athena access
        athena_client = session.client('athena')
        try:
            # List workgroups to test Athena access
            workgroups = athena_client.list_work_groups()
        except Exception as athena_error:
            st.warning(f"⚠️ Athena access failed: {athena_error}")
        
        return True
        
    except Exception as e:
        st.error(f"❌ AWS connection failed: {e}")
        return False

def test_credentials_simple():
    """Simple test to check if AWS credentials are working"""
    import boto3
    import os
    
    st.write("🔍 **Simple AWS Credentials Test**")
    
    # Check environment variables
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if not aws_access_key or not aws_secret_key:
        st.error("❌ AWS credentials not found in environment variables")
        return False
    
    try:
        # Create session with credentials
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        # Test STS
        sts_client = session.client('sts')
        identity = sts_client.get_caller_identity()
        return True
        
    except Exception as e:
        st.error(f"❌ AWS credentials test failed: {e}")
        return False

def main():
    """Main Streamlit dashboard application - Analysis Home Page"""
    
    # Main dashboard header
    st.title("📊 E-commerce Customer Behavior Analysis")
    st.markdown("Comprehensive analysis of customer behavior, product affinity, and business insights using AWS Athena")
    
    # AWS Connection Status
    st.sidebar.title("🔍 Analysis Dashboard")
    
    # Add test connection button
    if st.sidebar.button("🔌 Test AWS Connection", key="test_aws_connection"):
        test_aws_connection()
    
    # Add simple credentials test
    if st.sidebar.button("🔑 Test Credentials Only", key="test_credentials_simple"):
        test_credentials_simple()
    
    # Add manual credential input as fallback
    with st.sidebar.expander("🔑 Manual AWS Credentials (if needed)"):
        st.write("**Enter AWS credentials manually if environment variables are not working:**")
        
        manual_access_key = st.text_input("AWS Access Key ID", key="manual_access_key", type="password")
        manual_secret_key = st.text_input("AWS Secret Access Key", key="manual_secret_key", type="password")
        manual_region = st.text_input("AWS Region", value="ap-southeast-2", key="manual_region")
        
        if st.button("🔑 Set Manual Credentials", key="set_manual_credentials"):
            if manual_access_key and manual_secret_key:
                # Set environment variables for this session
                os.environ['AWS_ACCESS_KEY_ID'] = manual_access_key
                os.environ['AWS_SECRET_ACCESS_KEY'] = manual_secret_key
                if manual_region:
                    os.environ['AWS_DEFAULT_REGION'] = manual_region
                
                st.success("✅ Manual credentials set successfully!")
                st.rerun()  # Rerun the app to use the new credentials
            else:
                st.error("❌ Please provide both Access Key ID and Secret Access Key")
    
    st.sidebar.markdown("---")

    # Initialize Athena analyzer
    with st.spinner("Initializing Athena analyzer..."):
        try:
            # First, try to set AWS credentials
            # Check if credentials are already set in environment
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            
            if not aws_access_key or not aws_secret_key:
                st.warning("⚠️ AWS credentials not found in environment variables")
                st.info("""
                **Please set your AWS credentials using one of these methods:**
                
                1. **Use the manual credential input in the sidebar** (easiest for testing)
                2. **Set environment variables in your shell:**
                   ```bash
                   export AWS_ACCESS_KEY_ID="your_access_key"
                   export AWS_SECRET_ACCESS_KEY="your_secret_key"
                   ```
                3. **Set them in your Docker container:**
                   ```bash
                   docker run -e AWS_ACCESS_KEY_ID=your_key -e AWS_SECRET_ACCESS_KEY=your_secret ...
                   ```
                """)
                
                # Show current environment variables for debugging
                with st.expander("🔍 Current Environment Variables"):
                    aws_env_vars = {k: v for k, v in os.environ.items() if k.startswith('AWS')}
                    if aws_env_vars:
                        st.write("**Found AWS environment variables:**")
                        for key, value in aws_env_vars.items():
                            if 'SECRET' in key or 'KEY' in key:
                                masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 8 else '***'
                                st.write(f"  - {key}: {masked_value}")
                            else:
                                st.write(f"  - {key}: {value}")
                    else:
                        st.write("**No AWS environment variables found**")
                
                st.stop()  # Stop execution until credentials are provided
            
            # Now initialize the Athena analyzer
            athena_analyzer = AthenaAnalyzer()
            
        except Exception as e:
            st.error(f"❌ Failed to initialize Athena analyzer: {e}")
            
            # Provide helpful error information
            st.info("""
            **Troubleshooting AWS Credentials:**
            
            1. **Environment Variables**: Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
            2. **AWS CLI**: Run `aws configure` to set up default credentials
            3. **IAM Role**: If running on EC2, attach an IAM role
            4. **Profile**: If using named profiles, enter the profile name above
            
            **Required Permissions:**
            - Athena: `AmazonAthenaFullAccess`
            - S3: `AmazonS3FullAccess` (for the insightflow-dev-raw-bucket)
            """)
            
            # Show current AWS configuration
            with st.expander("Current AWS Configuration"):
                # Check for common AWS environment variables
                aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
                aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
                aws_session_token = os.getenv('AWS_SESSION_TOKEN')
                aws_region = os.getenv('AWS_DEFAULT_REGION')
                
                st.write("**Environment Variables:**")
                if aws_access_key:
                    st.write("✅ AWS_ACCESS_KEY_ID found in environment")
                else:
                    st.write("❌ AWS_ACCESS_KEY_ID not found in environment")
                
                if aws_secret_key:
                    st.write("✅ AWS_SECRET_ACCESS_KEY found in environment")
                else:
                    st.write("❌ AWS_SECRET_ACCESS_KEY not found in environment")
                
                if aws_session_token:
                    st.write("✅ AWS_SESSION_TOKEN found in environment")
                else:
                    st.write("❌ AWS_SESSION_TOKEN not found in environment")
                
                if aws_region:
                    st.write(f"✅ AWS_DEFAULT_REGION found: {aws_region}")
                else:
                    st.write("❌ AWS_DEFAULT_REGION not found in environment")
                
                # Try to check AWS credentials file
                import boto3
                try:
                    session = boto3.Session()
                    sts_client = session.client('sts')
                    identity = sts_client.get_caller_identity()
                    st.write(f"✅ AWS Identity: {identity.get('Arn', 'Unknown')}")
                except Exception as aws_error:
                    st.write(f"❌ AWS Credentials Error: {aws_error}")
            
            return
    
    # Render sidebar and get filters
    st.sidebar.header("📊 Filters")
    
    # Materialized View Control
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔄 Materialized Views")
    force_refresh_materialized_views = st.sidebar.checkbox(
        "Force refresh materialized views",
        value=False,
        help="Check this to drop and recreate materialized views. Uncheck to use existing views for better performance."
    )
    
    if force_refresh_materialized_views:
        st.sidebar.warning("⚠️ Materialized views will be refreshed (slower but ensures latest data)")
    else:
        st.sidebar.info("📋 Using existing materialized views (faster performance)")
    
    # Materialized View Status
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 View Status")
    
    # Check if materialized views exist
    try:
        view_status = athena_analyzer.get_materialized_view_status()
        
        for view_name, status in view_status.items():
            st.sidebar.text(f"{view_name.replace('_', ' ').title()}: {status}")
            
    except Exception as e:
        st.sidebar.warning("⚠️ Could not check view status")
    
    st.sidebar.markdown("---")
    
    # Get departments from Athena
    try:
        departments_df = athena_analyzer.get_departments(use_cache=True)
        if len(departments_df) > 0:
            # Create department options including "All Departments"
            department_options = ["All Departments"] + departments_df['department'].tolist()
            selected_departments = st.sidebar.multiselect(
                "Select Departments",
                options=department_options,
                default=["All Departments"]
            )
            
            # Show department count in sidebar
            st.sidebar.info(f"📁 **{len(departments_df)} departments available**")
        else:
            # Fallback if no departments found
            selected_departments = st.sidebar.multiselect(
                "Select Departments",
                ["All Departments"],
                default=["All Departments"]
            )
            st.sidebar.warning("⚠️ No departments found in database")
    except Exception as e:
        st.sidebar.warning(f"Could not load departments: {e}")
        selected_departments = st.sidebar.multiselect(
            "Select Departments",
            ["All Departments"],
            default=["All Departments"]
        )
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "🕸️ Product Affinity", 
        "🛤️ Customer Journey", 
        "💰 Lifetime Value", 
        "⚠️ Churn Analysis"
    ])
    
    # Render each tab with Athena analyzer
    with tab1:
        render_overview_tab(athena_analyzer, selected_departments)
    
    with tab2:
        render_product_affinity_tab(athena_analyzer, selected_departments, force_refresh_materialized_views)
    
    with tab3:
        render_customer_journey_tab(athena_analyzer, force_refresh_materialized_views)
    
    with tab4:
        render_lifetime_value_tab(athena_analyzer, force_refresh_materialized_views)
    
    with tab5:
        render_churn_analysis_tab(athena_analyzer, force_refresh_materialized_views)
    
    # Footer
    st.markdown("---")
    st.markdown("📊 **Dashboard created with Streamlit & Plotly** | Data: E-commerce Customer Behavior")

if __name__ == "__main__":
    main()