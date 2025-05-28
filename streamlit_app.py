import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum, avg, max, when, desc, lit

# Page configuration
st.set_page_config(
    page_title="Ski Resort Operations Hub",
    page_icon="⛷️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Global configuration
CACHE_TTL = 60  # seconds

# Initialize Snowpark session
@st.cache_resource
def init_session():
    """Initialize Snowpark session."""
    try:
        # Streamlit in Snowflake (SiS) path
        session = get_active_session()
    except Exception:
        # Locally, create a session (ensure your local credentials are set up)
        from snowflake.snowpark.session import Session
        session = Session.builder.create()
        session.use_schema("STREAMING_INGEST.STREAMING_INGEST")
    return session

session = init_session()

# Initialize session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Network Overview"

# Data fetching functions
@st.cache_data(ttl=CACHE_TTL)
def get_network_kpis(time_period: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch network-wide KPIs for current and previous periods."""
    # Define date filters
    date_filters = {
        "Today": {
            "current": col("RIDE_DATE") == session.sql("SELECT CURRENT_DATE()").collect()[0][0],
            "previous": col("RIDE_DATE") == session.sql("SELECT CURRENT_DATE() - 1").collect()[0][0]
        },
        "Last 7 Days": {
            "current": col("RIDE_DATE") >= session.sql("SELECT CURRENT_DATE() - 7").collect()[0][0],
            "previous": (col("RIDE_DATE") >= session.sql("SELECT CURRENT_DATE() - 14").collect()[0][0]) &
                        (col("RIDE_DATE") < session.sql("SELECT CURRENT_DATE() - 7").collect()[0][0])
        },
        "Month to Date": {
            "current": col("RIDE_DATE") >= session.sql("SELECT DATE_TRUNC('month', CURRENT_DATE())").collect()[0][0],
            "previous": (col("RIDE_DATE") >= session.sql("SELECT DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '1 month')").collect()[0][0]) &
                        (col("RIDE_DATE") < session.sql("SELECT DATE_TRUNC('month', CURRENT_DATE())").collect()[0][0])
        }
    }

    current_filter = date_filters[time_period]["current"]
    previous_filter = date_filters[time_period]["previous"]

    # Current period metrics
    current_metrics = session.table("RESORT_DAILY_SUMMARY").filter(current_filter).agg(
        sum(col("PEAK_VISITORS")).alias("total_visitors"),
        sum(col("TOTAL_REVENUE")).alias("total_revenue"),
        avg(col("AVG_CAPACITY_PCT")).alias("avg_capacity"),
        sum(col("TOTAL_RIDES")).alias("total_rides")
    )

    # Previous period metrics
    previous_metrics = session.table("RESORT_DAILY_SUMMARY").filter(previous_filter).agg(
        sum(col("PEAK_VISITORS")).alias("prev_visitors"),
        sum(col("TOTAL_REVENUE")).alias("prev_revenue"),
        sum(col("TOTAL_RIDES")).alias("prev_rides")
    )

    return current_metrics.to_pandas(), previous_metrics.to_pandas()

@st.cache_data(ttl=CACHE_TTL)
def get_resort_comparison(time_period: str) -> pd.DataFrame:
    """Fetch resort comparison data for the specified period."""
    date_filter_map = {
        "Today": col("RIDE_DATE") == session.sql("SELECT CURRENT_DATE()").collect()[0][0],
        "Last 7 Days": col("RIDE_DATE") >= session.sql("SELECT CURRENT_DATE() - 7").collect()[0][0],
        "Month to Date": col("RIDE_DATE") >= session.sql("SELECT DATE_TRUNC('month', CURRENT_DATE())").collect()[0][0]
    }

    return (session.table("RESORT_DAILY_SUMMARY")
            .filter(date_filter_map[time_period])
            .group_by("RESORT")
            .agg(
        sum(col("PEAK_VISITORS")).alias("TOTAL_VISITORS"),
        sum(col("TOTAL_REVENUE")).alias("TOTAL_REVENUE"),
        avg(col("AVG_CAPACITY_PCT")).alias("AVG_CAPACITY"),
        sum(col("TOTAL_RIDES")).alias("TOTAL_RIDES")
    )
            .order_by("RESORT")
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_time_series_data(time_period: str) -> pd.DataFrame:
    """Fetch time series data for trends analysis."""
    date_filter_map = {
        "Today": col("RIDE_DATE") == session.sql("SELECT CURRENT_DATE()").collect()[0][0],
        "Last 7 Days": col("RIDE_DATE") >= session.sql("SELECT CURRENT_DATE() - 7").collect()[0][0],
        "Month to Date": col("RIDE_DATE") >= session.sql("SELECT DATE_TRUNC('month', CURRENT_DATE())").collect()[0][0]
    }

    return (session.table("RESORT_DAILY_SUMMARY")
            .filter(date_filter_map[time_period])
            .group_by("RIDE_DATE", "RESORT")
            .agg(
        sum(col("PEAK_VISITORS")).alias("VISITORS"),
        sum(col("TOTAL_REVENUE")).alias("REVENUE"),
        avg(col("AVG_CAPACITY_PCT")).alias("CAPACITY_PCT")
    )
            .order_by("RIDE_DATE", "RESORT")
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_resort_status(time_period: str) -> pd.DataFrame:
    """Fetch current status for all resorts."""
    date_filter_map = {
        "Today": col("RIDE_DATE") == session.sql("SELECT CURRENT_DATE()").collect()[0][0],
        "Last 7 Days": col("RIDE_DATE") >= session.sql("SELECT CURRENT_DATE() - 7").collect()[0][0],
        "Month to Date": col("RIDE_DATE") >= session.sql("SELECT DATE_TRUNC('month', CURRENT_DATE())").collect()[0][0]
    }

    return (session.table("RESORT_DAILY_SUMMARY")
            .filter(date_filter_map[time_period])
            .group_by("RESORT")
            .agg(
        sum(col("PEAK_VISITORS")).alias("CURRENT_VISITORS"),
        avg(col("AVG_CAPACITY_PCT")).alias("CAPACITY_PCT"),
        sum(col("TOTAL_REVENUE")).alias("REVENUE")
    )
            .with_column("STATUS",
                         when(col("CAPACITY_PCT") < 70, lit("🟢 Normal"))
                         .when(col("CAPACITY_PCT") < 90, lit("🟡 Busy"))
                         .otherwise(lit("🔴 At Capacity")))
            .order_by(desc("CAPACITY_PCT"))
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_available_resorts() -> pd.DataFrame:
    """Fetch list of available resorts."""
    return (session.table("RESORT_DAILY_SUMMARY")
            .select("RESORT")
            .distinct()
            .order_by("RESORT")
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_mountain_operations(selected_resort: str) -> pd.DataFrame:
    """Fetch real-time operational metrics for a specific resort."""
    return (session.table("CURRENT_RESORT_STATUS")
            .filter(col("RESORT") == selected_resort)
            .select("CURRENT_VISITORS", "CURRENT_CAPACITY_PCT",
                    "CURRENT_HOUR_RIDES", "CURRENT_HOUR_REVENUE", "CAPACITY_STATUS")
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_top_lifts(selected_resort: str) -> pd.DataFrame:
    """Fetch top performing lifts for a resort."""
    return (session.table("TOP_LIFTS_TODAY")
            .filter(col("RESORT") == selected_resort)
            .order_by("USAGE_RANK")
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_hourly_patterns(selected_resort: str) -> pd.DataFrame:
    """Fetch hourly visitor patterns for a resort."""
    current_date = session.sql("SELECT CURRENT_DATE()").collect()[0][0]
    return (session.table("RESORT_HOURLY_SUMMARY")
            .filter((col("RESORT") == selected_resort) & (col("RIDE_DATE") == current_date))
            .order_by("RIDE_HOUR")
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_revenue_performance(selected_resort: str) -> pd.DataFrame:
    """Fetch revenue performance metrics for a resort."""
    current_date = session.sql("SELECT CURRENT_DATE()").collect()[0][0]
    return (session.table("REVENUE_PERFORMANCE_DAILY")
            .filter((col("RESORT") == selected_resort) & (col("RIDE_DATE") == current_date))
            .to_pandas())

@st.cache_data(ttl=CACHE_TTL)
def get_weekly_performance(selected_resort: str) -> pd.DataFrame:
    """Fetch weekly performance summary for a resort."""
    return (session.table("RESORT_WEEKLY_SUMMARY")
            .filter(col("RESORT") == selected_resort)
            .order_by(desc("WEEK_START_DATE"))
            .limit(4)
            .to_pandas())

# Utility functions
def calculate_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change between two values."""
    if previous and previous > 0:
        return (current - previous) / previous * 100
    return 0

def format_currency(amount: float) -> str:
    """Format number as currency."""
    return f"${amount:,.0f}"

def format_number(number: float) -> str:
    """Format number with thousands separator."""
    return f"{number:,.0f}"

def get_capacity_icon(capacity: float) -> str:
    """Get status icon based on capacity percentage."""
    if capacity < 70:
        return "🟢"
    elif capacity < 90:
        return "🟡"
    else:
        return "🔴"

# Chart styling
def style_chart(fig):
    """Apply consistent styling to charts."""
    fig.update_layout(
        font_family="Arial",
        title_font_size=16,
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    return fig

# Sidebar navigation
with st.sidebar:
    st.title("🏔️ Resort Operations")

    page_options = ["Network Overview", "Mountain Command Center"]
    selected_page = st.radio("Navigate to:", page_options,
                             index=page_options.index(st.session_state.current_page))

    if selected_page != st.session_state.current_page:
        st.session_state.current_page = selected_page
        st.rerun()

# Main content
if st.session_state.current_page == "Network Overview":
    st.title("🌐 Network Operations Dashboard")

    # Time period selector
    with st.sidebar:
        st.markdown("---")
        time_period = st.selectbox("Select Period:",
                                   ["Today", "Last 7 Days", "Month to Date"])
        st.markdown("---")

    # Network Performance Metrics
    st.header("📊 Network Performance")
    st.caption(f"Data for: {time_period}")

    current_data, previous_data = get_network_kpis(time_period)

    if not current_data.empty and not previous_data.empty:
        current = current_data.iloc[0]
        previous = previous_data.iloc[0]

        # Calculate changes
        visitor_change = calculate_percentage_change(
            current.get('TOTAL_VISITORS', 0),
            previous.get('PREV_VISITORS', 0)
        )
        revenue_change = calculate_percentage_change(
            current.get('TOTAL_REVENUE', 0),
            previous.get('PREV_REVENUE', 0)
        )
        rides_change = calculate_percentage_change(
            current.get('TOTAL_RIDES', 0),
            previous.get('PREV_RIDES', 0)
        )

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Visitors",
                      format_number(current.get('TOTAL_VISITORS', 0)),
                      f"{visitor_change:+.1f}%")

        with col2:
            st.metric("Total Revenue",
                      format_currency(current.get('TOTAL_REVENUE', 0)),
                      f"{revenue_change:+.1f}%")

        with col3:
            capacity = current.get('AVG_CAPACITY', 0) or 0
            st.metric("Average Capacity",
                      f"{capacity:.1f}% {get_capacity_icon(capacity)}",
                      help="Network-wide capacity utilization")

        with col4:
            st.metric("Total Lift Rides",
                      format_number(current.get('TOTAL_RIDES', 0)),
                      f"{rides_change:+.1f}%")

    # Resort Status Overview (moved above the two modules with trends)
    st.header("🏔️ Resort Status Summary")
    status_data = get_resort_status(time_period)

    if not status_data.empty:
        # Format data for display
        display_data = status_data.copy()
        display_data['CURRENT_VISITORS'] = display_data['CURRENT_VISITORS'].apply(format_number)
        display_data['CAPACITY_PCT'] = display_data['CAPACITY_PCT'].apply(lambda x: f"{x:.1f}%")
        display_data['REVENUE'] = display_data['REVENUE'].apply(format_currency)

        # Select and rename columns
        display_data = display_data[['RESORT', 'CURRENT_VISITORS', 'CAPACITY_PCT', 'REVENUE', 'STATUS']]
        display_data.columns = ['Resort', 'Visitors', 'Capacity', 'Revenue', 'Status']

        st.dataframe(display_data, use_container_width=True, hide_index=True)

    # Resort Comparison
    st.header("📊 Resort Performance Comparison")

    col1, col2 = st.columns([1, 3])

    with col1:
        comparison_metric = st.selectbox("Compare by:",
                                         ["Visitors", "Revenue", "Capacity %", "Lift Rides"])

    with col2:
        resort_data = get_resort_comparison(time_period)

        if not resort_data.empty:
            metric_mapping = {
                "Visitors": "TOTAL_VISITORS",
                "Revenue": "TOTAL_REVENUE",
                "Capacity %": "AVG_CAPACITY",
                "Lift Rides": "TOTAL_RIDES"
            }
            metric_column = metric_mapping[comparison_metric]

            if metric_column in resort_data.columns:
                fig = px.bar(
                    resort_data,
                    x="RESORT",
                    y=metric_column,
                    title=f"{comparison_metric} by Resort",
                    color=metric_column,
                    color_continuous_scale="Blues"
                )
                fig = style_chart(fig)
                st.plotly_chart(fig, use_container_width=True)

    # Trends Analysis
    st.header("📈 Performance Trends")

    col1, col2 = st.columns([1, 3])

    with col1:
        trend_metric = st.selectbox("Trend metric:",
                                    ["Visitors", "Revenue", "Capacity %"])

    with col2:
        time_series_data = get_time_series_data(time_period)

        if not time_series_data.empty and 'RIDE_DATE' in time_series_data.columns:
            trend_mapping = {
                "Visitors": "VISITORS",
                "Revenue": "REVENUE",
                "Capacity %": "CAPACITY_PCT"
            }
            trend_column = trend_mapping[trend_metric]

            if trend_column in time_series_data.columns:
                # Fix time axis formatting based on time period
                time_series_copy = time_series_data.copy()

                # Format dates properly based on time period
                if time_period == "Today":
                    # For today, ensure we have proper time formatting
                    time_series_copy['RIDE_DATE'] = pd.to_datetime(time_series_copy['RIDE_DATE'])
                else:
                    # For other periods, use date formatting
                    time_series_copy['RIDE_DATE'] = pd.to_datetime(time_series_copy['RIDE_DATE']).dt.strftime('%Y-%m-%d')

                fig = px.line(
                    time_series_copy,
                    x="RIDE_DATE",
                    y=trend_column,
                    color="RESORT",
                    title=f"{trend_metric} Trends by Resort",
                    markers=True
                )
                fig = style_chart(fig)
                st.plotly_chart(fig, use_container_width=True)

else:  # Mountain Command Center
    st.title("🎿 Mountain Command Center")

    # Resort selection
    resorts_df = get_available_resorts()

    if not resorts_df.empty:
        with st.sidebar:
            st.markdown("---")
            selected_resort = st.selectbox("Select Resort:", resorts_df['RESORT'].tolist())
            st.markdown("---")

        st.header(f"📍 {selected_resort} Operations")

        # Real-time Operations Metrics
        st.header("⚡ Live Operations Status")

        operations_data = get_mountain_operations(selected_resort)

        if not operations_data.empty:
            ops = operations_data.iloc[0]

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Current Visitors", format_number(ops.get('CURRENT_VISITORS', 0)))

            with col2:
                capacity = ops.get('CURRENT_CAPACITY_PCT', 0)
                status = ops.get('CAPACITY_STATUS', 'NORMAL')
                icon = {"HIGH": "🔴", "MODERATE": "🟡", "NORMAL": "🟢"}.get(status, "🟢")
                st.metric("Live Capacity", f"{capacity:.1f}% {icon}")

            with col3:
                st.metric("Rides This Hour", format_number(ops.get('CURRENT_HOUR_RIDES', 0)))

            with col4:
                st.metric("Revenue This Hour", format_currency(ops.get('CURRENT_HOUR_REVENUE', 0)))


        # Top Lifts Performance
        st.header("🚁 Top Performing Lifts")

        top_lifts_data = get_top_lifts(selected_resort)

        if not top_lifts_data.empty:
            # Option 1: Single enhanced display with visual elements
            st.subheader("Daily Performance by Lift")

            # Get top 5 lifts and max value for scaling
            top_5_lifts = top_lifts_data.head(5)
            max_rides = top_5_lifts['DAILY_RIDES'].max()

            # Create columns for better layout
            for _, lift in top_5_lifts.iterrows():
                col1, col2, col3 = st.columns([3, 1, 1])

                with col1:
                    # Create a visual bar using progress
                    progress_value = lift['DAILY_RIDES'] / max_rides
                    st.write(f"**{lift['LIFT']}**")
                    st.progress(progress_value)

                with col2:
                    st.metric("Rides", format_number(lift['DAILY_RIDES']))

                with col3:
                    st.metric("Visitors", format_number(lift['DAILY_VISITORS']))

            # Alternative: Simple table-only approach (uncomment below and comment above if preferred)
            # st.subheader("Top Performing Lifts")
            # table_data = top_lifts_data[['LIFT', 'DAILY_RIDES', 'DAILY_VISITORS', 'USAGE_RANK']].copy()
            # table_data['DAILY_RIDES'] = table_data['DAILY_RIDES'].apply(format_number)
            # table_data['DAILY_VISITORS'] = table_data['DAILY_VISITORS'].apply(format_number)
            # table_data.columns = ['Lift', 'Daily Rides', 'Daily Visitors', 'Rank']
            # st.dataframe(table_data.head(5), use_container_width=True, hide_index=True)

        # Hourly Patterns
        st.header("📈 Hourly Activity Patterns")

        hourly_data = get_hourly_patterns(selected_resort)

        if not hourly_data.empty and all(col in hourly_data.columns for col in ['RIDE_HOUR', 'VISITOR_COUNT', 'CAPACITY_PCT']):
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            fig.add_trace(
                go.Scatter(
                    x=hourly_data['RIDE_HOUR'],
                    y=hourly_data['VISITOR_COUNT'],
                    name='Visitors',
                    mode='lines+markers',
                    line=dict(color='#2E86AB', width=3)
                ),
                secondary_y=False
            )

            fig.add_trace(
                go.Scatter(
                    x=hourly_data['RIDE_HOUR'],
                    y=hourly_data['CAPACITY_PCT'],
                    name='Capacity %',
                    mode='lines+markers',
                    line=dict(color='#A23B72', width=3)
                ),
                secondary_y=True
            )

            fig = style_chart(fig)
            fig.update_layout(title="Hourly Visitor Flow & Capacity Utilization")
            fig.update_xaxes(title_text="Hour of Day")
            fig.update_yaxes(title_text="Visitor Count", secondary_y=False)
            fig.update_yaxes(title_text="Capacity %", secondary_y=True)

            st.plotly_chart(fig, use_container_width=True)

        # Revenue and Weekly Performance
        col1, col2 = st.columns(2)

        with col1:
            st.header("💰 Revenue Performance")

            revenue_data = get_revenue_performance(selected_resort)

            if not revenue_data.empty:
                rev = revenue_data.iloc[0]

                # Horizontal layout with three columns
                col_a, col_b, col_c = st.columns(3)

                with col_a:
                    st.metric("Today's Revenue", format_currency(rev.get('TOTAL_REVENUE', 0)))

                with col_b:
                    st.metric("Revenue Target", format_currency(rev.get('REVENUE_TARGET_USD', 0)))

                with col_c:
                    # Status - fixed to handle potential None/undefined values
                    status_icons = {'ABOVE_TARGET': '🟢', 'NEAR_TARGET': '🟡', 'BELOW_TARGET': '🔴'}
                    status = rev.get('PERFORMANCE_STATUS', 'UNKNOWN')
                    icon = status_icons.get(status, '⚪')
                    target_pct = rev.get('REVENUE_TARGET_PCT', 0) or 0  # Handle None values
                    st.metric("Target Achievement", f"{target_pct:.1f}% {icon}")

        with col2:
            st.header("📅 Weekly Performance")

            weekly_data = get_weekly_performance(selected_resort)

            if not weekly_data.empty:
                # Format weekly data
                display_data = weekly_data.copy()

                # Format columns that exist
                format_funcs = {
                    'WEEK_START_DATE': lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'),
                    'WEEK_PEAK_VISITORS': format_number,
                    'AVG_DAILY_VISITORS': format_number,
                    'WEEK_TOTAL_REVENUE': format_currency,
                    'AVG_DAILY_REVENUE': format_currency,
                    'WEEK_PEAK_CAPACITY_PCT': lambda x: f"{x:.1f}%"
                }

                for col_name, func in format_funcs.items():
                    if col_name in display_data.columns:
                        display_data[col_name] = display_data[col_name].apply(func)

                # Select and rename columns
                column_map = {
                    'WEEK_START_DATE': 'Week Starting',
                    'WEEK_PEAK_VISITORS': 'Peak Visitors',
                    'AVG_DAILY_VISITORS': 'Avg Daily Visitors',
                    'WEEK_TOTAL_REVENUE': 'Total Revenue',
                    'AVG_DAILY_REVENUE': 'Avg Daily Revenue',
                    'WEEK_PEAK_CAPACITY_PCT': 'Peak Capacity'
                }

                # Filter existing columns and rename
                existing_cols = [col for col in column_map.keys() if col in display_data.columns]
                display_data = display_data[existing_cols].rename(columns=column_map)

                st.dataframe(display_data, use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.markdown("⛷️ Powered by Snowflake Dynamic Tables", help="Dynamic data processing for real-time insights")