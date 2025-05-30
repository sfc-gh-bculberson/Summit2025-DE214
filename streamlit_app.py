import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum, avg, max, when, desc, lit

# Global configuration
DEFAULT_CACHE_TTL = 60  # seconds
NETWORK_PAGE_NAME = "Network Overview"
RESORT_PAGE_NAME = "Mountain Operations Center"

# High-level page configuration
# This must be the first Streamlit command
st.set_page_config(
    page_title="Ski Resort Operations Hub",
    page_icon="⛷️",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

# Helper function to get date values
@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_date_values():
    """Get max ride date and calculated date ranges based off that, due to simulation clock in the future."""
    # noinspection SqlResolve
    query = """
    WITH max_ride_info AS (
        SELECT             
            MAX(RIDE_TIME) as last_data_timestamp
        FROM LIFT_RIDE
    )
    SELECT 
        last_data_timestamp,
        DATE(last_data_timestamp) as last_data_date,
        HOUR(last_data_timestamp) as last_data_hour,      
        last_data_date - INTERVAL '1 day' as yesterday,
        last_data_date - INTERVAL '7 days' as week_ago,
        last_data_date - INTERVAL '14 days' as two_weeks_ago,
        DATE_TRUNC('month', last_data_date) as month_start,
        DATE_TRUNC('month', last_data_date - INTERVAL '1 month') as prev_month_start
    FROM max_ride_info
    """
    result = session.sql(query).collect()[0]
    return {
        'last_data_time': result[0],
        'current_date': result[1],
        'current_hour': result[2],
        'yesterday': result[3],
        'week_ago': result[4],
        'two_weeks_ago': result[5],
        'month_start': result[6],
        'prev_month_start': result[7]
    }

def get_last_data_time():
    return get_date_values()["last_data_time"]

def get_current_date():
    return get_date_values()["current_date"]

def get_current_hour():
    return get_date_values()["current_hour"]

# Helper function to display simulation status
def display_simulation_status():
    """Display current simulation date and last data received time."""
    try:
        # Get simulation clock data
        sim_date = get_current_date()
        last_data = get_last_data_time()

        # Format dates for display
        if sim_date is None:
            sim_date = "N/A"
        else:
            sim_date = sim_date.strftime('%A, %B %d, %Y')
        if last_data is None:
            last_data = "No data received yet"
        else:
            last_data = last_data.strftime('%H:%M:%S')

        # Use columns for spacing
        col1, col2, col3 = st.columns([0.3, 0.3, 0.4])
        with col1:
            st.markdown(f"📅 **Simulation Date:** {sim_date}")
        with col2:
            st.markdown(f"🕐 **Last Data Received:** {last_data}")

    except Exception as e:
        st.error(f"Error fetching simulation status: {str(e)}")

# Data fetching functions using Snowpark
@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_network_kpis(time_period: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch network-wide KPIs for current and previous periods."""
    dates = get_date_values()

    # Define date filters
    if time_period == "Today":
        current_filter = col("RIDE_DATE") == dates['current_date']
        previous_filter = col("RIDE_DATE") == dates['yesterday']
    elif time_period == "Last 7 Days":
        current_filter = col("RIDE_DATE") >= dates['week_ago']
        previous_filter = (col("RIDE_DATE") >= dates['two_weeks_ago']) & (col("RIDE_DATE") < dates['week_ago'])
    else:  # Month to Date
        current_filter = col("RIDE_DATE") >= dates['month_start']
        previous_filter = (col("RIDE_DATE") >= dates['prev_month_start']) & (col("RIDE_DATE") < dates['month_start'])

    # Current period metrics
    current_metrics_df = session.table("V_DAILY_NETWORK_METRICS").filter(current_filter).agg(
        sum(col("TOTAL_VISITORS")).alias("total_visitors"),
        sum(col("TOTAL_REVENUE")).alias("total_revenue"),
        avg(col("AVG_CAPACITY")).alias("avg_capacity"),
        sum(col("TOTAL_RIDES")).alias("total_rides")
    )
    # Previous period metrics
    previous_metrics_df = session.table("V_DAILY_NETWORK_METRICS").filter(previous_filter).agg(
        sum(col("TOTAL_VISITORS")).alias("prev_visitors"),
        sum(col("TOTAL_REVENUE")).alias("prev_revenue"),
        sum(col("TOTAL_RIDES")).alias("prev_rides")
    )

    return current_metrics_df.to_pandas(), previous_metrics_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_network_resort_comparison(time_period: str) -> pd.DataFrame:
    """Fetch resort comparison data for the specified period."""
    dates = get_date_values()
    if time_period == "Today":
        date_filter = col("RIDE_DATE") == dates['current_date']
    elif time_period == "Last 7 Days":
        date_filter = col("RIDE_DATE") >= dates['week_ago']
    else:  # Month to Date
        date_filter = col("RIDE_DATE") >= dates['month_start']
    results_df = (session.table("DAILY_RESORT_SUMMARY")
                  .filter(date_filter)
                  .group_by("RESORT")
                  .agg(
        sum(col("PEAK_VISITORS")).alias("TOTAL_VISITORS"),
        sum(col("TOTAL_REVENUE")).alias("TOTAL_REVENUE"),
        avg(col("AVG_CAPACITY_PCT")).alias("AVG_CAPACITY"),
        sum(col("TOTAL_RIDES")).alias("TOTAL_RIDES")
    ).order_by("RESORT"))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_network_time_series_data(time_period: str) -> pd.DataFrame:
    """Fetch time series data for trends analysis."""
    dates = get_date_values()
    if time_period == "Today":
        date_filter = col("RIDE_DATE") == dates['current_date']
    elif time_period == "Last 7 Days":
        date_filter = col("RIDE_DATE") >= dates['week_ago']
    else:  # Month to Date
        date_filter = col("RIDE_DATE") >= dates['month_start']
    results_df = (session.table("DAILY_RESORT_SUMMARY")
                  .filter(date_filter)
                  .select("RIDE_DATE", "RESORT",
                          col("PEAK_VISITORS").alias("VISITORS"),
                          col("TOTAL_REVENUE").alias("REVENUE"),
                          col("AVG_CAPACITY_PCT").alias("CAPACITY_PCT"))
                  .order_by("RIDE_DATE", "RESORT"))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_network_status_by_resort(time_period: str) -> pd.DataFrame:
    """Fetch current status for all resorts."""
    dates = get_date_values()
    if time_period == "Today":
        date_filter = col("RIDE_DATE") == dates['current_date']
    elif time_period == "Last 7 Days":
        date_filter = col("RIDE_DATE") >= dates['week_ago']
    else:  # Month to Date
        date_filter = col("RIDE_DATE") >= dates['month_start']
    results_df = (session.table("DAILY_RESORT_SUMMARY")
                  .filter(date_filter)
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
                  .order_by(desc("CAPACITY_PCT")))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_available_resorts() -> pd.DataFrame:
    """Fetch list of available resorts."""
    results_df = (session.table("DAILY_RESORT_SUMMARY")
                  .select("RESORT")
                  .distinct()
                  .order_by("RESORT"))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_resort_operations_data(selected_resort: str) -> pd.DataFrame:
    """Fetch latest operational metrics for a specific resort from most recent hourly data."""
    latest_date = get_current_date()
    latest_hour = get_current_hour()
    results_df = (session.table("HOURLY_RESORT_SUMMARY")
                  .filter((col("RESORT") == selected_resort) &
                          (col("RIDE_DATE") == latest_date) &
                          (col("RIDE_HOUR") == latest_hour))
                  .select(col("VISITOR_COUNT").alias("CURRENT_VISITORS"),
                          col("CAPACITY_PCT").alias("CURRENT_CAPACITY_PCT"),
                          col("TOTAL_RIDES").alias("CURRENT_HOUR_RIDES"),
                          col("TOTAL_REVENUE").alias("CURRENT_HOUR_REVENUE"),
                          col("CAPACITY_STATUS")))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_resort_top_lifts(selected_resort: str) -> pd.DataFrame:
    """Fetch top performing lifts for a resort from last 30 minutes."""
    results_df = (session.table("V_RT_LIFT_PERFORMANCE")
                  .filter((col("RESORT") == selected_resort) &
                          (col("USAGE_RANK") <= 5))
                  .select("LIFT",
                          col("RIDES_30MIN").alias("RIDES_30MIN"),
                          col("VISITORS_30MIN").alias("VISITORS_30MIN"),
                          col("RIDES_PER_HOUR").alias("RIDES_PER_HOUR"),
                          col("LAST_ACTIVITY").alias("LAST_ACTIVITY"),
                          col("USAGE_RANK").alias("RANK"))
                  .order_by("RANK"))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_resort_hourly_patterns(selected_resort: str) -> pd.DataFrame:
    """Fetch hourly visitor patterns for a resort from most recent date."""
    latest_date = get_current_date()
    results_df = (session.table("HOURLY_RESORT_SUMMARY")
                  .filter((col("RESORT") == selected_resort) & (col("RIDE_DATE") == latest_date))
                  .select("RIDE_HOUR", "VISITOR_COUNT", "CAPACITY_PCT", "TOTAL_REVENUE")
                  .order_by("RIDE_HOUR"))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_resort_revenue_performance(selected_resort: str) -> pd.DataFrame:
    """Fetch revenue performance metrics for a resort from most recent date."""
    latest_date = get_current_date()
    results_df = (session.table("V_DAILY_REVENUE_PERFORMANCE")
                  .filter((col("RESORT") == selected_resort) & (col("RIDE_DATE") == latest_date))
                  .select("TOTAL_REVENUE", "REVENUE_TARGET_USD", "REVENUE_TARGET_PCT", "PERFORMANCE_STATUS"))
    return results_df.to_pandas()

@st.cache_data(ttl=DEFAULT_CACHE_TTL)
def get_resort_weekly_performance(selected_resort: str) -> pd.DataFrame:
    """Fetch weekly performance summary for a resort."""
    results_df = (session.table("WEEKLY_RESORT_SUMMARY")
                  .filter(col("RESORT") == selected_resort)
                  .select("WEEK_START_DATE", "WEEK_PEAK_VISITORS", "AVG_DAILY_VISITORS",
                          "WEEK_TOTAL_REVENUE", "AVG_DAILY_REVENUE", "WEEK_PEAK_CAPACITY_PCT")
                  .order_by(desc("WEEK_START_DATE"))
                  .limit(4))
    return results_df.to_pandas()

# Handle page refresh action
def handle_refresh(page_name):
    """Handle refresh with user feedback."""
    st.cache_data.clear()
    st.rerun()

# Utility functions
def calculate_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change between two values."""
    if previous and previous > 0:
        return (current - previous) / previous * 100
    return 0

def format_currency(amount: float) -> str:
    """Format number as currency."""
    if pd.isna(amount):
        return "$0"
    return f"${amount:,.0f}"

def format_number(number: float) -> str:
    """Format number with thousands separator."""
    if pd.isna(number):
        return "0"
    return f"{number:,.0f}"

def get_capacity_icon(capacity: float) -> str:
    """Get status icon based on capacity percentage."""
    if pd.isna(capacity):
        return "⚪"
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

# Initialize session state
# Default to the network overview page
if 'current_page' not in st.session_state:
    st.session_state.current_page = NETWORK_PAGE_NAME

# Persistent sidebar navigation
with st.sidebar:
    st.title("🏔️ Resort Operations")
    page_options = [NETWORK_PAGE_NAME, RESORT_PAGE_NAME]
    selected_page = st.radio("Navigate to:", page_options,
                             index=page_options.index(st.session_state.current_page))
    if selected_page != st.session_state.current_page:
        st.session_state.current_page = selected_page
        # rerun() causes the app to refresh and load the new page
        st.rerun()

# Main content
if st.session_state.current_page == NETWORK_PAGE_NAME:
    # Page title with refresh button
    col1, col2 = st.columns([6, 1])
    with col1:
        st.title("🌐 Network Overview Dashboard")
    with col2:
        # st.write("")  # Spacing
        if st.button("🔄 Refresh",
                     key="refresh_network",
                     help="Refresh all network data",
                     type="secondary"):
            handle_refresh("Network")

    # Display simulation status
    display_simulation_status()
    st.markdown("---")

    # Page-specific sidebar with time period selector
    with st.sidebar:
        st.markdown("---")
        time_period = st.selectbox("Select Period:",
                                   ["Today", "Last 7 Days", "Month to Date"])
        st.markdown("---")

    # Network Performance Metrics
    st.header("📊 Network Performance")
    st.caption(f"Data for: {time_period}")

    try:
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
        else:
            st.info("No data available for the selected time period.")
    except Exception as e:
        st.error(f"Error fetching network KPIs: {str(e)}")

    # Resort Status Overview
    st.header("🏔️ Resort Status Summary")
    try:
        status_data = get_network_status_by_resort(time_period)

        if not status_data.empty:
            # Format data for display
            display_data = status_data.copy()
            display_data['CURRENT_VISITORS'] = display_data['CURRENT_VISITORS'].apply(format_number)
            display_data['CAPACITY_PCT'] = display_data['CAPACITY_PCT'].apply(
                lambda x: f"{x:.1f}%" if not pd.isna(x) else "0%")
            display_data['REVENUE'] = display_data['REVENUE'].apply(format_currency)

            # Select and rename columns
            display_data = display_data[['RESORT', 'CURRENT_VISITORS', 'CAPACITY_PCT', 'REVENUE', 'STATUS']]
            display_data.columns = ['Resort', 'Visitors', 'Capacity', 'Revenue', 'Status']

            st.dataframe(display_data, use_container_width=True, hide_index=True)
        else:
            st.info("No resort status data available for the selected time period.")
    except Exception as e:
        st.error(f"Error fetching resort status: {str(e)}")

    # Resort Comparison
    st.header("📊 Resort Performance Comparison")

    col1, col2 = st.columns([1, 3])

    with col1:
        comparison_metric = st.selectbox("Compare by:",
                                         ["Visitors", "Revenue", "Capacity %", "Lift Rides"])

    with col2:
        try:
            resort_data = get_network_resort_comparison(time_period)

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
            else:
                st.info("No resort comparison data available for the selected time period.")
        except Exception as e:
            st.error(f"Error fetching resort comparison data: {str(e)}")

    # Trends Analysis
    st.header("📈 Performance Trends")
    # Add context based on time period
    if time_period == "Today":
        st.caption("⚠️ Single day view - select a longer time period for meaningful trend data")
    else:
        st.caption(f"Trends across {time_period.lower()}")

    col1, col2 = st.columns([1, 3])
    with col1:
        trend_metric = st.selectbox("Trend metric:",
                                    ["Visitors", "Revenue", "Capacity %"])
    with col2:
        try:
            time_series_data = get_network_time_series_data(time_period)

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
                        time_series_copy['RIDE_DATE'] = pd.to_datetime(time_series_copy['RIDE_DATE']).dt.strftime(
                            '%Y-%m-%d')

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
            else:
                st.info("No time series data available for the selected time period.")
        except Exception as e:
            st.error(f"Error fetching time series data: {str(e)}")

elif st.session_state.current_page == RESORT_PAGE_NAME:

    # Get available resorts
    resorts_df = None
    try:
        resorts_df = get_available_resorts()
    except Exception as e:
        st.error(f"Error loading resort list: {str(e)}")
    if resorts_df is None or resorts_df.empty:
        st.error("No resort data is available.")
        st.stop()  # Prevent the rest of the page from rendering

    # Initialize selected_resort in session state if not exists
    # This ensures that the selected resort persists across reruns and page changes
    if 'selected_resort' not in st.session_state:
        st.session_state.selected_resort = resorts_df['RESORT'].tolist()[0]  # Default to first resort

    # Page-specific sidebar with time period selector
    with st.sidebar:
        st.markdown("---")

        # Get the current index of the selected resort for the selectbox
        resort_list = resorts_df['RESORT'].tolist()
        try:
            current_index = resort_list.index(st.session_state.selected_resort)
        except ValueError:
            # If the stored resort is no longer available, default to first
            current_index = 0
            st.session_state.selected_resort = resort_list[0]

        # Create selectbox to select resort
        selected_resort = st.selectbox(
            "Select Resort:",
            resort_list,
            index=current_index,
            key='resort_selectbox'
        )
        # Update session state and refresh page when selection changes
        if selected_resort != st.session_state.selected_resort:
            st.session_state.selected_resort = selected_resort
            st.rerun()  # Force refresh to update the display

        st.markdown("---")

    # Use the session state value for consistency
    selected_resort = st.session_state.selected_resort

    # Page title with refresh button
    col1, col2 = st.columns([6, 1])
    with col1:
        st.title(f"🎿 {selected_resort} Operations Center")
    with col2:
        if st.button("🔄 Refresh",
                     key="refresh_resort",
                     help="Refresh resort data",
                     type="secondary"):
            handle_refresh("Resort")

    # Display simulation status
    display_simulation_status()
    st.markdown("---")

    # Real-time Operations Metrics
    st.header("⚡ Live Operations Status")
    st.caption("Live performance in the current hour")

    try:
        operations_data = get_resort_operations_data(selected_resort)

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
        else:
            st.info("No current operations data available for this resort.")
    except Exception as e:
        st.error(f"Error fetching operations data: {str(e)}")

    # Top Lifts Performance - Last 30 Minutes
    st.header("📍 Top Performing Lifts")
    st.caption("Live performance in the last 30 minutes")

    try:
        realtime_lifts = get_resort_top_lifts(selected_resort)

        if not realtime_lifts.empty:
            # Get max rides for scaling the bars
            max_rides = realtime_lifts['RIDES_30MIN'].max()

            for idx, lift in realtime_lifts.iterrows():
                # Create container for each lift
                with st.container():
                    col1, col2, col3, col4 = st.columns([4, 1.5, 1.5, 1])

                    with col1:
                        # Lift name with rank indicator
                        rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][int(lift['RANK']) - 1]
                        st.write(f"**{rank_emoji} {lift['LIFT']}**")

                        # Progress bar showing relative activity
                        progress_value = lift['RIDES_30MIN'] / max_rides if max_rides > 0 else 0
                        st.progress(progress_value)

                    with col2:
                        st.metric(
                            "Rides (30min)",
                            f"{int(lift['RIDES_30MIN'])}",
                            help="Total rides in last 30 minutes"
                        )

                    with col3:
                        st.metric(
                            "Visitors",
                            f"{int(lift['VISITORS_30MIN'])}",
                            help="Unique visitors in last 30 minutes"
                        )

                    with col4:
                        # Activity indicator
                        rides_per_hour = lift['RIDES_PER_HOUR']
                        if rides_per_hour > 200:
                            activity_indicator = "🔥"
                            activity_text = "Hot"
                        elif rides_per_hour > 100:
                            activity_indicator = "⚡"
                            activity_text = "Active"
                        else:
                            activity_indicator = "🟢"
                            activity_text = "Normal"

                        st.write(f"{activity_indicator}")
                        st.caption(f"{activity_text}")

        else:
            st.info("No recent lift activity data available.")

    except Exception as e:
        st.error(f"Error fetching real-time lift data: {str(e)}")

    # Hourly Patterns
    st.header("📈 Hourly Activity Patterns")

    try:
        hourly_data = get_resort_hourly_patterns(selected_resort)

        if not hourly_data.empty and 'RIDE_HOUR' in hourly_data.columns and 'VISITOR_COUNT' in hourly_data.columns:

            fig = go.Figure()

            # Single line showing visitor count throughout the day
            fig.add_trace(
                go.Scatter(
                    x=hourly_data['RIDE_HOUR'],
                    y=hourly_data['VISITOR_COUNT'],
                    name='Visitors on Mountain',
                    mode='lines+markers',
                    line=dict(color='#2E86AB', width=3),
                    fill='tozeroy',
                    fillcolor='rgba(46, 134, 171, 0.1)'
                )
            )

            fig.update_layout(
                title="Visitors on Mountain Throughout the Day",
                xaxis_title="Hour of Day",
                yaxis_title="Number of Visitors",
                showlegend=False,
                plot_bgcolor='white',
                paper_bgcolor='white',
                font_family="Arial",
                xaxis=dict(
                    range=[6, 22],  # Show full ski day (6 AM to 10 PM)
                    dtick=2  # Show every 2 hours
                ),
                yaxis=dict(
                    rangemode='tozero'  # Start from 0
                )
            )

            st.plotly_chart(fig, use_container_width=True)


        else:
            st.info("No hourly pattern data available for this resort.")
    except Exception as e:
        st.error(f"Error fetching hourly patterns: {str(e)}")



    # Revenue and Weekly Performance
    col1, col2 = st.columns(2)

    with col1:
        st.header("💰 Daily Performance")

        try:
            revenue_data = get_resort_revenue_performance(selected_resort)

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
            else:
                st.info("No revenue performance data available for this resort.")
        except Exception as e:
            st.error(f"Error fetching revenue performance: {str(e)}")

    with col2:
        st.header("📅 Weekly Performance")

        try:
            weekly_data = get_resort_weekly_performance(selected_resort)

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
            else:
                st.info("No weekly performance data available for this resort.")
        except Exception as e:
            st.error(f"Error fetching weekly performance: {str(e)}")


else:
    st.error("Invalid page selected. Please choose a valid page from the sidebar.")

# Footer
st.markdown("---")
st.markdown("⛷️ Powered by Snowflake", help="Simplified data processing for real-time insights")
