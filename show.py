import streamlit as st
import pymongo
import pandas as pd
import plotly.express as px

# ---------------------------
# CẤU HÌNH
# ---------------------------

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "processed_data"

# Cấu hình Streamlit
st.set_page_config(page_title="Social Media Analytics", page_icon="📈", layout="wide")

# Custom CSS
st.markdown(
    """
    <style>
        .stSelectbox>div>div>div {
            border: 2px solid #4CAF50; /* Màu viền selectbox */
            border-radius: 5px;
            padding: 5px;
        }
        .stTextInput>div>div>div {
            border: 2px solid #2196F3; /* Màu viền text input */
            border-radius: 5px;
            padding: 5px;
        }
        .stButton>button {
            background-color: #4CAF50; /* Màu nền nút */
            color: white;
            border: none;
            border-radius: 5px;
            padding: 10px 20px;
            cursor: pointer;
        }
        .stButton>button:hover {
            background-color: #367c39; /* Màu nền nút khi hover */
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------
# KẾT NỐI MONGODB
# ---------------------------

@st.cache_resource
def init_connection():
    return pymongo.MongoClient(MONGO_URI)

client = init_connection()
db = client[MONGO_DB]

# ---------------------------
# HÀM LẤY DỮ LIỆU
# ---------------------------

@st.cache_data(ttl=600)
def load_data(collection_name):
    collection = db[collection_name]
    data = list(collection.find())
    return pd.DataFrame(data)

def get_kol_details(kol_id):
    kol_details = db["preprocess_twitter_users"].find_one({"_id": kol_id})
    return kol_details

# ---------------------------
# TRANG QUERY
# ---------------------------

def query_page():
    st.title("Data Query")

    # Form input
    st.markdown("### Select Collection and Enter Query")
    collection_name = st.selectbox("Select Collection", ["Projects social media", "Tweets", "Twitter users"])

    if collection_name == "Projects social media":
        project_id = st.text_input("Enter Project ID")
        if st.button("Query"):
            if project_id:
                result = db['preprocess_projects_social_media'].find_one({"projectId": project_id})
                if result:
                    df_result = pd.DataFrame([result])
                    st.markdown("#### Query Result")
                    st.table(df_result)
                else:
                    st.write("No matching project found.")
            else:
                st.write("Please enter a Project ID.")

    elif collection_name == "Tweets":
        author_name = st.text_input("Enter Author Name")
        if st.button("Query"):
            if author_name:
                results = list(db['preprocess_tweets'].find({"authorName": author_name}))
                if results:
                    df_result = pd.DataFrame(results)
                    st.markdown("#### Query Result")
                    st.table(df_result)
                else:
                    st.write("No matching tweets found.")
            else:
                st.write("Please enter an Author Name.")

    elif collection_name == "Twitter users":
        user_name = st.text_input("Enter User Name")
        if st.button("Query"):
            if user_name:
                result = db['preprocess_twitter_users'].find_one({"userName": user_name})
                if result:
                    # Loại bỏ trường engagementChangeLogs trước khi tạo DataFrame
                    result_without_engagement = {k: v for k, v in result.items() if k != 'engagementChangeLogs'}
                    df_result = pd.DataFrame([result_without_engagement])
                    st.markdown("#### Query Result")
                    st.table(df_result)

                    # Vẽ biểu đồ engagementChangeLogs
                    if 'engagementChangeLogs' in result and result['engagementChangeLogs']:
                        engagement_data = result['engagementChangeLogs']
                        timestamps = sorted(engagement_data.keys(), key=int)
                        data_lines = {'Timestamp': timestamps, 'Likes': [], 'Replies': [], 'Retweets': []}
                        for ts in timestamps:
                            data_lines['Likes'].append(engagement_data[ts][0])
                            data_lines['Replies'].append(engagement_data[ts][1])
                            data_lines['Retweets'].append(engagement_data[ts][2])

                        df_engagement = pd.DataFrame(data_lines)
                        fig_engagement = px.line(df_engagement, x='Timestamp', y=['Likes', 'Replies', 'Retweets'],
                                                 title='Engagement Change Logs',
                                                 labels={'value': 'Count', 'Timestamp': 'Time'})
                        st.plotly_chart(fig_engagement)
                    else:
                        st.write("No engagement change logs found for this user.")

                else:
                    st.write("No matching user found.")
            else:
                st.write("Please enter a User Name.")

# ---------------------------
# TRANG CHÍNH
# ---------------------------

def main():
    # Load dataframes
    df_projects_social_media = load_data("preprocess_projects_social_media")
    df_kol_folder = load_data("KOL_folder")
    df_users = load_data("preprocess_twitter_users")
    df_general_users = load_data("general_users")
    df_project_folder = load_data("Project_folder")

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["KOL Analysis", "General Analytics", "Query"])

    # Main Page - KOL Analysis
    if page == "KOL Analysis":
        st.title("KOL Analysis")

        # Filter KOLs by Project
        if not df_projects_social_media.empty and not df_kol_folder.empty:
            df_projects_social_media['twitter'] = df_projects_social_media['twitter'].apply(lambda x: x['url'] if isinstance(x, dict) and 'url' in x else None)
            df_merged = pd.merge(df_kol_folder, df_projects_social_media, left_on='url', right_on='twitter', how='inner')
            df_merged = df_merged.drop(['_id_y', 'twitter', 'website'], axis=1)
            df_merged = df_merged.rename(columns={'_id_x': '_id'})

            st.markdown("### KOLs by Project")
            selected_project = st.selectbox("Select Project", ["All"] + list(df_merged["projectId"].unique()))

            if selected_project != "All":
                filtered_kols = df_merged[df_merged["projectId"] == selected_project]
            else:
                filtered_kols = df_merged

            # Display KOL details
            st.markdown("### KOL Details")
            if not filtered_kols.empty:
                st.write(filtered_kols)
                st.markdown("#### Information of KOLs")
                selected_kol_id = st.selectbox("Select KOL", filtered_kols["_id"].unique())

                kol_details = get_kol_details(selected_kol_id)

                if kol_details:
                    kol_info_df = pd.DataFrame([kol_details])
                    kol_info_df = kol_info_df.drop(columns=["_id"])
                    st.write(kol_info_df)

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Influence Score", f"{kol_details.get('influence_score', 0):,.2f}")
                    with col2:
                        st.metric("Followers", f"{kol_details.get('followersCount', 0):,}")
                    with col3:
                        st.metric("Friends", f"{kol_details.get('friendsCount', 0):,}")
                else:
                    st.write("KOL details not found.")

            # Overall KOL Statistics
            st.markdown("### Overall KOL Statistics")
            col1, col2, col3 = st.columns(3)
            col1.metric("Total KOLs", len(df_merged))
            col2.metric("Avg Followers", f"{df_merged['followersCount'].mean():,.2f}")
            col3.metric("Max Influence Score", f"{df_merged['influence_score'].max():,.2f}")

            # Top KOLs
            st.markdown("### Top 5 KOLs by Influence Score")
            top_kols = df_merged.sort_values("influence_score", ascending=False).head(5)
            fig_top_kols = px.bar(top_kols, x="userName", y="influence_score", color="influence_score",
                                 title="Top 5 KOLs by Influence Score")
            st.plotly_chart(fig_top_kols)
        else:
            st.write("Data is empty")

    # Main Page - General Analytics
    elif page == "General Analytics":
        st.title("General Analytics")

        # Location Analysis
        if not df_general_users.empty:
            st.markdown("### Location Analysis")
            location_stats = df_general_users[df_general_users['_id'] == 'location_stats']
            if not location_stats.empty and 'data' in location_stats.columns:
                location_data = location_stats['data'].iloc[0]

                if isinstance(location_data, dict):
                    location_df = pd.DataFrame(list(location_data.items()), columns=["Location", "Count"])
                    fig_location = px.pie(location_df, names="Location", values="Count", title="User Distribution by Location")
                    st.plotly_chart(fig_location)
                else:
                    st.write("Location data is not in the correct format.")
            else:
                st.write("Location statistics not found.")

            # Blue Checkmark Analysis
            st.markdown("### Blue Checkmark Analysis")
            blue_stats = df_general_users[df_general_users['_id'] == 'blue_stats']
            if not blue_stats.empty and 'data' in blue_stats.columns:
                blue_data = blue_stats['data'].iloc[0]

                if isinstance(blue_data, dict):
                    blue_df = pd.DataFrame(list(blue_data.items()), columns=["Blue Checkmark", "Count"])
                    fig_blue = px.pie(blue_df, names="Blue Checkmark", values="Count", title="Blue Checkmark Distribution")
                    st.plotly_chart(fig_blue)
                else:
                    st.write("Blue Checkmark data is not in the correct format.")
            else:
                st.write("Blue Checkmark statistics not found.")

            # User Engagement Analysis
            if not df_users.empty:
                st.markdown("### User Engagement Analysis")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Users", len(df_users))
                col2.metric("Avg Tweets", f"{df_users['total_tweets'].mean():,.2f}")
                col3.metric("Max Views", f"{df_users['max_views'].max():,}")

                # Top Active Users
                st.markdown("#### Top 5 Active Users by Tweet Count")
                top_active_users = df_users.sort_values("total_tweets", ascending=False).head(5)
                fig_top_users = px.bar(top_active_users, x="userName", y="total_tweets", color="total_tweets",
                                       title="Top 5 Active Users by Tweet Count")
                st.plotly_chart(fig_top_users)
            else:
                st.write("User data is empty.")
        else:
            st.write("General users data is empty.")

    # Main Page - Query
    elif page == "Query":
        query_page()

if __name__ == "__main__":
    main()