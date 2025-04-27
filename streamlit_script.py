import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
from pyspark.sql import SparkSession

# Initialize Spark session for Streamlit
spark = SparkSession.builder.appName("StudentPerformanceDashboard").getOrCreate()

# Function to load processed data (Parquet)
def load_data(file_name):
    df = spark.read.parquet(f'file:///home/meqlad/venv/processed_data/{file_name}')
    return df.toPandas()

# Load datasets
student_info = load_data('studentInfo.parquet')
courses = load_data('courses.parquet')
student_registration = load_data('studentRegistration.parquet')
student_vle = load_data('studentVle.parquet')

# Streamlit UI
st.title('📚 Student Performance Dashboard')
st.write("Visualizing processed student performance data.")

# Display student info
st.subheader('🔎 Student Info Sample')
st.dataframe(student_info.head())

# -------------------------
# 1. Final Result Distribution
# -------------------------
st.subheader('📊 Final Result Distribution')

fig1, ax1 = plt.subplots()
student_info['final_result'].value_counts().plot(kind='bar', ax=ax1, color='skyblue')
ax1.set_xlabel('Final Result')
ax1.set_ylabel('Number of Students')
ax1.set_title('Distribution of Final Results')
st.pyplot(fig1)

# -------------------------
# 2. Education Level Distribution
# -------------------------
st.subheader('🎓 Education Level of Students')

fig2, ax2 = plt.subplots()
student_info['education_level'].value_counts().plot(kind='pie', ax=ax2, autopct='%1.1f%%', startangle=90, colors=sns.color_palette('pastel'))
ax2.set_ylabel('')  # Hide the y-label
ax2.set_title('Education Level Distribution')
st.pyplot(fig2)

# -------------------------
# 3. Students Registered per Course
# -------------------------
st.subheader('📝 Students Registered per Course')

# Merge student_registration with courses to get course names if needed
merged = student_registration.merge(courses, on='code_module', how='left')

fig3, ax3 = plt.subplots(figsize=(10,5))
merged['code_module'].value_counts().plot(kind='barh', ax=ax3, color='lightgreen')
ax3.set_xlabel('Number of Students')
ax3.set_ylabel('Course Code')
ax3.set_title('Students Registered per Course')
st.pyplot(fig3)

# -------------------------
# 4. Total Clicks per Student (Top 20)
# -------------------------
st.subheader('🖱️ Top 20 Students by Total Clicks')

fig4, ax4 = plt.subplots(figsize=(10,5))
top_clicks = student_vle.sort_values('sum(sum_click)', ascending=False).head(20)
sns.barplot(x='sum(sum_click)', y='id_student', data=top_clicks, ax=ax4, palette='Blues_r')
ax4.set_xlabel('Total Clicks')
ax4.set_ylabel('Student ID')
ax4.set_title('Top 20 Students by Online Platform Interaction')
st.pyplot(fig4)
