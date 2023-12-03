import time
import pandas as pd
import streamlit as st
from datetime import datetime
from sqlalchemy import create_engine

try:
    conn_str = 'clickhouse://default:@localhost/default'
    engine = create_engine(conn_str)
    session = sessionmaker(bind_engine)()
except Exception as e:
    print("Error while connecting to clickhouse"+str(e))
    
st.set_page_config(layout = "wide")
st.title("Realtime update on facility resources")

now = datetime.now()
dt_string = now.strftime("%d %m %y %H:%M:%S")
st.write = print(f"last update:{dt_string}")

if not "sleep_time" in st.session_state:
    st.session_state.sleep_time = 5
    
if not "auto_refresh" in st.session_state:
    st.session_state.auto_refresh = True

mapping = {
            "20 mintues":{"period":"20", "granularity":"minute", "raw": 20},
            "15 mintues":{"period":"15", "granularity":"minute", "raw": 15},
            "10 mintues":{"period":"10", "granularity":"minute", "raw": 10},
            "5 mintues":{"period":"5", "granularity":"minute", "raw": 5}
            }

with st.expander("Configure Dashboard", expanded=True):
    left, right = st.columns(2)
    
    with left:
        auto_refresh = st.checkbox('Auto Refresh?', st.session_state.auto_refresh)
        if auto_refresh:
            number = st.number_input("Refresh rate in seconds", value = st.session_state.sleep_time)
            st.session_state.sleep_time = number
    
    with right:
        time_ago = st.radio("Time period to cover", mapping.keys(), horizontal=True, key="Time Ago")
        
    st.header("Live Kafka Facility Resources")
    
minute = mapping[time_ago]["period"]
print(str(minute))

query = f"""SELECT created_date, salesordernumber, COUNT(*) AS num_of_transactions, COUNT(DISTINCT(salesordernumber)) AS 'num orders',
            (toInt32(ROUND(SUM(salesamount), 2))) AS sales
            FROM "default".vw_sales
            WHERE created_date >= dt - INTERVAL(minute) MINUTE
            GROUP BY created_date, salesordernumber
            ORDER BY 1;"""

df = pd.read_sql(query, engine)
df.style.format('{:,}')

metric1, metric2, metric3 = st.columns(3)

metric1.metric(
                label = "No of transactions",
                value = (df['Num_of_transaction'].sum()),
                )

metric2.metric(
                label = "Num of Orders",
                value = (df['Num Orders'].sum()),
                )

metric3.metric(
                label = "Sales Amount",
                value = (df.sales.sum()),
                )

st.header(f"Transaction since last {minute} minutes....")

st.line_chart(data = df, x = "created_date", y = "sales")

st.write(df[['salesordernumber', 'created_date', 'sales']])

if auto_refresh:
    time.sleep(number)
    st.experimental_rerun()
    
# streamlit run app.py










