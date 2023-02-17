import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import streamlit as st
import seaborn as sns


# river: https://water.weather.gov/ahps/forecasts.php

# https://www.ncei.noaa.gov/access/past-weather/
# https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/doc/GHCND_documentation.pdf
# https://www.ncei.noaa.gov/access/search/data-search/daily-summaries
# https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/locations/FIPS:06057/detail
# https://openweathermap.org/history
DONNER = 'USW00023226' # 'USC00042467' 
TRUCKEE = 'USS0020K13S' # 'USC00049043' #  US1CANV0051   (USS0020K13S = best)
START = '2017-01-01'
END = '2021-12-01'
STATION = TRUCKEE
WEATHER_URL = f'https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations={STATION}&startDate={START}&endDate={END}&format=JSON&units=metric'


res = requests.get(WEATHER_URL).json()
df = pd.DataFrame(res)
# df = df.drop(columns=['STATION', 'TMIN', 'TMAX', 'TOBS'])

# cols = df.columns.drop('DATE')
# df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
# datatime_str = "%Y-%m-%d"
# df.DATE = df.DATE.apply(
#     lambda x: datetime.strptime(x, datatime_str)
# )


if __name__ == '__main__':
    df = get_climate_data()
    # df = df.set_index('DATE')
    # st.dataframe(data=df)
    # st.line_chart(df.PRCP)
    # st.line_chart(df.WESD)
    # st.line_chart(df.TAVG)
    # st.line_chart(df.SNWD)

    # fig = sns.pairplot(df )
    # st.pyplot(fig)
