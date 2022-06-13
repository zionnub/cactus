############################################
############################################
############################################
## Import the libraries
import pandas as pd
import datapane as dp
import plotly as pt
import plotly.express as px
import numpy as np

############################################
############################################
############################################
## Load and process the raw data
raw_data = pd.read_csv('raw_data.csv')
# raw_data.head()

# raw_data.info()
raw_data['Revenue USD']= raw_data['Revenue USD'].replace(',','',regex=True)
raw_data['Revenue USD']=round(pd.to_numeric(raw_data['Revenue USD']))
raw_data['Job creation date']=pd.to_datetime(raw_data['Job creation date'])
raw_data['Research Area'] = raw_data['Research Area'].fillna("Unknown")
raw_data['weekday'] =raw_data['Job creation date'].dt.day_name()
raw_data['dayofweek']=  raw_data['Job creation date'].dt.dayofweek

raw_data_modified = raw_data.copy()
raw_data_modified['mod_month'] =raw_data_modified['Month']
raw_data_modified = raw_data_modified.replace({'mod_month':{4:1,5:2,6:3,7:4,8:5,9:6,10:7,11:8,12:9,1:10,2:11,3:12}})
raw_data_modified['mod_month'] = np.where(raw_data_modified['mod_month']<10,str(0)+raw_data_modified['mod_month'].astype(str),raw_data_modified['mod_month'].astype(str))
raw_data_modified['fy_month'] = raw_data_modified['Fiscal year'].astype(str)+"-"+raw_data_modified['mod_month'].astype(str)

############################################
############################################
############################################
## Create  header

css = ("""
        <html>
        <head><style>
          }
            .cent {
              text-align: center;
              padding: 2px;
              font-size: 40px;
              color: Black;
              display: block
            }
            span {
             display: inline;
             color: red;
                }
            #block_container
            {
                text-align:center;
            }
            #bloc1
            {
                display:inline;
                font-size: 40px;
            }
            #bloc2
            {
                display:inline;
                font-size: 20px;
            }
            div.a {
              text-align: center;
            }

          </style>
                  </head>
                  <body>
        """)


header = dp.HTML(css+"<div id='block_container'><div id='bloc1'>CA<span>C</span>TUS</div><div id='bloc2'> Newsletter </div></div>")


############################################
############################################
############################################
## Create functions

## Bignumbers

def bignumbers(base_df):
    # base_df = p2m1_data_country
    base_df['units_pct']=round(base_df['jobs'].pct_change()*100,2)
    base_df['revenue_pct']=round(base_df['revenue'].pct_change()*100,2)
    base_df=base_df.fillna(0)

    bg_nr_2018_units=   dp.BigNumber(
             heading="2018",
             value=str(round(base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['jobs']/1000))+"k",
             change=base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['units_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['units_pct']>=0
             )
    bg_nr_2019_units=   dp.BigNumber(
             heading="2019",
             value=str(round(base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['jobs']/1000))+"k",
             change=base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['units_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['units_pct']>=0
             )
    bg_nr_2020_units=   dp.BigNumber(
             heading="2020",
             value=str(round(base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['jobs']/1000))+"k",
             change=base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['units_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['units_pct']>=0
             )
    bg_nr_2021_units=   dp.BigNumber(
             heading="2021",
             value=str(round(base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['jobs']/1000))+"k",
             change=base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['units_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['units_pct']>=0
             )
    if(base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['revenue']<1000000):
        rev_2018=str(round(base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['revenue']/1000))+"k"
    else:
        rev_2018=str(round(base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['revenue']/1000000,1))+"M"

    bg_nr_2018_revenue=   dp.BigNumber(
             heading="2018",
             value=rev_2018,
             change=base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['revenue_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2018'].iloc[0]['revenue_pct']>=0
             )
    if(base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['revenue']<1000000):
        rev_2019=str(round(base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['revenue']/1000))+"k"
    else:
        rev_2019=str(round(base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['revenue']/1000000,1))+"M"
    bg_nr_2019_revenue=   dp.BigNumber(
             heading="2019",
             value=rev_2019,
             change=base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['revenue_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2019'].iloc[0]['revenue_pct']>=0
             )
    if(base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['revenue']<1000000):
        rev_2020=str(round(base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['revenue']/1000))+"k"
    else:
        rev_2020=str(round(base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['revenue']/1000000,1))+"M"
    bg_nr_2020_revenue=   dp.BigNumber(
             heading="2020",
             value=rev_2020,
             change=base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['revenue_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2020'].iloc[0]['revenue_pct']>=0
             )
    if(base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['revenue']<1000000):
        rev_2021=str(round(base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['revenue']/1000))+"k"
    else:
        rev_2021=str(round(base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['revenue']/1000000,1))+"M"
    bg_nr_2021_revenue=   dp.BigNumber(
             heading="2021",
             value=rev_2021,
             change=base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['revenue_pct'],
             is_upward_change=base_df[base_df['Fiscal year']=='FY2021'].iloc[0]['revenue_pct']>=0
             )

    big_numbers = dp.Group(dp.Group(dp.HTML(css+"<div class='a'><h1 class='cent'><p class='cent'>Job Orders</p></h1><hr></div>"),dp.Group(bg_nr_2021_units,bg_nr_2020_units,bg_nr_2019_units,bg_nr_2018_units,columns=4)), \
                        dp.Group(dp.HTML(css+"<div class='a'><h1 class='cent'><p class='cent'>Revenue</p></h1><hr></div>"),dp.Group(bg_nr_2021_revenue,bg_nr_2020_revenue,bg_nr_2019_revenue,bg_nr_2018_revenue,columns=4)),columns=2)
    return big_numbers

## Create base summary
def summary(base_df,filter,selectable=""):
    base_df = base_df.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    base_df['revenue_per_job'] = round(base_df['revenue']/base_df['jobs'],0)
    filter_2 = filter.copy()
    filter_2.remove('Fiscal year')
    print(filter_2)
    if(filter_2 == None or filter_2 ==[]):
        base_df['jobs_YOY_change']=base_df.jobs.pct_change()
        base_df['revenue_YOY_change']= base_df['revenue'].pct_change()
        base_df['rpj_YOY_change']= base_df['revenue_per_job'].pct_change()
    else:
        base_df['jobs_YOY_change']=base_df.groupby(filter_2).jobs.pct_change()
        base_df['revenue_YOY_change']=base_df.groupby(filter_2).revenue.pct_change()
        base_df['rpj_YOY_change']=base_df.groupby(filter_2).revenue_per_job.pct_change()

    base_df = base_df.fillna(0)
    # base_df=base_df.style.format({'count_YOY_change': "{:.1%}",'revenue_YOY_change': "{:.1%}",'rpj_YOY_change': "{:.1%}"}).hide(axis='index')
    return base_df
def styler(df):
    df=df.style.format({'jobs': "{:.0f}",'revenue': "{:.0f}",'revenue_per_job': "{:.0f}",'jobs_YOY_change': "{:.1%}",'revenue_YOY_change': "{:.1%}",'rpj_YOY_change': "{:.1%}"}).hide(axis='index')
    return df

## Create New vs repeat
def new_repeat(data):
    data_pivot=pd.DataFrame(data.pivot_table(index='Fiscal year', columns='Category', values=['jobs','revenue'],aggfunc=np.sum).reset_index())
    data_pivot.columns = [f'{j}_{i}' for i, j in data_pivot.columns]
    data_pivot['new job %'] =round(data_pivot['New customer_jobs']/(data_pivot['New customer_jobs']+data_pivot['Repeat business_jobs']),4)
    data_pivot['new revenue %'] =round(data_pivot['New customer_revenue']/(data_pivot['New customer_revenue']+data_pivot['Repeat business_revenue']),4)
    data_pivot=data_pivot.style.format({ 'New customer_jobs':"{:,.0f}",'Repeat business_jobs':"{:,.0f}",'New customer_revenue':"{:,.0f}",'Repeat business_revenue':"{:,.0f}",'new job %': "{:.1%}",'new revenue %': "{:.1%}"}).hide(axis='index')
    data_pivot_gr = dp.Group(dp.Text(""" ## New Vs Repeat"""),dp.Table(data_pivot))
    return data_pivot_gr

#pie plot
def pie_plots(level,filter):
    print(level)
    level =str(level)
    print(level)
    monthly_data_pie = raw_data_modified.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    monthly_data_pie = monthly_data_pie[monthly_data_pie['Fiscal year']=='FY2021']
    fig_pie = px.pie(monthly_data_pie, values=monthly_data_pie["jobs"], names=monthly_data_pie[f'{level}'], title=level+' job share in 2021')
    fig_pie_2 = px.pie(monthly_data_pie, values=monthly_data_pie["revenue"], names=monthly_data_pie[f'{level}'], title=level+' revenue share in 2021')
    fig_pie_3 = px.pie(monthly_data_pie, values=round(monthly_data_pie["revenue"]/monthly_data_pie["jobs"]), names=monthly_data_pie[f'{level}'], title=level+' wise RPJ in 2021')
    fig_pie_3.update_traces(textinfo='value')

    pie_plots= dp.Group(dp.HTML("<h1>"+level+" Statistics in 2021 </h1>"),dp.Group(fig_pie,fig_pie_2,fig_pie_3,columns=3))
    return pie_plots

def country_sb(country,filter):
# filter=['Fiscal year','Service','Document type']
    country_df = raw_data[raw_data['Country']==country]
    country_df_fg = country_df.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    country_df_fg['revenue'] =country_df_fg['revenue'].astype(int)
    fy_2018 = country_df_fg[country_df_fg['Fiscal year']=="FY2018"]
    fy_2019 = country_df_fg[country_df_fg['Fiscal year']=="FY2019"]
    fy_2020 = country_df_fg[country_df_fg['Fiscal year']=="FY2020"]
    fy_2021 = country_df_fg[country_df_fg['Fiscal year']=="FY2021"]
    sb_2018=px.sunburst(fy_2018,path=filter,values='revenue', hover_data=['jobs'],branchvalues="total")
    sb_2018.update_layout(margin = dict(t=0, l=0, r=0, b=0))
    sb_2019=px.sunburst(fy_2019,path=filter,values='revenue', hover_data=['jobs'],branchvalues="total")
    sb_2019.update_layout(margin = dict(t=0, l=0, r=0, b=0))
    sb_2020=px.sunburst(fy_2020,path=filter,values='revenue', hover_data=['jobs'],branchvalues="total")
    sb_2020.update_layout(margin = dict(t=0, l=0, r=0, b=0))
    sb_2021=px.sunburst(fy_2021,path=filter,values='revenue', hover_data=['jobs'],branchvalues="total")
    sb_2021.update_layout(margin = dict(t=0, l=0, r=0, b=0))
    grouped_sb= dp.Group(dp.Plot(sb_2021),dp.Plot(sb_2020),dp.Plot(sb_2019),dp.Plot(sb_2018),columns=4)
    return grouped_sb




############################################
############################################
############################################
## Page 1 - OverView

##Big Number Module
filter_bg =['Fiscal year']
p1m1_data = raw_data.groupby(filter_bg).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
p1m1 = bignumbers(p1m1_data)

## Base Table
filter =['Fiscal year']
fy_smmary=styler(summary(raw_data,filter))
table =dp.Group(dp.Text(""" ##  Year on Year Comaprision """), dp.Table(fy_smmary),dp.Text(""" * YOY : Year on Year , RPJ : Revenue per Job"""))

## Country growth
filter_c =['Country','Fiscal year']
country_data=summary(raw_data,filter_c)
country_data = country_data[country_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
country_data = styler(country_data)
country_table =dp.Group(dp.Text(""" ## Country Comaprision """), dp.Table(country_data))


## Service growth
filter_c =['Service','Fiscal year']
service_data=summary(raw_data,filter_c)
service_data = service_data[service_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
service_data = styler(service_data)
service_table =dp.Group(dp.Text(""" ## Service Comaprision """), dp.Table(service_data))

## Research Aread growth
filter_c =['Research Area','Fiscal year']
res_data=summary(raw_data,filter_c)
res_data = res_data[res_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
res_data = styler(res_data)
res_table =dp.Group(dp.Text(""" ## Research Area Comaprision """), dp.Table(res_data))


## Research Aread growth
filter_c =['Document type','Fiscal year']
dt_data=summary(raw_data,filter_c)
dt_data = dt_data[dt_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
dt_data = styler(dt_data)
dt_table_mix =dp.Group(dp.Text(""" ## Document Type Comaprision """), dp.Table(dt_data))

## Nre Vs repeat
filter =['Fiscal year','Category']
p1nr_data = raw_data.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
p1nr= new_repeat(p1nr_data)



filter = ['Country','Fiscal year']
country_pie =pie_plots('Country',filter)
filter = ['Service','Fiscal year']
service_pie =pie_plots('Service',filter)
filter = ['Document type','Fiscal year']
doc_pie =pie_plots('Document type',filter)
filter = ['Research Area','Fiscal year']
research_pie =pie_plots('Research Area',filter)



## Trendlines
# Add monthly charts

# line chart function
def line_chart(filter,code):
    df = raw_data_modified.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    fig = px.line(df, x=df["fy_month"], y=df["jobs"], color=df[code], title=code+" Wise Job Order")
    fig_2 = px.line(df, x=df["fy_month"], y=df["revenue"], color=df[code], title=code+" Wise Revenue")
    plot_grouped = dp.Group(dp.Plot(fig),dp.Plot(fig_2))
    return plot_grouped

country_line_plot = dp.Group(dp.Text(""" ## Country Timeline"""),dp.Text(" * Modified the months to match Fiscal year. FY2018-01 represent month of Arpil in the year 2017 and so on"),line_chart(['Country','fy_month'],"Country"))

Service_line_plot = dp.Group(dp.Text(""" ## Service Timeline"""),line_chart(['Service','fy_month'],"Service"))

Document_type_line_plot = dp.Group(dp.Text(""" ## Document Type Timeline"""),line_chart(['Document type','fy_month'],"Document type"))

Research_Area_line_plot = dp.Group(dp.Text(""" ## Research Area Timeline"""),line_chart(['Research Area','fy_month'],"Research Area"))

linecharts = dp.Group(country_pie,country_table,country_line_plot,service_pie,service_table,Service_line_plot,doc_pie,dt_table_mix,Document_type_line_plot,research_pie,res_table,Research_Area_line_plot)

## Weekday Research
daily_data= raw_data[raw_data['Job creation date'].notnull()]
daily_data=  daily_data.groupby(['weekday','dayofweek']).agg(job_count=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
daily_data = daily_data.sort_values(by='dayofweek')
daily_bar = px.bar(daily_data, x='weekday', y='job_count')
daily_bar_plot=dp.Group(dp.Text(""" ## Weekday Analysis"""),dp.Plot(daily_bar))

## forecast
forecast = dp.Group(dp.Text(""" ## Forecast for upcoming days"""),dp.Media(file='./overall_forecast.png'))

## Month Seasonality
Monthly_Seasonality = dp.Group(dp.Text(""" ## Monthly seasonality"""),dp.Media(file='./monthy_trends_overall.png'))

## Forecast datapane
forecast_data = pd.read_csv("forecast_future.csv")
forecast_data=forecast_data.style.format({'Expected_jobs': "{:.0f}",'Min_jobs': "{:.0f}",'Max_jobs': "{:.0f}"}).hide(axis='index')
forecast_table = dp.Group(dp.Text(""" ### Forecasted data """),dp.Table(forecast_data))

page_1 = dp.Page(title="OverView",blocks=[header,p1m1,table,p1nr,linecharts,daily_bar_plot,forecast,forecast_table])


############################################
############################################
############################################
## Page 2 - country

country_data =[]
country_list = raw_data.groupby('Country').agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count")).reset_index()
country_list = country_list.sort_values(by=['jobs'], ascending=False).Country.unique()

# Bignumber Data
filter_country =['Country','Fiscal year']
p2m1_data = raw_data.groupby(filter_country).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
#new vs Repeat Data
filter_cat =['Country','Fiscal year','Category']
p2nr_data = raw_data.groupby(filter_cat).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()


for country in country_list:
    #BigNumber module
    country_dp= dp.HTML(css+"<div id='block_container'><div id='bloc1'><span>"+country+"</span></div>")
    p2m1_data_country =p2m1_data[p2m1_data['Country']==country]
    p2m1_data_country = p2m1_data_country.drop(columns=['Country'])
    p2m1_bgnumber = bignumbers(p2m1_data_country)
    country_smmary=summary(raw_data,filter_country)
    country_smmary=country_smmary[country_smmary['Country']==country]
    # country_table = dp.Table(styler(country_smmary),label=country)
    country_table = dp.Group(dp.Text(""" ##  Year on Year Comaprision """), dp.Table(styler(country_smmary)),dp.Text(""" * YOY : Year on Year , RPJ : Revenue per Job"""))
    # new vs repeat
    country_nr_data = p2nr_data[p2nr_data['Country']==country]
    country_nr=new_repeat(country_nr_data)
    filter_sb= ['Fiscal year','Service','Research Area']
    sb_plots_ra = dp.Group(dp.Text(""" ## Sunburst Chart for Services and Research Area across Fiscal year"""),country_sb(country,filter_sb))
    filter_sb1=['Fiscal year','Service','Document type']
    sb_plots_dt = dp.Group(dp.Text(""" ## Sunburst Chart for Services and Document type across Fiscal year"""),country_sb(country,filter_sb1))
    country_base_data = raw_data[raw_data['Country']==country]
    filter_c =['Service','Fiscal year']
    country_service_data=summary(country_base_data,filter_c)
    country_service_data = country_service_data[country_service_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
    country_service_data = styler(country_service_data)
    country_service_table =dp.Group(dp.Text(""" ## Service Comaprision """), dp.Table(country_service_data))
    filter_c =['Research Area','Fiscal year']
    res_service_data=summary(country_base_data,filter_c)
    res_service_data = res_service_data[res_service_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
    res_service_data = styler(res_service_data)
    res_service_table =dp.Group(dp.Text(""" ## Research Area Comaprision """), dp.Table(res_service_data))
    filter_c =['Document type','Fiscal year']
    dt_service_data=summary(country_base_data,filter_c)
    dt_service_data = dt_service_data[dt_service_data['Fiscal year']=="FY2021"].sort_values(by=['jobs'] ,ascending=False)
    dt_service_data = styler(dt_service_data)
    dt_service_table =dp.Group(dp.Text(""" ## Document type Comaprision """), dp.Table(dt_service_data))

    country_gp= dp.Group(country_dp,p2m1_bgnumber,country_table,country_nr,sb_plots_ra,sb_plots_dt,country_service_table,res_service_table,dt_service_table,label=country)
    country_data.append(country_gp)
country_grouped=dp.Select(blocks=country_data,type=dp.SelectType.TABS)
page_2 = dp.Page(title="Country",blocks=[header,country_grouped])

############################################
############################################
############################################
## Cretae report

Overview_an = """
# Overall

## Year on Year- Jobs and Revenue analysis

- The overall growth rate of job orders are in the range of 8-11%
- Job order in the Fiscal year 2021 grew by 10.31% over the previous fiscal Year
- The jump in revenue is quite substantial as compared to job Orders
- The revenue growth in the Fiscal year 2021 is around 25% as compared to 10.53% growth rate in Jobs
- The difference in growth rate of revenue to jobs indicate substantial growth in revenue per job

## New vs Repeat

- The new vs repeat seems to be constant across four financial years  and usually hovers around 25% for jobs and 29% for revenue
- New customer acquisition in Australia is comparatively lower as compared to other countries

## Country Statistics

- Australia has the highest market share of 51.4% w.r.t jobs and 54% w.r.t revenue
- Followed by Malaysia and Thailand hovering around 18% for both
- Although Mexico has high jobs compared to Argentina, the revenue generated is less, indicating very low revenue per jobs ($18)
- Sweden has the highest revenue per Jobs
- Though Australia is growing continuously, the growth rate of Malaysia is the highest
- Australia has very high seasonality and  usually observe huge spike in the month of February and March
## Service Statistics

- Global budget , Global economy and Global premium economy are three most important services and contribute over 81% of the Jobs
- Among the 3, global premium has the highest growth rate
- In terms of revenue, Global premium at 21.7% share is almost on par with Global budget at 24.3%

## Document Type Statistics

- Standard document type is the most relevant document type and contribute to 71.7% of the revenue.
- Standard document type also has high growth rate

## Research Area Statistics

- All the four categories has decent contribution towards revenue and Jobs
- However cryptocurrencies  have picked up recently and contribute 31% towards job & 29.4% towards revenue
- There are missing tags in the fiscal month of FY2019-08 (November 2018) and FY2019-10(December 2018)

 ## Weekday analysis

- Monday receives the maximum jobs, and its a downward trend from there on
- Saturday and Sunday are quite sloppy as compared to other weekdays
---

# countries

## Australia

- Jobs growth in FY2021 is 5.3% , however the revenue grew by 25%
- Due to disproportionate growth in jobs vs revenue, we observe high changes in revenue per job
- New acquisition is lower than overall average, and hovers around 19%
- Global premium economy became the largest service in FY2021 , replacing Global budget
- This growth might be linked to increase in cryptocurrencies jobs which is the largest research area in Global premium economy
- Within Global budget, manufacturing & Law are the biggest research Area
- Standard document type dominates over other document types
- Standard ,Medical , Letter,Long and Book has positive growth rate, while Short,PPT, collaboration & others have negative growth rate

## Thailand

- Jobs in FY2021 grew by 8.7% , and revenue grew by 26%
- Similar to Australia , we observe high growth in revenue per orders
- New customer acquisition hovers around 26% mark which is close to overall average
- Global Budget and Global premium economy are two largest services in Thailand
- Law dominates over other Research area
- Standard ,Medical , Letter,Long and Book has positive growth rate, while Short,PPT, collaboration & others have negative growth rate

## Malaysia

- Malaysia has the high growth in jobs at 22.7%, as well as in revenue at 21.9%
- The revenue per jobs seems to be pegged around $44 mark since last 3 years
- New customer acquisition is higher than overall average around 35%
- Global economy and Global budget are the biggest services in Malaysia
- Law and Telemedicine are the most prominent research Area
- Standard document type has over 85% share
- Only PPT has negative growth rate

## Sweden

- Both jobs(20.8%) and revenue(28.6%) grew over 20% mark
- Revenue per job is highest across all countries
- New customer acquisition is very high at 40%
- Global budget and Global economy are the biggest service
- Sweden has equal distribution across all research area

## Mexico
- We observe very high growth rate in Mexico
- However the scale is really small as compared to other countries (around 4.4%)
- Mexico has the lowest revenue per job (around $18)
- We observe dip in new customer acquisition
- Global budget is the largest service
- Telemedicine and cryptocurrencies are largest research area in Mexico

## Argentina
- Argentina has the lowest contribution towards jobs around(3.38%)
- However, Argentina has higher revenue as compared to Mexico and hence has higher revenue per jobs
- The growth rate is slower as compared to other countries , around 11.7% for jobs and 15.2% for revenue
- The new customer acquisition is declining across the financial years
- Global budget, localized others and   Global economy are the largest services in Argentina
- manufacturing and cryptocurrencies are the largest research area

"""



page_3 = dp.Page(title="Analysis",blocks=[header,dp.Text(Overview_an)])


############################################
############################################
############################################
## Cretae report

r=  dp.Report(page_1,page_2,page_3)
r.save(path="cactus_newsletter.html",open=False)
