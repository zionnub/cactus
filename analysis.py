import pandas as pd
import datapane as dp
import plotly as pt
import plotly.express as px
import numpy as np
# from prophet import Prophet


raw_data = pd.read_csv('raw_data.csv')
# raw_data.head()

# raw_data.info()
raw_data['Revenue USD']= raw_data['Revenue USD'].replace(',','',regex=True)
raw_data['Revenue USD']=pd.to_numeric(raw_data['Revenue USD'])
raw_data['Job creation date']=pd.to_datetime(raw_data['Job creation date'])
raw_data['Research Area'] = raw_data['Research Area'].fillna("Unknown")
raw_data['weekday'] =raw_data['Job creation date'].dt.day_name()
raw_data['dayofweek']=  raw_data['Job creation date'].dt.dayofweek



######################## Module Start ########################
css = """
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
        """


header = dp.HTML(css+"<div id='block_container'><div id='bloc1'>CA<span>C</span>TUS</div><div id='bloc2'> Newsletter </div></div>")

# bignumbers
filter =['Fiscal year']
base_df = raw_data.groupby(filter).agg(job_count=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()

base_df['units_pct']=round(base_df['job_count'].pct_change()*100,2)
base_df['revenue_pct']=round(base_df['revenue'].pct_change()*100,2)
base_df=base_df.fillna(0)

bg_nr_2018_units=   dp.BigNumber(
         heading="2018",
         value=str(round(base_df[base_df['Fiscal year']=='FY2018'].job_count[0]/1000))+"k",
         change=base_df[base_df['Fiscal year']=='FY2018'].units_pct[0],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2018'].units_pct[0]>=0
      )
bg_nr_2019_units=   dp.BigNumber(
         heading="2019",
         value=str(round(base_df[base_df['Fiscal year']=='FY2019'].job_count[1]/1000))+"k",
         change=base_df[base_df['Fiscal year']=='FY2019'].units_pct[1],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2019'].units_pct[1]>=0
      )
bg_nr_2020_units=   dp.BigNumber(
         heading="2020",
         value=str(round(base_df[base_df['Fiscal year']=='FY2020'].job_count[2]/1000))+"k",
         change=base_df[base_df['Fiscal year']=='FY2020'].units_pct[2],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2020'].units_pct[2]>=0
      )
bg_nr_2021_units=   dp.BigNumber(
         heading="2021",
         value=str(round(base_df[base_df['Fiscal year']=='FY2021'].job_count[3]/1000))+"k",
         change=base_df[base_df['Fiscal year']=='FY2021'].units_pct[3],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2021'].units_pct[3]>=0
      )
if(base_df[base_df['Fiscal year']=='FY2018'].revenue[0]<1000000):
    rev_2018=str(round(base_df[base_df['Fiscal year']=='FY2018'].revenue[0]/1000))+"k"
else:
    rev_2018=str(round(base_df[base_df['Fiscal year']=='FY2018'].revenue[0]/1000000,1))+"M"

bg_nr_2018_revenue=   dp.BigNumber(
         heading="2018",
         value=rev_2018,
         change=base_df[base_df['Fiscal year']=='FY2018'].revenue_pct[0],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2018'].revenue_pct[0]>=0
      )
if(base_df[base_df['Fiscal year']=='FY2019'].revenue[1]<1000000):
    rev_2019=str(round(base_df[base_df['Fiscal year']=='FY2019'].revenue[1]/1000))+"k"
else:
    rev_2019=str(round(base_df[base_df['Fiscal year']=='FY2019'].revenue[1]/1000000,1))+"M"
bg_nr_2019_revenue=   dp.BigNumber(
         heading="2019",
         value=rev_2019,
         change=base_df[base_df['Fiscal year']=='FY2019'].revenue_pct[1],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2019'].revenue_pct[1]>=0
      )
if(base_df[base_df['Fiscal year']=='FY2020'].revenue[2]<1000000):
    rev_2020=str(round(base_df[base_df['Fiscal year']=='FY2020'].revenue[2]/1000))+"k"
else:
    rev_2020=str(round(base_df[base_df['Fiscal year']=='FY2020'].revenue[2]/1000000,1))+"M"
bg_nr_2020_revenue=   dp.BigNumber(
         heading="2020",
         value=rev_2020,
         change=base_df[base_df['Fiscal year']=='FY2020'].revenue_pct[2],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2020'].revenue_pct[2]>=0
      )
if(base_df[base_df['Fiscal year']=='FY2021'].revenue[3]<1000000):
    rev_2021=str(round(base_df[base_df['Fiscal year']=='FY2021'].revenue[3]/1000))+"k"
else:
    rev_2021=str(round(base_df[base_df['Fiscal year']=='FY2021'].revenue[3]/1000000,1))+"M"
bg_nr_2021_revenue=   dp.BigNumber(
         heading="2021",
         value=rev_2021,
         change=base_df[base_df['Fiscal year']=='FY2021'].revenue_pct[3],
         is_upward_change=base_df[base_df['Fiscal year']=='FY2021'].revenue_pct[3]>=0
      )

big_numbers = dp.Group(dp.Group(dp.HTML(css+"<div class='a'><h1 class='cent'><p class='cent'>Job Orders</p></h1><hr></div>"),dp.Group(bg_nr_2021_units,bg_nr_2020_units,bg_nr_2019_units,bg_nr_2018_units,columns=4)), \
                    dp.Group(dp.HTML(css+"<div class='a'><h1 class='cent'><p class='cent'>Revenue</p></h1><hr></div>"),dp.Group(bg_nr_2021_revenue,bg_nr_2020_revenue,bg_nr_2019_revenue,bg_nr_2018_revenue,columns=4)),columns=2)


# Summary function
def summary(base_df,filter,selectable=""):
    base_df = raw_data.groupby(filter).agg(job_count=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    base_df['revenue_per_job'] = round(base_df['revenue']/base_df['job_count'],0)
    filter_2 = filter.copy()
    filter_2.remove('Fiscal year')
    print(filter_2)
    if(filter_2 == None or filter_2 ==[]):
        base_df['count_YOY_change']=base_df.job_count.pct_change()
        base_df['revenue_YOY_change']= base_df['revenue'].pct_change()
        base_df['rpj_YOY_change']= base_df['revenue_per_job'].pct_change()
    else:
        base_df['count_YOY_change']=base_df.groupby(filter_2).job_count.pct_change()
        base_df['revenue_YOY_change']=base_df.groupby(filter_2).revenue.pct_change()
        base_df['rpj_YOY_change']=base_df.groupby(filter_2).revenue_per_job.pct_change()

    base_df = base_df.fillna(0)
    # base_df=base_df.style.format({'count_YOY_change': "{:.1%}",'revenue_YOY_change': "{:.1%}",'rpj_YOY_change': "{:.1%}"}).hide(axis='index')
    return base_df
def styler(df):
    df=df.style.format({'count_YOY_change': "{:.1%}",'revenue_YOY_change': "{:.1%}",'rpj_YOY_change': "{:.1%}"}).hide(axis='index')
    return df
# Overall
# FY Summary
filter =['Fiscal year']
fy_smmary=styler(summary(raw_data,filter))
table = dp.Table(fy_smmary)



# Add monthly charts
raw_data_modified = raw_data.copy()
raw_data_modified['mod_month'] = np.where(raw_data_modified['Month']<10,str(0)+raw_data_modified['Month'].astype(str),raw_data_modified['Month'].astype(str))
raw_data_modified['fy_month'] = raw_data_modified['Fiscal year'].astype(str)+"-"+raw_data_modified['mod_month'].astype(str)

#line chart
monthly_data = raw_data_modified.groupby(['Country','fy_month']).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
fig = px.line(monthly_data, x=monthly_data["fy_month"], y=monthly_data["jobs"], color=monthly_data['Country'], title="Country Wise Job Order")
fig_2 = px.line(monthly_data, x=monthly_data["fy_month"], y=monthly_data["revenue"], color=monthly_data['Country'], title="Country Wise Revenue")
#pie plot
monthly_data_pie = raw_data_modified.groupby(['Country','Fiscal year']).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
monthly_data_pie = monthly_data_pie[monthly_data_pie['Fiscal year']=='FY2021']
fig_pie = px.pie(monthly_data_pie, values=monthly_data_pie["jobs"], names=monthly_data_pie["Country"], title='Country job share in 2021')
fig_pie_2 = px.pie(monthly_data_pie, values=monthly_data_pie["revenue"], names=monthly_data_pie["Country"], title='Country revenue share in 2021')
fig_pie_3 = px.pie(monthly_data_pie, values=round(monthly_data_pie["revenue"]/monthly_data_pie["jobs"]), names=monthly_data_pie["Country"], title='Country wise RPJ in 2021')
fig_pie_3.update_traces(textinfo='value')


monthly_data_services = raw_data_modified.groupby(['Service','fy_month']).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
fig_3 = px.line(monthly_data_services, x=monthly_data_services["fy_month"], y=monthly_data_services["jobs"], color=monthly_data_services['Service'], title="Service Wise Job Order")
fig_4 = px.line(monthly_data_services, x=monthly_data_services["fy_month"], y=monthly_data_services["revenue"], color=monthly_data_services['Service'], title="Service Wise Revenue")

# line chart function
def line_chart(filter,code):
    df = raw_data_modified.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    fig = px.line(df, x=df["fy_month"], y=df["jobs"], color=df[code], title=code+" Wise Job Order")
    fig_2 = px.line(df, x=df["fy_month"], y=df["revenue"], color=df[code], title=code+" Wise Revenue")
    plot_grouped = dp.Group(dp.Plot(fig),dp.Plot(fig_2))
    return plot_grouped

country_line_plot = line_chart(['Country','fy_month'],"Country")

Service_line_plot = line_chart(['Service','fy_month'],"Service")

Document_type_line_plot = line_chart(['Document type','fy_month'],"Document type")

Research_Area_line_plot = line_chart(['Research Area','fy_month'],"Research Area")

# Daily trends
daily_data= raw_data[raw_data['Job creation date'].notnull()]
daily_data=  daily_data.groupby(['weekday','dayofweek']).agg(job_count=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
daily_data = daily_data.sort_values(by='dayofweek')
daily_bar = px.bar(daily_data, x='weekday', y='job_count')
daily_bar.show()




page=dp.Page(title="OverView",blocks=[header,big_numbers,table,country_line_plot,dp.Group(dp.Plot(fig_pie),dp.Plot(fig_pie_2),dp.Plot(fig_pie_3),columns=3),Service_line_plot,Document_type_line_plot,Research_Area_line_plot,dp.Plot(daily_bar)])

#### Create Country Page ####
date_range =pd.date_range(start="2018-01-01",end="2021-12-31")
date_df =pd.DataFrame(date_range)
date_df.columns=['DS']

country_df = raw_data[raw_data['Country']=="Australia"]

country_df=country_df.groupby('Job creation date').agg(Y=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
date_df_merged = date_df.merge(country_df, left_on='DS', right_on='Job creation date')
date_df_merged=date_df_merged.drop(columns=['Job creation date'])


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


    # grouped_sb= dp.Group(dp.Group(dp.Plot(sb_2021),dp.Plot(sb_2020),columns=2),dp.Group(dp.Plot(sb_2019),dp.Plot(sb_2018),columns=2))
    grouped_sb= dp.Group(dp.Plot(sb_2021),dp.Plot(sb_2020),dp.Plot(sb_2019),dp.Plot(sb_2018),columns=4)
    return grouped_sb

def new_repeat(country):
    country_df = raw_data[raw_data['Country']==country]
    country_df_fg = country_df.groupby(filter).agg(jobs=pd.NamedAgg(column="Job code", aggfunc="count"),revenue=pd.NamedAgg(column="Revenue USD", aggfunc="sum")).reset_index()
    country_df_fg=pd.DataFrame(country_df_fg.pivot_table(index='Fiscal year', columns='Category', values=['jobs','revenue'],aggfunc=np.sum).reset_index())
    country_df_fg.columns = [f'{j}_{i}' for i, j in country_df_fg.columns]
    country_df_fg['nvr job pct'] =round(country_df_fg['New customer_jobs']/(country_df_fg['New customer_jobs']+country_df_fg['Repeat business_jobs']),4)
    country_df_fg['nvr revenue pct'] =round(country_df_fg['New customer_revenue']/(country_df_fg['New customer_revenue']+country_df_fg['Repeat business_revenue']),4)
    country_df_fg=country_df_fg.style.format({'nvr job pct': "{:.1%}",'nvr revenue pct': "{:.1%}"}).hide(axis='index')
    return dp.Table(country_df_fg)
    # Apply column rename

    # country_df_fg = pd.DataFrame(country_df_fg.to_records()).drop("index",axis=1)
country_data =[]
country_list = raw_data['Country'].unique()
for country in country_list:
    filter_country =['Country','Fiscal year']
    country_smmary=summary(raw_data,filter_country)
    country_smmary=country_smmary[country_smmary['Country']==country]
    country_table = dp.Table(styler(country_smmary),label=country)
    # filter_nr =['Country','Fiscal year','Category']
    # nr_smmary=summary(raw_data,filter_nr)
    # nr_smmary=nr_smmary[nr_smmary['Country']==country]
    # nr_table = dp.Table(styler(nr_smmary),label=country
    filter_nr = filter=['Fiscal year','Category']
    nr_table=new_repeat(country)
    filter_sb= ['Fiscal year','Service','Research Area']
    sb_plots_ra =country_sb(country,filter_sb)
    filter_sb1=['Fiscal year','Service','Document type']
    sb_plots_dt =country_sb(country,filter_sb1)
    country_gp= dp.Group(country_table,nr_table,sb_plots_ra,sb_plots_dt,label=country)

    country_data.append(country_gp)
country_grouped=dp.Select(blocks=country_data,type=dp.SelectType.TABS)

page_2 = dp.Page(title="Country",blocks=[header,country_grouped])



#To add new vs repeat  , what kind of service new users use vs old user , add bignumbers on page 1
# Add daywise trends and analysis
r=  dp.Report(page,page_2)
r.save(path="cactus_newsletter.html",open=False)
