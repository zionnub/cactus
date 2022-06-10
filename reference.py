import os.path
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq as pbq
import datetime as dt
from datetime import date
from datetime import datetime
import numpy as np
import sys
import sendgrid
import base64
import logging
import datapane as dp
import math
import plotly.express as px
from plotly.graph_objs import *
from bs4 import BeautifulSoup as bs
from premailer import transform
from google.cloud import bigquery
# import altair as alt
# from datapane import templates as tmp
from plotly.subplots import make_subplots
import plotly.graph_objects as go
# import markdown
import seaborn as sns
import plotly as pt
import os
import pyarrow
import google
from google.cloud import bigquery_storage
# from zipfile import ZipFile
import grpc
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId)
import warnings
# print(dp.__version__)
warnings.filterwarnings('ignore','.*Property:.*')

cm = sns.light_palette("orange", as_cmap=True)
cmg = sns.light_palette("yellow", as_cmap=True)

logging.basicConfig(level=logging.INFO,format = '%(asctime)s:%(levelname)s:%(message)s')
logging.info('RCA Initiated')
try:
    if(os.environ['USER']=='sdas'):
        development = True
    else:
        development = False
except:
    development= False

if(development== True):
    try:
        from dotenv import load_dotenv
        load_dotenv(".env")
        sg=  os.environ.get('SENDGRID_API_KEY')
        credentials = service_account.Credentials.from_service_account_file('/Users/sdas/Documents/cred_new.json')
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/sdas/Documents/cred_new.json"
        logging.info("fetched cred from local")
    except:
        logging.info("error fetching local credentials")
else:
    sg = os.environ['SENDGRID_API_KEY']
    credentials = service_account.Credentials.from_service_account_file('/credentials/credentials.json')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/credentials/credentials.json"
sg = sendgrid.SendGridAPIClient(sg)
client = bigquery.Client(project='noonbigmerch')

def sendemail(to_email,cc_email,from_email,subject,message_body,attachments):
    to_email_list = list()
    for i in range(len(to_email)):
        to_email_list.append(dict({'email':to_email[i]}))
    cc_email_list = list()
    for j in range(len(cc_email)):
        cc_email_list.append(dict({'email':cc_email[j]}))
    #cc_email_list.append(dict({'email':cc_email}))
    from_email_id = dict({'email':from_email})
    #from_email_id.append(dict({'email':from_email}))
    personalizations = list()
    personalizations.append(dict({'to':to_email_list,'cc':cc_email_list,'subject':subject}))
    content = list()
    content.append(dict({'type':'text/html','value': message_body}))
    msg = dict({'personalizations':personalizations,'from':from_email_id,'content':content,'attachments':attachments})
    response = sg.send(msg)
    return response.status_code

# L30_start = str(((dt.datetime.now()-dt.timedelta(hours =44))-dt.timedelta(days=30)).date())
# L30_end = str(((dt.datetime.now()-dt.timedelta(hours =44)).date()-dt.timedelta(days=30)))

# sys.executable
#!/opt/anaconda3/bin/python -m pip install --upgrade slimmer

css = """ <html>
        <head><style>
  .sticker {
    margin: auto;
    text-align: center;
    position: -webkit-sticky;
    position: sticky;
    top: 0;
    background-color: yellow;
    padding: 5px;
    font-size: 20px;
    display: block
  }
  .center {
    text-align: center;
    background-color: yellow;
    padding: 10px;
    font-size: 30px;
    display: block
  }
  .center_plane {
    text-align: center;
    padding: 10px;
    font-size: 30px;
    display: block
  }
  .center_small {
    text-align: center;
    padding: 2px;
    font-size: 20px;
    display: block
  }
    .cent {
      text-align: center;
      padding: 2px;
      font-size: 40px;
      color: Blue;
      display: block
    }
  </style>
          </head>
          <body>
        """

def tpf(tp='live'):
    global benchnmark_start
    global benchnmark_end
    global observation_start
    global observation_end
    global Hour_start
    global Hour_End
    if(tp=='live'):
        # benchnmark_start = str((dt.datetime.now()-dt.timedelta(hours =23)).date())
        # benchnmark_end = str((dt.datetime.now()-dt.timedelta(hours =23)).date())
        # observation_start = str((dt.datetime.now()-dt.timedelta(hours =1)).date())
        # observation_end = str((dt.datetime.now()-dt.timedelta(hours =1)).date())
        benchnmark_start = str((dt.date.today()-dt.timedelta(days =1)))
        benchnmark_end = str((dt.date.today()-dt.timedelta(days =1)))
        observation_start = str((dt.date.today()-dt.timedelta(days =0)))
        observation_end = str((dt.date.today()-dt.timedelta(days =0)))

        # Hour_now = (dt.datetime.now()-dt.timedelta(hours =1)-dt.timedelta(minutes =30)).hour

        Hour_start= 0
        try:
            if(os.environ['USER']=='sdas'):
                Hour_End = (dt.datetime.now()-dt.timedelta(minutes = 90)).hour
            else:
                Hour_End = (dt.datetime.now()+dt.timedelta(hours =4)).hour
        except:
            Hour_End = (dt.datetime.now()+dt.timedelta(hours =4)).hour

    elif(tp=='yst'):
        # benchnmark_start = str((dt.datetime.now()-dt.timedelta(hours =44)).date())
        # benchnmark_end = str((dt.datetime.now()-dt.timedelta(hours =44)).date())
        # observation_start = str((dt.datetime.now()-dt.timedelta(hours =20)).date())
        # observation_end = str((dt.datetime.now()-dt.timedelta(hours =20)).date())
        benchnmark_start = str((dt.date.today()-dt.timedelta(days =2)))
        benchnmark_end = str((dt.date.today()-dt.timedelta(days =2)))
        observation_start = str((dt.date.today()-dt.timedelta(days =1)))
        observation_end = str((dt.date.today()-dt.timedelta(days =1)))

        Hour_start= 0
        Hour_End = 23
    logging.info("--------------------------")
    logging.info('date assigned : '+str(tp))


def fetch_data(reporting_bu,su=False):
    if(su==True):
        ft_type='shopping_unit'
    else:
        ft_type='product_type'


    platform_query="""
            select country,date_config,hour,sum(gmv_aed) as gmv_sum,count(distinct order_pdate) as days_count,
            round(sum(gmv_aed)/count(distinct order_pdate)) as gmv_average,
            from
            (
            select distinct
            order_nr,
            item_nr,country,
            sc.family,
            shopping_unit,product_type,product_subtype,
            sc.brand_code,
            extract(hour from order_timestamp_gst) as hour,
            order_pdate,
            sku_config,
            case when order_pdate between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""' then "bm"
            when order_pdate between '"""+observation_start+"""' and '"""+observation_end+"""' then "obs"
            else null
            end as date_config,
            round(gmv_aed) gmv_aed
            from `noonbigmktg.reporting_noon.sales_complete` as sc
            left join (select sku_config,product_type,product_subtype from `noonbigmktg.reporting_noon.catalog`) using(sku_config)
            left join (select sku_config,reporting_bu,shopping_unit from `noonbigmktg.cache_noon.sku_attributes`) as sa using(sku_config)
            where  extract(hour from order_timestamp_Gst)<="""+str(Hour_End)+""" and extract(hour from order_timestamp_gst) >="""+str(Hour_start)+"""
            and (order_pdate between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""'
            or order_pdate between '"""+observation_start+"""' and '"""+observation_end+"""')
            )
            group by 1,2,3
                    """
    query_job = client.query(platform_query)
    # logging.info(platform_query)
    platform_data = query_job.to_dataframe()
    # platform_data =  pbq.read_gbq(platform_query,project_id = 'noonbigmerch',credentials=credentials, dialect = 'standard')
    platform_data = platform_data.fillna(0)
    logging.info('Platform Hourly Data Fetched for :' +str(reporting_bu))

    hourly_query="""with data as

            (

            select country,family,shopping_unit,product_type,product_subtype,brand_code,date_config,hour,sum(gmv_aed) as gmv_sum,count(distinct order_pdate) as days_count,
            round(sum(gmv_aed)/count(distinct order_pdate)) as gmv_average,
            from
            (
            select distinct
            order_nr,
            item_nr,country,
            sc.family,
            shopping_unit,product_type,product_subtype,
            (ifnull(rca_stream,sc.brand_code)) as brand_code,
            extract(hour from order_timestamp_gst) as hour,
            order_pdate,
            sku_config,
            case when order_pdate between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""' then "bm"
            when order_pdate between '"""+observation_start+"""' and '"""+observation_end+"""' then "obs"
            else null
            end as date_config,
            round(gmv_aed) gmv_aed
            from `noonbigmktg.reporting_noon.sales_complete` as sc
            left join (select sku_config,product_type,product_subtype from `noonbigmktg.reporting_noon.catalog`) using(sku_config)
            left join (select sku_config,reporting_bu,shopping_unit from `noonbigmktg.cache_noon.sku_attributes`) as sa using(sku_config)
            left join `noonbigmktg.cache_noon.sku_rca_stream` using(sku_config)
            where  extract(hour from order_timestamp_Gst)<="""+str(Hour_End)+""" and extract(hour from order_timestamp_gst) >="""+str(Hour_start)+"""
            and (order_pdate between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""'
            or order_pdate between '"""+observation_start+"""' and '"""+observation_end+"""')
            and reporting_bu='"""+reporting_bu+"""')
            group by 1,2,3,4,5,6,7,8
            )

            select country,family,shopping_unit,product_type,brand_code,if(pst_rank > 20,"others",product_subtype) product_subtype ,date_config,hour,gmv_sum,days_count,gmv_average from
            (select * from data
            left join
            (select distinct country,family,product_subtype,row_number() over(partition by country,family order by sum(gmv_sum) desc) as pst_rank     from data
                group by  1,2,3) using(country,family,product_subtype))
                    """
    query_job = client.query(hourly_query)
    # logging.info(hourly_query)
    hourly_data = query_job.to_dataframe()
    hourly_data = hourly_data.fillna(0)
    logging.info('Hourly Data Fetched for :' +str(reporting_bu))

    sku_query="""select * except(sku_pt_rank) from
                (
                select * , row_number() over(partition by country,family,"""+str(ft_type)+""" order by abs(delta) desc ) as sku_pt_rank from
                (
                select country,family,shopping_unit,product_type,product_subtype,sku_config,name,
                 sum(case when date_config ='bm' then gmv_average else 0 end) as bm_gmv,
                 sum(case when date_config ='obs' then gmv_average else 0 end) as obs_gmv,
                 (sum(case when date_config ='obs' then gmv_average else 0 end)-sum(case when date_config ='bm' then gmv_average else 0 end)) as delta
                  from
                (
                select country,family,shopping_unit,product_type,product_subtype,sku_config,name,date_config
                ,count(distinct order_pdate) as day_count
                ,sum(gmv_aed) as gmv
                ,round(sum(gmv_aed)/count(distinct order_pdate)) as gmv_average
                 from
                (
                select distinct
                order_nr,
                item_nr,country,family,shopping_unit,
                product_type,product_subtype,
                order_pdate,
                sku_config,
                left(title_en,30) as name,
                case when order_pdate between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""' then "bm"
                when order_pdate between '"""+observation_start+"""' and '"""+observation_end+"""' then "obs"
                else null
                end as date_config,
                round(gmv_aed) gmv_aed
                from `noonbigmktg.reporting_noon.sales_complete` as sc
                left join (select sku_config,product_type,product_subtype,title_en from `noonbigmktg.reporting_noon.catalog`) using(sku_config)
                left join (select sku_config,reporting_bu,shopping_unit from `noonbigmktg.cache_noon.sku_attributes`) as sa using(sku_config)
                where  extract(hour from order_timestamp_Gst)<="""+str(Hour_End)+""" and extract(hour from order_timestamp_gst) >="""+str(Hour_start)+"""
                and (order_pdate between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""'
                or order_pdate between '"""+observation_start+"""' and '"""+observation_end+"""')
                and reporting_bu='"""+reporting_bu+"""'
                )
                group by 1,2,3,4,5,6,7,8
                )
                group by 1,2,3,4,5,6,7)
                left join (
                select sku_config,country,
                round(sum(case when date_range ='bm' then live_perc else 0 end),2) as bm_live,
                round(sum(case when date_range ='obs' then live_perc else 0 end),2) as obs_live,
                round(sum(case when date_range ='bm' then retail_live_perc else 0 end),2) as bm_r_live,
                round(sum(case when date_range ='obs' then retail_live_perc else 0 end),2) as obs_r_live,
                round(sum(case when date_range ='bm' then mean_price else 0 end),0) as bm_lp,
                round(sum(case when date_range ='obs' then mean_price else 0 end),0) as obs_lp,
                round(sum(case when date_range ='bm' then if(retail_price=99999999,null,retail_price) else 0 end),0) as bm_r_lp,
                round(sum(case when date_range ='obs' then if(retail_price=99999999,null,retail_price) else 0 end),0) as obs_r_lp
                from
                (
                select sku_config,country,
                date_range,
                safe_divide(sum(is_live),count(is_live)) as live_perc,
                safe_divide(sum(is_live_retail),count(is_live_retail)) as retail_live_perc,
                safe_divide(sum(if(is_live=1,cast(least_price as int64),0)),count(if(is_live=1,least_price,Null))) as mean_price,
                safe_divide(sum(if(is_live_retail=1,cast(retail_price as int64),0)),count(if(is_live=1,retail_price,Null))) as retail_price
                from
                (select sku_config,country,
                case when date between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""' then "bm"
                when date between '"""+observation_start+"""' and '"""+observation_end+"""' then "obs"
                else null end as date_range,
                date,
                hour,
                if(greatest(is_live_retail,is_live_fbn,is_live_b2b)>0,1,0) as is_live ,
                is_live_retail,
                least(ifnull(price_retail,99999999),ifnull(price_fbn,99999999),ifnull(price_b2b,99999999)) as least_price,
                min(ifnull(price_retail,99999999)) as retail_price
                from `noonbigmktg.merch_noon.sku_log`
                where  hour<="""+str(Hour_End)+""" and hour >="""+str(Hour_start)+"""
                and (date between '"""+benchnmark_start+"""' and '"""+benchnmark_end+"""'
                or date between '"""+observation_start+"""' and '"""+observation_end+"""')
                group by 1,2,3,4,5,6,7,8)
                group by 1,2,3)
                group by 1,2
                                ) using(sku_config,country)
                ) where sku_pt_rank <30
                order by delta desc
                """
    query_job = client.query(sku_query)
    # logging.info(sku_query)
    sku_data = query_job.to_dataframe()
    sku_data = sku_data.fillna(0)
    logging.info('sku Data Fetched for :' +str(reporting_bu))

    return hourly_data,sku_data,platform_data



def simple_line_chart(source,X_axis,Y_axis,Color,title):
    fig = px.line(source, x=X_axis, y=Y_axis, color=Color,template='none',title=title)
    obs_data = source[source['date_config']=='obs']
    x_list=list(obs_data['hour'])
    y_list=list(obs_data['gmv_average'])
    fig.add_bar(x=x_list, y=y_list,marker=dict(color="MediumPurple"),name="obs histogram")
    # fig.show()
    return fig

def plot_report(plot,chart_caption=None,label=None):
    if(chart_caption != None):
        plot_report_dp = dp.Plot(plot,caption=chart_caption,label=label)
    else:
        plot_report_dp = dp.Plot(plot,label=label)
    return plot_report_dp


def plot_radial_pie(data,X_axis,category,title):
    obs_data = data[data['date_config']=="obs"]
    bm_data = data[data['date_config']=="bm"]
    labels_obs=list(obs_data[category])
    values_obs= list(obs_data[X_axis])
    labels_bm=list(bm_data[category])
    values_bm= list(bm_data[X_axis])
    # logging.info(labels_obs)
    # logging.info(values_obs)
    # logging.info(labels_bm)
    # logging.info(values_bm)
    fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'}, {'type':'domain'}]])
    fig.add_trace(go.Pie(labels=labels_obs, values=values_obs, name="Obs",scalegroup='two'),1, 1)
    fig.add_trace(go.Pie(labels=labels_bm, values=values_bm, name="bm",scalegroup='two'), 1, 2)
    if(category=='product_subtype'):
        fig.update_traces(textposition='inside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    #
    # # Use `hole` to create a donut-like pie chart
    fig.update_traces(hole=.4, hoverinfo="label+percent+name")
    #
    fig.update_layout(
        title_text=str(title)+" Benchmark Vs Observed",
        # Add annotations in the center of the donut pies.
        annotations=[dict(text='OBS', x=0.20, y=0.5, font_size=20, showarrow=False),
                     dict(text='BM', x=0.8, y=0.5, font_size=20, showarrow=False)])
    # fig.show()
    return fig

def calculate_impedence(data,label=None,custom_sort=False,report=True):
    country_dict = {'SA': 0, 'AE': 1, 'EG': 3}
    data['delta'] = (data['obs']-data['bm'])
    total_delta = data['delta'].sum()
    total_bm = data['bm'].sum()
    data['bm_share']=data['bm']/total_bm
    data['delta_share']=data['delta']/total_delta
    data['growth'] = np.where(data['bm'].isin([np.nan, 0]),1,data['delta']/data['bm'])
    max_bm_share=max(np.where(data['bm_share']>0.3,0.3,data['bm_share']))
    max_abs_growth = max(abs(data['growth']))
    max_abs_delta = max(abs(data['delta']))
    data['snorm'] = np.where(data['bm_share']>0.3,0.3,data['bm_share'])/(2*max_bm_share)
    data['gnorm'] = abs(data['growth'])/(2*max_abs_growth)
    data['dnorm'] = abs(data['delta'])/(2*max_abs_delta)

    def imp_cal(a,b,c):
        imp = math.atanh(a)+math.atanh(b)+math.atanh(c)
        return imp
    # data['impedence'] = math.atanh(data['snorm'])+math.atanh(data['gnorm'])+math.atanh(data['dnorm'])
    data['impedence'] = data.apply(lambda row : math.atanh(row['snorm'])+math.atanh(row['gnorm'])+math.atanh(row['dnorm']), axis = 1)
    data = data.drop(['snorm','dnorm','gnorm'], axis=1)
    if(custom_sort==False):
        data = data.sort_values(by=["impedence"], ascending=False)
    else:
        data = data.sort_values(by=['country'], key=lambda x: x.map(country_dict))

    if(report==True):
        data=data.style.format({'bm': "{:,.0f}",'obs': "{:,.0f}",'delta': "{:,.0f}",'bm_share': "{:.1%}",'delta_share': "{:.1%}",'growth': "{:.1%}",
                                                         'impedence': "{:,.2f}"}).bar(subset= pd.IndexSlice[:, ['delta','growth']], align='mid', color=['#d65f5f', '#5fba7d']).background_gradient(subset=['impedence']
                                                         ,cmap=cm).hide_index()
        table_data = dp.Table(data,label=label)
    else:
        table_data=data
    # +math.atanh(abs(data['growth'])/(2*max_abs_growth))+math.atanh(abs(data['delta'])/(2*max_abs_delta))
    logging.info("impedence calculated")
    return table_data


def prep_country_data(data,rbu=None):
    # country_dict = {'SA': 0, 'AE': 1, 'EG': 3}
    if(rbu):
        country_text = dp.Text(""" ## Country Data (Reporting BU) :"""+str(rbu))
    else:
        country_text = dp.Text(""" ## Country Data - Platform level """)
    country_df = data.groupby(['country','date_config']).agg({'gmv_average':'sum'}).reset_index()
    # country_df = country_df.sort_values(by=['country'], key=lambda x: x.map(country_dict))
    country_df = pd.DataFrame(country_df.pivot_table(index='country', columns='date_config', values='gmv_average').reset_index())
    country_df = pd.DataFrame(country_df.to_records()).drop("index",axis=1)
    country_df_data = calculate_impedence(country_df,custom_sort=True)
    country_df_data = dp.Group(country_text,country_df_data)
    return country_df_data


def get_bignumder_html(obs_num,bm_num,ratio,title,state=True):
    if(state==True):
        class_mod="success"
        direc = "up"
    else:
        class_mod="danger"
        direc = "down"
    html_data="""
    <html>
  <head>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3" crossorigin="anonymous">
    <style>
      .arrow {
        border: solid #000;
        border-width: 0 3px 3px 0;
        display: inline-block;
        padding: 3px
      }

      .right {
        transform: rotate(-45deg);
        -webkit-transform: rotate(-45deg)
      }

      .left {
        transform: rotate(135deg);
        -webkit-transform: rotate(135deg)
      }

      .up {
        transform: rotate(-135deg);
        -webkit-transform: rotate(-135deg)
      }

      .down {
        transform: rotate(45deg);
        -webkit-transform: rotate(45deg)
      }

      .block {
        display: block;
        width: 100%;
        border: none;
        padding: 14px 28px;
        font-size: 16px;
        cursor: pointer;
        text-align: center
      }
    </style>
  </head>
  <body>
    <div class="card">
      <div class="card-header text-center">"""+str(title)+"""</div>
      <div class="card-title"></div>
      <div class="card-body">
        <blockquote class="blockquote mb-0">
          <p class="text-center">
            <span style="font-size:22px">"""+str(obs_num)+"""</span>
          </p>
          <footer class="blockquote-footer text-center">From <cite title="Source Title">"""+str(bm_num)+"""</cite>
          </footer>
        </blockquote>
      </div>
      <div class="text-center">
        <button type="button" class="btn btn-"""+str(class_mod)+""" position-relative block">
          <span style="font-size:15px"> """+str(ratio)+""" %&nbsp; <i class="arrow """+str(direc)+""""></i>
          </span></button>
      </div>
    </div>
  </body>
    """
    return html_data


def get_bignumbers(platform_data,hourly_data,rbu,filter="Overall"):
    if(filter!="Overall"):
        platform_data = platform_data[platform_data['country']==filter]
        hourly_data = hourly_data[hourly_data['country']==filter]
    Obs_number_platform = platform_data[platform_data['date_config']=='obs']['gmv_average'].sum()
    bm_number_platform = platform_data[platform_data['date_config']=='bm']['gmv_average'].sum()
    plat_change = round((abs(Obs_number_platform-bm_number_platform)/bm_number_platform)*100)
    if(Obs_number_platform > 1000000 and bm_number_platform > 1000000 ):
        Obs_number_platform_str = str(round(Obs_number_platform/1000000,2))+"M"
        bm_number_platform_str = str(round(bm_number_platform/1000000,2))+"M"
    else:
        Obs_number_platform_str = str(round(Obs_number_platform/1000))+"K"
        bm_number_platform_str = str(round(bm_number_platform/1000))+"K"
    # plat_obs_bgnum = dp.BigNumber(heading="PLATFORM", value= Obs_number_platform_str,prev_value=bm_number_platform_str,change=str(plat_change)+"%",is_upward_change=(Obs_number_platform>bm_number_platform),is_positive_intent=(Obs_number_platform>bm_number_platform))
    plat_obs_bgnum=get_bignumder_html(obs_num=Obs_number_platform_str,bm_num=bm_number_platform_str,ratio=plat_change,title="PLATFORM",state=(Obs_number_platform>bm_number_platform))
    plat_obs_bgnum = dp.HTML(plat_obs_bgnum)
    # plat_bm_bgnum = dp.BigNumber(heading="Platform BM GMV", value= bm_number_platform,change=str(plat_change)+"%",is_upward_change=(Obs_number_platform<=bm_number_platform))
    Obs_number_rbu = hourly_data[hourly_data['date_config']=='obs']['gmv_average'].sum()
    bm_number_rbu = hourly_data[hourly_data['date_config']=='bm']['gmv_average'].sum()
    rbu_change = round((abs(Obs_number_rbu-bm_number_rbu)/bm_number_rbu)*100)
    if(Obs_number_platform > 1000000 and bm_number_platform > 1000000 ):
        Obs_number_rbu_str = str(round(Obs_number_rbu/1000000,2))+"M"
        bm_number_rbu_str = str(round(bm_number_rbu/1000000,2))+"M"
    else:
        Obs_number_rbu_str = str(round(Obs_number_rbu/1000))+"K"
        bm_number_rbu_str = str(round(bm_number_rbu/1000))+"K"
    # rbu_obs_bgnum = dp.BigNumber(heading=str(rbu.upper()), value= Obs_number_rbu_str,prev_value=bm_number_rbu_str,change=str(rbu_change)+"%",is_upward_change=(Obs_number_rbu>bm_number_rbu),is_positive_intent=(Obs_number_rbu>bm_number_rbu))
    # rbu_bm_bgnum = dp.BigNumber(heading=str(rbu)+" BM GMV", value= bm_number_rbu,change=str(rbu_change)+"%",is_upward_change=(Obs_number_rbu<=bm_number_rbu))
    rbu_obs_bgnum=get_bignumder_html(obs_num=Obs_number_rbu_str,bm_num=bm_number_rbu_str,ratio=rbu_change,title=rbu.upper(),state=(Obs_number_rbu>bm_number_rbu))
    rbu_obs_bgnum = dp.HTML(rbu_obs_bgnum)
    bgnum = dp.Group(dp.HTML(css+"<p class='center_small'>"+filter+"</p><hr>"),dp.Group(plat_obs_bgnum,rbu_obs_bgnum,columns=2))
    return bgnum




def hourly_plot_func(data,family):
    df_consolidated = data.groupby(['date_config','hour']).agg({'gmv_average':'sum'}).reset_index()
    get_chart = simple_line_chart(df_consolidated,'hour','gmv_average','date_config',title="Hourly GMV : "+str(family))
    graph_plot = dp.Plot(get_chart,label=family)
    return graph_plot

def dataproc(data,grouping):
    data = data.groupby([grouping,'date_config']).agg({'gmv_average':'sum'}).reset_index()
    data = data.dropna()

    rad_plot = plot_report(plot_radial_pie(data,'gmv_average',grouping,title=grouping),label=grouping)
    df_pivot = pd.DataFrame(data.pivot_table(index=grouping, columns='date_config', values='gmv_average').reset_index())
    df_pivot = pd.DataFrame(df_pivot.to_records()).drop("index",axis=1)
    df_pivot_data = calculate_impedence(df_pivot,label=grouping)
    return rad_plot,df_pivot_data
def gainlose(data,gain=True):
    if(gain==True):
        g_ascending=False
        t_label ="Top 10 Gainers"
    else:
        g_ascending=True
        t_label ="Top 10 Losers"
    data = data.sort_values(by='delta',ascending=g_ascending)
    try:
        su_length=len(data['shopping_unit'].unique())
    except:
        su_length=0
    if(su_length>=1):
        data = data.drop(['country','family','shopping_unit','product_type'],axis=1)
    else:
        data = data.drop(['country','family','shopping_unit','product_type'],axis=1)
    data = data[:10]
    # logging.info(data)
    gainer_data=data.style.format({'bm_gmv': "{:,.0f}",'obs_gmv': "{:,.0f}",'delta': "{:,.0f}",'bm_live': "{:.1%}",'obs_live': "{:.1%}",'bm_r_live': "{:.1%}",
                                                     'obs_r_live': "{:,.1%}",'bm_lp': "{:,.0f}",'obs_lp': "{:,.0f}",'bm_r_lp': "{:,.0f}",'obs_r_lp': "{:,.0f}"}).bar(subset= pd.IndexSlice[:, ['delta']], align='mid', color=['#d65f5f', '#5fba7d']).hide_index()
    gainer_data = dp.Table(gainer_data,label=t_label)
    return gainer_data

def gainlose_brand(data,gain=True):
    # data=family_base_data
    if(gain==True):
        g_ascending=False
        t_label ="Top 10 Gainer Brands"
    else:
        g_ascending=True
        t_label ="Top 10 Loser Brands"

    data = data.groupby(['brand_code','date_config']).agg({'gmv_average':'sum'}).reset_index()
    df_pivot = pd.DataFrame(data.pivot_table(index='brand_code', columns='date_config', values='gmv_average').reset_index())
    df_pivot = pd.DataFrame(df_pivot.to_records()).drop("index",axis=1)
    df_pivot = df_pivot.fillna(0)
    data = calculate_impedence(df_pivot,report=False)
    data = data.sort_values(by='delta',ascending=g_ascending)
    if(gain==True):
        data = data[data['delta']>=0]
    else:
        data = data[data['delta']<0]
    data = data[:10]
    # logging.info(data)
    gainer_data=data.style.format({'bm': "{:,.0f}",'obs': "{:,.0f}",'delta': "{:,.0f}",'bm_share': "{:.1%}",'delta_share': "{:.1%}",'growth': "{:.1%}",
                                                     'impedence': "{:,.2f}"}).bar(subset= pd.IndexSlice[:, ['delta','growth']], align='mid', color=['#d65f5f', '#5fba7d']).background_gradient(subset=['impedence']
                                                     ,cmap=cm).hide_index()
    gainer_data = dp.Table(gainer_data,label=t_label)
    return gainer_data

# def prep_sku_data(data,grouping):
#     pt_list = data[grouping].unique()
#     sku_data_list=[]
#     for pt in pt_list:
#         pst_data = data[data[grouping]==pt]
#         pst_data = pst_data.drop(['country','family','shopping_unit','product_type','sku_pt_rank'],axis=1)
#         pst_data =pst_data.sort_values(by='delta', key=abs,ascending=False)
#
#         # pst_data=pst_data.set_index(['sku_config'])
#         # pst_data=pst_data.style.set_sticky(axis="index")
#         pst_data=pst_data.style.format({'bm_gmv': "{:,.0f}",'obs_gmv': "{:,.0f}",'delta': "{:,.0f}",'bm_live': "{:.1%}",'obs_live': "{:.1%}",'bm_r_live': "{:.1%}",
#                                                          'obs_r_live': "{:,.1%}",'bm_lp': "{:,.0f}",'obs_lp': "{:,.0f}",'bm_r_lp': "{:,.0f}",'obs_r_lp': "{:,.0f}"}).bar(subset= pd.IndexSlice[:, ['delta']], align='mid', color=['#d65f5f', '#5fba7d']).hide_index()
#         pst_data = dp.Table(pst_data,label=pt)
#         sku_data_list.append(pst_data)
#     if(len(pt_list)>1):
#         sku_text = dp.Text("""## SKU performance""")
#         sku_text_1 = dp.Text("""- please select the respective product type / shopping unit from dropdown""")
#         sku_text = dp.Group(sku_text,sku_text_1)
#         sku_c_data = dp.Select(blocks=sku_data_list,type=dp.SelectType.DROPDOWN)
#         sku_c_data = dp.Group(sku_text,sku_c_data)
#     else:
#         sku_text = dp.Text("""## SKU performance""")
#         sku_c_data=pst_data
#         sku_c_data = dp.Group(sku_text,sku_c_data)
#     return sku_c_data

def prep_complete_data(data,sku_df,platform_data,su):
    # country_dict = {'SA': 0, 'AE': 1, 'EG': 3}
    country_dp =[]
    hr_break = dp.HTML("<HR>")
    country_text = dp.Text(""" ## Country X Family """)
    hourly_text = dp.Text(""" ## Hourly Plot """)
    country_list =['SA','AE','EG']
    for country in country_list:
        logging.info(country)
        # country='AE'
        # data = hourly_data
        country_base_df = data[data['country']==country]
        sku_df_country = sku_df[sku_df['country']==country]
        country_df = country_base_df.groupby(['country','family','date_config']).agg({'gmv_sum':'sum'}).reset_index()
        # country_df = country_df.sort_values(by=['country'], key=lambda x: x.map(country_dict))
        country_df = pd.DataFrame(country_df.pivot_table(index=['country','family'], columns='date_config', values='gmv_sum').reset_index())
        country_df = pd.DataFrame(country_df.to_records()).drop("index",axis=1)
        country_dpf = calculate_impedence(country_df,label=country)
        logging.info("impedence calculated for : country :"+country)
        family_list = country_base_df['family'].unique()
        logging.info(family_list)
        fam_plot_list=[]
        for family in family_list:
            logging.info("Processing data for family : "+family)
            # family='fragrance'
            try:
                family_base_data = country_base_df[country_base_df['family']==family]
                sku_df_family = sku_df_country[sku_df_country['family']==family]
                fam_hourly_plot = hourly_plot_func(family_base_data,family)
                logging.info("Hourly plot for : country :"+country+"& Family :" +family)
                # platform_data
                platform_plot = hourly_plot_func(platform_data,family='platform')
                logging.info("Hourly plot for platform")
                fam_hourly_plot = dp.Group(fam_hourly_plot,platform_plot,columns=2)
                fam_hourly_plot=dp.Group(hr_break,hourly_text,fam_hourly_plot,label=family)
                if(su==True):
                    try:
                        pt_plot,pt_data = dataproc(family_base_data,'shopping_unit')
                        # sku_dp=prep_sku_data(sku_df_family,'shopping_unit')
                    except:
                        pt_plot,pt_data = dataproc(family_base_data,'product_type')
                        # sku_dp=prep_sku_data(sku_df_family,'product_type')
                    fam_hourly_plot = dp.Group(fam_hourly_plot,dp.HTML("<br><hr>"),pt_plot,pt_data,label=family)
                else:
                    pt_plot,pt_data = dataproc(family_base_data,'product_type')
                    pst_plot,pst_data = dataproc(family_base_data,'product_subtype')
                    # sku_dp=prep_sku_data(sku_df_family,'product_type')
                    fam_hourly_plot = dp.Group(fam_hourly_plot,dp.HTML("<br><hr>"),pt_plot,pt_data,dp.HTML("<br><hr>"),pst_plot,pst_data,label=family)
                top_gbrands=gainlose_brand(family_base_data,gain=True)
                top_lbrands=gainlose_brand(family_base_data,gain=False)
                top_gaienrs = gainlose(sku_df_family,gain=True)
                top_losers = gainlose(sku_df_family,gain=False)
                fam_hourly_plot = dp.Group(fam_hourly_plot,dp.HTML("<br><hr>"),dp.Text('## Top Gainer Brands /revenue stream'),top_gbrands,dp.Text('## Top Loser Brands / revenue stream'),top_lbrands,dp.Text('## Top Gainers'),top_gaienrs,dp.HTML("<br><hr>"),dp.Text('## Top Losers'),top_losers,label=family)
                fam_plot_list.append(fam_hourly_plot)
                logging.info("completed for Family : "+family)
            except:
                logging.info(" Error processing for family : "+family)
        if(len(family_list)>1):
            fam_plot_grouped= dp.Select(blocks=fam_plot_list,type=dp.SelectType.TABS)
        else:
            fam_plot_grouped=fam_hourly_plot

        country_dfp = dp.Group(hr_break,country_text,country_dpf,dp.HTML("<br><hr>"),fam_plot_grouped,label=country)
        country_dp.append(country_dfp)
        logging.info("completed for country : "+country)
    # country_dp_grouped =dp.Group(blocks=country_dp)
    country_dp_grouped= dp.Select(blocks=country_dp,type=dp.SelectType.DROPDOWN)
    # country_df_data = dp.Group(hr_break,country_text,country_dp_grouped)

    return country_dp_grouped



# tpf(tp='yst')
# hourly_data,sku_data =  fetch_data('beauty',su=True)

def create_report(rbu,easy_name,su=False,to_emails=['sdas@noon.com']):
    logging.info("=======================================================")
    logging.info(rbu)
    logging.info("=======================================================")
    rs_list=['mobiles']
    if(rbu in rs_list):
        rs =True
        logging.info(rs)
    html_header ="""
            <div class="sticker">
                <a class="nav-link" href="#"><img src="https://z.nooncdn.com/s/app/com/noon/images/logos/noon-black-en.svg"></a>
            </div><div><h2 class ="center_plane">POP report (Reporting BU level) : """+str(easy_name)+""" </h2>
            </div><hr>
                    """
    html_header = dp.HTML(css+html_header)

    glossary_text = css+"""<p>
                    <em>bm</em>&nbsp&nbsp&nbsp : Benchmark  <br>
                    <em>obs </em>&nbsp&nbsp&nbsp             : Observation  <br>
                    <em>bm_share</em>&nbsp&nbsp&nbsp         : Benchmark share  <br>
                    <em>delta_share</em>&nbsp&nbsp&nbsp      : Contribution of the items towards movement  <br>
                    <em>impedence </em>&nbsp&nbsp&nbsp       : Calculated severity index  <br>
                    <em>bm_gmv</em>&nbsp&nbsp&nbsp           : Benchmark GMV  <br>
                    <em>obs_gmv </em>&nbsp&nbsp&nbsp         : Observed GMV  <br>
                    <em>bm_live </em>&nbsp&nbsp&nbsp         : Percentage of time the sku was live on platform during <em>Benchmark</em>  <br>
                    <em>obs_live </em>&nbsp&nbsp&nbsp        : Percentage of time the sku was live on platform during <em>Observation</em>  <br>
                    <em>bm_r_live </em>&nbsp&nbsp&nbsp       : Percentage of time the sku was live from retail platform during <em>Benchmark</em>  <br>
                    <em>obs_r_live</em>&nbsp&nbsp&nbsp       : Percentage of time the sku was live from retail platform during <em>Observation</em>  <br>
                    <em>bm_lp </em>&nbsp&nbsp&nbsp           : least price accross platform during <em>Benchmark</em>  <br>
                    <em>obs_lp </em>&nbsp&nbsp&nbsp          : least price accross platform during <em>Observation</em>  <br>
                    <em>bm_r_lp </em>&nbsp&nbsp&nbsp         : least price from retail during <em>Benchmark</em>  <br>
                    <em>obs_r_lp </em>&nbsp&nbsp&nbsp        : least price from retail during <em>Observation</em>  <br>
                    </p>
                    """
    glossary = dp.Group(dp.HTML("<HR>"),dp.Text("## Glossary"),dp.HTML(glossary_text),dp.HTML("<HR>"))
    def date_table(tp):
        if(tp in ['yst','live']):
            data = [{'Benchmark Date':benchnmark_start,'Observation Date': observation_end,'Time Observed':str(Hour_start)+" - "+str(Hour_End)}]
        else:
            data = [{'Benchmark Start Date':benchnmark_start,'Benchmark End Date':benchnmark_end, 'Observation Start Date': observation_start,'Observation End Date': observation_end,'Time Observed':str(Hour_start)+" - "+str(Hour_End)}]
        df = pd.DataFrame(data)
        df = df.style.hide_index()
        date_df = dp.Table(df)
        return date_df
    if(sys.argv[1]=='daily'):
        tpf(tp='yst')
        # rbu='beauty'
        # su=True reporting_bu
        # hourly_data,sku_data =  fetch_data(reporting_bu='beauty',su=True)

        runtime_data =date_table('yst')
        hourly_data,sku_data,platform_data =  fetch_data(rbu,su)
        bgnums_1 = dp.Group(get_bignumbers(platform_data,hourly_data,rbu),get_bignumbers(platform_data,hourly_data,rbu,filter="SA"),columns=2)
        bgnums_2 = dp.Group(get_bignumbers(platform_data,hourly_data,rbu,filter="AE"),get_bignumbers(platform_data,hourly_data,rbu,filter="EG"),columns=2)
        bgnums = dp.Group(dp.Group(bgnums_1,bgnums_2,columns=2),dp.HTML("<HR>"))
        platform_data_dp = prep_country_data(platform_data)
        country_data = prep_country_data(hourly_data,rbu)
        complete_data = prep_complete_data(hourly_data,sku_data,platform_data,su)
        try:
            yst_page=dp.Page(label="Yesterday",blocks=[dp.HTML(css+"<p class='cent'>Yesterday Report</p>"),html_header,runtime_data,bgnums,platform_data_dp,country_data,complete_data,glossary])
        except:
            yst_page=dp.Page(title="Yesterday",blocks=[dp.HTML(css+"<p class='cent'>Yesterday Report</p>"),html_header,runtime_data,bgnums,platform_data_dp,country_data,complete_data,glossary])
    tpf(tp='live')
    runtime_data = date_table('live')
    hourly_data,sku_data,platform_data =  fetch_data(rbu,su)
    bgnums_1_live = dp.Group(get_bignumbers(platform_data,hourly_data,rbu),get_bignumbers(platform_data,hourly_data,rbu,filter="SA"),columns=2)
    bgnums_2_live = dp.Group(get_bignumbers(platform_data,hourly_data,rbu,filter="AE"),get_bignumbers(platform_data,hourly_data,rbu,filter="EG"),columns=2)
    bbgnums_live = dp.Group(dp.Group(bgnums_1_live,bgnums_2_live,columns=2),dp.HTML("<HR>"))
    platform_data_dp = prep_country_data(platform_data)
    country_data = prep_country_data(hourly_data,rbu)
    complete_data = prep_complete_data(hourly_data,sku_data,platform_data,su)
    try:
        live_page=dp.Page(label="Live",blocks=[dp.HTML(css+"<p class='cent'>Live Report</p>"),html_header,runtime_data,bbgnums_live,platform_data_dp,country_data,complete_data,glossary])
    except:
        live_page=dp.Page(title="Live",blocks=[dp.HTML(css+"<p class='cent'>Live Report</p>"),html_header,runtime_data,bbgnums_live,platform_data_dp,country_data,complete_data,glossary])
    # r=  dp.Report(live_page,yst_page)
    if(sys.argv[1]=='daily'):
        r=  dp.Report(live_page,yst_page)
    else:
        r=  dp.Report(live_page)
    # r=  dp.Report(yst_page)
    # r =tmp.add_header(r,html_header)
    # easy_name ='beauty'
    # file_path ='/Users/sdas/Documents/charting_exp/'+str(easy_name)+'.html'
    now = datetime.now()
    timestamp_file = datetime.timestamp(now)
    filename =str(easy_name)+' - '+str(int(timestamp_file))+'.html'
    r.save(path=filename,open=False)
    if(sys.argv[1]=='daily'):
        subject ="POP Report ("+rbu+"): Date: "+observation_end
    else:
        subject ="Evening Refresh : POP Report ("+rbu+"): Date: "+observation_end
    with open(filename, 'rb') as f:
        data = f.read()
        f.close()
    message = Mail(
        from_email=('sdas@noon.com', 'POP RCA'),
        to_emails= to_emails,
        # to_emails= ('sdas@noon.com'),
        subject= subject,
        html_content= """<b>Please download the HTML file.</b>"""
                        )
    encoded = base64.b64encode(data).decode()
    attachment = Attachment()
    attachment.file_content = FileContent(encoded)
    attachment.file_type = FileType('Text/HTML')
    attachment.file_name = FileName(filename)
    attachment.disposition = Disposition('attachment')
    attachment.content_id = ContentId('Example Content ID')
    message.attachment = attachment

    try:
        # sendgrid_client = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        logging.info(response.status_code)
        logging.info(response.body)
        logging.info(response.headers)
        os.remove(filename)
    except Exception as e:
        logging.info(e.message)

## Beauty
beauty_emails = [
    ('rjain@noon.com'),
    ('ktirumala@noon.com'),
    ('ryekula@noon.com'),
    ('palaa@noon.com'),
    ('karimgendy@noon.com'),
    ('saktiwari@noon.com'),
    ('miafzal@noon.com'),
    ('karimgendy@noon.com'),
    ('lgemayel@noon.com'),
    ('zakhan@noon.com'),
    ('sasingh@noon.com'),
    ('musingh@noon.com'),
    ('ajain@noon.com'),
    ('sdas@noon.com'),
    ('analytics@noon.com'),
    ('onsite-core@noon.com'),
    ('mosalem@noon.com'),
    ('melansary@noon.com'),
    ('aalqersh@noon.com'),
    ('ahmsayibrahim@noon.com'),
    ('ggarg@noon.com')]
# to_emails =[('das.shreekant@gmail.com')]
# beauty_emails =[('sdas@noon.com')]
try:
    create_report('beauty',easy_name='beauty',su=True,to_emails=beauty_emails)
except:
    logging.info("Error in create report for Beauty ")
# ### Beauty
baby_emails = [
    ('tzantout@noon.com'),
    ('Nchoudhary@noon.com'),
    ('kdave@noon.com'),
    ('aparameethal@noon.com'),
    ('rarajput@noon.com'),
    ('amusk@noon.com'),
    ('vkhandelwal@noon.com'),
    ('sasingh@noon.com'),
    ('musingh@noon.com'),
    ('ajain@noon.com'),
    ('sdas@noon.com'),
    ('analytics@noon.com'),
    ('onsite-core@noon.com'),
    ('mosalem@noon.com'),
    ('melansary@noon.com'),
    ('aalqersh@noon.com')]
# baby_emails =[('sdas@noon.com')]
try:
    create_report('baby/toys',easy_name='baby_toys',su=False,to_emails=baby_emails)
except:
    logging.info("Error in create report for Baby ")
### home
home_emails = [
    ('aaltall@noon.com'),
    ('gcaesar@noon.com'),
    ('japsingh@noon.com'),
    ('hmallak@noon.com'),
    ('home-uae@noon.com'),
    ('homeksa@noon.com'),
    ('sasingh@noon.com'),
    ('musingh@noon.com'),
    ('ajain@noon.com'),
    ('sdas@noon.com'),
    ('analytics@noon.com'),
    ('onsite-core@noon.com'),
    ('aalqersh@noon.com'),
    ('aelhout@noon.com'),
    ('melhakim@noon.com')]
# home_emails =[('sdas@noon.com')]
try:
    create_report('home',easy_name='Home',su=False,to_emails=home_emails)
except:
    logging.info("Error in create report for Home ")
### health_nutrition
health_emails = [
    ('syrizvi@noon.com'),
    ('sdas@noon.com'),
    ('health_nutritionteam@noon.com'),
    ('analytics@noon.com'),
    ('aalqersh@noon.com')]
# health_emails =[('sdas@noon.com')]
try:
    create_report('H&N ',easy_name='health_nutrition',su=False,to_emails=health_emails)
except:
    logging.info("Error in create report for H&N ")

### fashion
fashion_emails = [
    ('nchoudhary@noon.com'),
    ('sdas@noon.com'),
    ('nkshah@noon.com'),
    ('analytics@noon.com'),
    ('dkibbi@noon.com'),
    ('shmathur@noon.com')]
# health_emails =[('sdas@noon.com')]
try:
    create_report('fashion',easy_name='fashion',su=False,to_emails=fashion_emails)
except:
    logging.info("Error in create report for fashion ")
