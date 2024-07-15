import os
from itertools import chain
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, RunReportRequest, Dimension
from google.analytics.data_v1beta.types import FilterExpression, Filter, Metric, FilterExpressionList
from google.analytics.data_v1beta.types.analytics_data_api import RunReportResponse

from dotenv import load_dotenv

load_dotenv()


import clickhouse_connect


#PATH_TO_JSON_CREDS_GA4 = 'ga4.json'

# Set env variable
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = PATH_TO_JSON_CREDS_GA4

FIELDS_TO_SET = {
    "transactions": {

      "table_name": "temporary.i_bartenev_ga4_app_api_transactions",

        "dimension_list": [
            "source",
            "medium",
            "campaignName",
            "platform",
            "city",
            "customEvent:transaction_id"
        ],
        "metric_list": [],
        "filters": FilterExpression(
                and_group=FilterExpressionList(expressions=[
                    FilterExpression(not_expression=FilterExpression(
                        filter=Filter(
                            field_name="platform",
                            string_filter=Filter.StringFilter(value='web')
                        )
                    )),
                    FilterExpression(
                        filter=Filter(
                            field_name="eventName",
                            string_filter=Filter.StringFilter(value="purchase",
                                                              match_type=Filter.StringFilter.MatchType.CONTAINS)
                        )
                    )
                ])
            ),

    },

    "sessions": {
      "table_name": "temporary.i_bartenev_ga4_app_api_sessions",

        "dimension_list": [
            "source",
            "medium",
            "campaignName",
            "platform",
            "city"
        ],
        "metric_list": ["sessions", "totalUsers"],
        "filters": FilterExpression(
                and_group=FilterExpressionList(expressions=[
                    FilterExpression(not_expression=FilterExpression(
                        filter=Filter(
                            field_name="platform",
                            string_filter=Filter.StringFilter(value='web')
                        )
                    )),
                    FilterExpression(
                        filter=Filter(
                            field_name="eventName",
                            string_filter=Filter.StringFilter(value="session_start")
                        )
                    )
                ])
            ),

    }
}

client = clickhouse_connect.get_client(host=os.getenv("HOST_CLICKHOUSE"), port=os.getenv("PORT_CLICKHOUSE"), username=os.getenv("USERNAME_CLICKHOUSE"), password=os.getenv("PASSWORD_CLICKHOUSE"))


def get_data_from_api_ga4(property_id: int,
                          start_date: str,
                          end_date: str,
                          dimension_list: list,
                          metric_list: list,
                          filters: FilterExpression,
                          limit: int = 10000,**kwargs) -> RunReportResponse:

  """
    Make simple daily RunReport request to GA4 property, parse it to pandas.DataFrame

    :param property_id: ID of GA4 property
    :param start_date: Start-date of reporting period in the following format: YYYY-MM-DD
    :param end_date: End-date of reporting period in the following format: YYYY-MM-DD
    :param dimension_list: List of dimension names to get (according to https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/)
    :param metric_list: List of metric names to get (according to https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/)
    :param limit: Limit of rows in each report
    :return: RunReportResponse
  """
  client = BetaAnalyticsDataClient()

  dimensions = [Dimension(name=dim) for dim in dimension_list]
  metrics = [Metric(name=metric) for metric in metric_list]

  request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=filters,
            limit=limit,
            keep_empty_rows=True,
            return_property_quota=True
        )

  print(f"Start report request from {start_date}:")
  response = client.run_report(request)
  print(f"Получено {len(response.rows)}")
  return response

def transform_str_to_int_or_float(data_str:str) -> float|int:
  if type(data_str) != str:
    raise ValueError("Нужно передать строку")

  if "," in data_str:
    data_str = data_str.replace(",",".")
  if "." in data_str:
    try:
      return float(data_str)
    except Exception as e:
      print( data_str + " не float")
  else:
    try:
      return int(data_str)
    except Exception as e:
      print( data_str + " не int и не float")

def clear_data_from_quotation_mark(data_str:str) -> str:
  return data_str.replace("'","")

def return_data_for_ch_query(responseGa4:RunReportResponse, date:str) -> str:
  def _get_data(objects_data,name_attr):
    return (getattr(object_data,name_attr) for object_data in objects_data)
  
  done_str = ""
  for row in responseGa4.rows:
    dimension_values = map(clear_data_from_quotation_mark,_get_data(row.dimension_values,"value"))
    metric_values = map(transform_str_to_int_or_float,_get_data(row.metric_values,"value"))
    done_str += str(tuple(chain((date,),dimension_values,metric_values))) + ","
  
  return done_str[:-1]




# sessions = get_data_from_api_ga4(property_id=PROPERTY_ID_GA4, start_date=day, end_date=day,**FIELDS_TO_SET['sessions'] )



def send_data_to_clickhouse(table,values,client=client):
  q_insert = f"""INSERT INTO {table} VALUES"""
  return client.query(q_insert + values)


def get_data_from_ga4_and_send_to_clickhouse(property_id: int,
                          day,
                          settings,
                          field_in_settings = 'sessions',
                          client = client,
                          ):
  response_ga4 = get_data_from_api_ga4(property_id=property_id, start_date=day, end_date=day,**settings[field_in_settings] )
  print(f"Данные из ga4 для {field_in_settings} получены")

  response_clickhouse = send_data_to_clickhouse(
                          settings[field_in_settings]["table_name"],
                          return_data_for_ch_query(response_ga4,day),
                          client
                          
                          )
  
  print(f"Данные в clickhouse для {field_in_settings} отправлены")

  return response_clickhouse


PROPERTY_ID_GA4 = os.getenv("PROPERTY_ID_GA4")

day = "2024-04-02"

print(get_data_from_ga4_and_send_to_clickhouse(PROPERTY_ID_GA4,day,FIELDS_TO_SET,field_in_settings='sessions',client=client))
print(get_data_from_ga4_and_send_to_clickhouse(PROPERTY_ID_GA4,day,FIELDS_TO_SET,field_in_settings='transactions',client=client))




