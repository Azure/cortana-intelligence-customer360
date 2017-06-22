import os
import json
import pytz
import pandas as pd
import numpy as np
from azure.servicebus import (ServiceBusService, _common_error, _common_serialization, _http)
from datetime import datetime, timedelta
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from time import sleep

class ServiceBusService2(ServiceBusService):
  def __init__(self, **kwargs):
    ServiceBusService.__init__(self, **kwargs)

  def send_event_batch(self, hub_name, messages, device_id=None):
    '''
    Sends a new batch of message events to an Event Hub.
    '''
    _common_error._validate_not_none('hub_name', hub_name)
    request = _http.HTTPRequest()
    request.method = 'POST'
    request.host = self._get_host()
    if device_id:
      request.path = '/{0}/publishers/{1}/messages?api-version=2014-01'.format(hub_name, device_id)
    else:
      request.path = '/{0}/messages?api-version=2014-01'.format(hub_name)
    request.body = _common_serialization._get_request_body(messages)
    request.path, request.query = self._httpclient._update_request_uri_query(request)
    request.headers.append(('Content-Type', 'application/vnd.microsoft.servicebus.json'))
    request.headers = self._update_service_bus_header(request)
    self._perform_request(request)

def generate_time_sequence(start_time, end_time, delta_steps, time_format="%Y-%m-%dT%H:%M:%S.%fZ"):
    '''
    Generate the time sequence in a described format based on a time delta
    :param start_time:
    :param end_time:
    :param delta_steps:
    :param time_format:
    :return: list
    '''
    seq = []
    while start_time <= end_time:
        seq.append(start_time.isoformat('T'))
        start_time += delta_steps
    return seq

def generate_telemetry(time_sequence, distributions, offset, total=1000, round=1):
    '''
    Generate telemetry dataframe
    :param time_sequence output from generate_time_sequence(...)
    :param distributions ((n1, n2), (n1, n2), (n1, n2)) where each tuple pair sets the random distribution for a category class
    :param offset customer offset
    :param total total customers to work on
    :param round value for np.round(x) for each category value
    '''
    np.random.seed(1234)
    n = len(time_sequence)
    t1_dist, t2_dist, t3_dist = distributions
    print('Generating telemetry for {} - {}'.format(offset+1, offset+total))

    df = pd.DataFrame()
    for i in np.arange(offset + 1, offset + total + 1):
        customer = pd.DataFrame({"userDatetime": time_sequence})
        customer["customerID"] = i
        customer["category_T1"] = abs(np.round(np.random.normal(*t1_dist, size=n), round))
        customer["category_T2"] = abs(np.round(np.random.normal(*t2_dist, size=n), round))
        customer["category_T3"] = abs(np.round(np.random.normal(*t3_dist, size=n), round))
        df = df.append(customer)
    return df
    
def sbs_request_session(namespace, max_retries=5, backoff=0.1):
    '''
    Create a request session to manage retries for ServiceBusService
    :param namespace ServiceBus namespace name
    :param max_retries max allowed retries
    :param backoff modify wait time before retry
    '''
    retries = Retry(total=max_retries, connect=max_retries, backoff_factor=backoff)
    request_session = Session()
    request_session.mount('https://{}.servicebus.windows.net'.format(namespace), HTTPAdapter(max_retries=retries))
    return request_session
    
def eh_batch_from_df(df, offset, max):
    '''
    Return a slice from a dataframe formatted as an Event Hub batch
    :param df dataframe
    :param offset row offset
    :param max max rows in batch
    '''
    return list(map(lambda x: {"Body": json.dumps(x)}, df.iloc[offset:offset+max].to_dict(orient='records')))

if __name__ == '__main__':

    # runtime configuration
    start = datetime(2017, 3, 1, 6, 00, 00, tzinfo=pytz.utc)
    end = datetime(2017, 3, 2, 6, 00, 00, tzinfo=pytz.utc)
    step = timedelta(hours=6)
    time_sequence = generate_time_sequence(start, end, step)
    
    # 210K messages in ~1 hour, or ~60 messages per second
    batch_size = 100
    batch_delay_seconds = 12
    
    # event hub configuration
    sbs_namespace = os.environ.get('EVENTHUB_NAMESPACE')
    sbs_access_key_name = os.environ.get('EVENTHUB_KEY_NAME')
    sbs_access_key_value = os.environ.get('EVENTHUB_KEY')
    eh_name = os.environ.get('EVENTHUB_NAME')
   
    # build data
    df = pd.concat((
        generate_telemetry(time_sequence, ((450, 50), (10, 10), (15, 10)), offset=0),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=1000),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=2000),
        generate_telemetry(time_sequence, ((450, 50), (10, 10), (15, 10)), offset=3000),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=4000),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=5000)))
    
    # send data to event hub
    sbs = ServiceBusService2(
        service_namespace = sbs_namespace, 
        shared_access_key_name = sbs_access_key_name,
        shared_access_key_value = sbs_access_key_value,
        request_session = sbs_request_session(sbs_namespace))
    eh_start_time = datetime.now()
    total_messages_sent = 0
    while total_messages_sent < len(df):
        batch_start_time = datetime.now()
        batch = eh_batch_from_df(df, total_messages_sent, batch_size)
        payload = bytes(json.dumps(batch), 'utf8')
        print('Sending events {} - {}'.format(total_messages_sent+1, total_messages_sent+len(batch)))
        sbs.send_event_batch(eh_name, payload)
        total_messages_sent += len(batch)
        print('Completed batch in {}. Sleeping for {} seconds'.format(datetime.now() - batch_start_time, batch_delay_seconds))
        sleep(batch_delay_seconds)
    print('Completed all batches in {}'.format(datetime.now() - eh_start_time))
    
    while True:
        print('Sleeping...')
        sleep(60)