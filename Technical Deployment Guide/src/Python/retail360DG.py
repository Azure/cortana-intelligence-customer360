import os
import sys
import json
import pytz
import itertools
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

def generate_telemetry(time_sequence, distributions, offset, total=1000, round=1, rand_seed=1234):
    '''
    Generate telemetry dataframe
    :param time_sequence output from generate_time_sequence(...)
    :param distributions ((n1, n2), (n1, n2), (n1, n2)) where each tuple pair sets the random distribution for a category class
    :param offset customer offset
    :param total total customers to work on
    :param round value for np.round(x) for each category value
    '''
    np.random.seed(rand_seed)
    n = len(time_sequence)
    t1_dist, t2_dist, t3_dist = distributions
    print('Generating telemetry for {} - {}'.format(offset+1, offset+total))

    df = pd.DataFrame()
    for i in np.arange(offset + 1, offset + total + 1):
        customer = pd.DataFrame({"userdatetime": time_sequence})
        customer["customerid"] = i
        customer["category_t1"] = abs(np.round(np.random.normal(*t1_dist, size=n), round))
        customer["category_t2"] = abs(np.round(np.random.normal(*t2_dist, size=n), round))
        customer["category_t3"] = abs(np.round(np.random.normal(*t3_dist, size=n), round))
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

def seed_browsing_df(new_seed=1234):
    '''
    Return a dataframe with generated with new seed
    :param new_seed [int] see to create new browsing data with
    '''
    print('Generating new batch of browsing dataset with seed: {}'.format(new_seed))
    df = pd.concat((
        generate_telemetry(time_sequence, ((700, 50), (10, 10), (15, 10)), offset=0, rand_seed=new_seed),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=1000, rand_seed=new_seed),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=2000, rand_seed=new_seed),
        generate_telemetry(time_sequence, ((10, 10), (1000, 50), (650, 10)), offset=3000, rand_seed=new_seed),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=4000, rand_seed=new_seed),
        generate_telemetry(time_sequence, ((10, 10), (10, 10), (15, 10)), offset=5000, rand_seed=new_seed))).sample(frac=0.5)
    return df 

def generate_seed_digits(num_of_digits=4):
    ''' 
    Returns a random digits of size num_of_digits
    :param num_of_digits[int] size of digits to return
    '''
    # Use set difference to extract the first digit out of the population
    #   - Generate the first integer randomly between 1 through 9. i.e. avoid 0 as first digit.
    #   - Remove first_digit from sample set
    #   - Last num_of_digits - 1 is generated as 3 distinct elements from the remaining values in our sample set
    sample_set = set(range(10))
    first_digit = np.random.randint(1, 9)
    last_digits = np.random.choice(list(sample_set - {first_digit}), (num_of_digits - 1), replace=False)
    return int(str(first_digit) + ''.join(map(str, last_digits)))

if __name__ == '__main__':

    # Constant definition
    NUM_SESSIONS_TO_RUN = 4
    SESSION_SLICE = 900 # 15mins in secs

    # runtime configuration
    start = datetime(2017, 1, 1, 6, 00, 00, tzinfo=pytz.utc)
    end = datetime(2017, 3, 8, 6, 00, 00, tzinfo=pytz.utc)
    step = timedelta(days=1)
    time_sequence = generate_time_sequence(start, end, step)
    
    # 201k messages in ~10mins
    # i.e. Send 1675 messages every 5 seconds
    batch_size = 1675 
    batch_delay_seconds = 5
    
    # event hub configuration
    sbs_namespace = os.environ.get('EVENTHUB_NAMESPACE')
    sbs_access_key_name = os.environ.get('EVENTHUB_KEY_NAME')
    sbs_access_key_value = os.environ.get('EVENTHUB_KEY')
    eh_name = os.environ.get('EVENTHUB_NAME')
   
    # send data to event hub
    sbs = ServiceBusService2(
        service_namespace = sbs_namespace, 
        shared_access_key_name = sbs_access_key_name,
        shared_access_key_value = sbs_access_key_value,
        request_session = sbs_request_session(sbs_namespace))

    # build data that records four sessions 
    # Generate browsing data to egest into the pipeline for 1hour (1 sessions/15min)
    # i.e. 60+ days of browsing data for each session. One session
    # runs for 15 minutes 
    for i in np.arange(NUM_SESSIONS_TO_RUN):
        print('[SESSION INFO] - Begin Session: #{}'.format(i+1))
        df = seed_browsing_df(generate_seed_digits())
        print('[INFO] - Length of activities to send: {}'.format(len(df)))
        eh_start_time = datetime.now()
        total_messages_sent = 0
        while total_messages_sent < len(df):
            batch_start_time = datetime.now()
            batch = eh_batch_from_df(df, total_messages_sent, batch_size)
            payload = bytes(json.dumps(batch), 'utf8')
            print('[EVENTHUB] - Sending events {} - {}'.format(total_messages_sent+1, total_messages_sent+len(batch)))
            sbs.send_event_batch(eh_name, payload)
            total_messages_sent += len(batch)
            print('[EVENTHUB] - Completed batch in {}. Sleeping for {} seconds'.format(datetime.now() - batch_start_time, batch_delay_seconds))
            print('[EVENTHUB] -     {} seconds elapsed so far for session #{}'.format((datetime.now() - eh_start_time), i+1))
            sleep(batch_delay_seconds)
        print('[EVENTHUB] - Completed all batches for Session #{} in {}'.format(i+1, (datetime.now() - eh_start_time)))
        
        # Avoid slice overlap. Guarantee all 60+ browsing is sent in session slice
        session_batch_secs = (datetime.now() - eh_start_time).total_seconds() 
        if session_batch_secs < SESSION_SLICE:
            print('Waiting for slice to elapse in {} seconds...'.format((SESSION_SLICE - session_batch_secs)))
            sleep(SESSION_SLICE - session_batch_secs)
        
        print('[SESSION INFO] - Finished Session: #{}'.format(i+1))
    
    # Trigger a continuous webjob.
    while True:
        print('Sleeping...')
        sleep(60)
