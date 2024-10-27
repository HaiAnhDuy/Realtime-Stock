from vnstock3 import Vnstock # Nạp thư viện để sử dụng
from datetime import datetime as d
import pandas as pd
from ride import Ride
from kafka import KafkaProducer
from typing import List,Dict
import  time
import json
# test = df.values.tolist()

# for data in test:
#     print (data)

class JsonProducer(KafkaProducer):
    def __init__(self, props:Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def get_data_from_df(data):
        new_lst = []
        lst_data = data.values.tolist()
        for row in lst_data:
            new_lst.append(Ride(arr=row))
        return new_lst
    
    def push_to_kafka(self,topic:str,data:List[Ride]):
        for row in data:
            try:
                self.producer.send(
                    topic,
                    key=row.id,
                    value= row
                )
                print("---------------Đang xử lý ở id {}-------------------".format(row.id))
                time.sleep(5)
            except Exception as e:
                print("ERROR: ",e)
       
    
    
    


if __name__ == "__main__":
    currentime = d.now().strftime('%Y-%m-%d')
    stock = Vnstock().stock(symbol='ACB', source='VCI') 

    df = stock.quote.history(start='2024-01-01',end=currentime, interval='1D') # Thiết lập thời gian tải dữ liệu và khung thời gian tra cứu là 1 ngày
    df = pd.DataFrame(df)
    # df = df.head(20)
    data_time = df['time'].tolist()  
    ids = []
    for row in data_time:
        row = str(row)
        date_only = row.split(' ')[0]
        date_only = d.strptime(date_only, "%Y-%m-%d")
        date_only = d.strftime(date_only,"%Y-%m-%d")

        y,m,dy = date_only.split("-")
        new_id = str(y+m+dy)
        ids.append(new_id)

    df['id'] = ids
    topic = "stock_topic"

    config = {
        'bootstrap_servers':'localhost:9092',
        'key_serializer': lambda x : str(df['id']).encode(),
        'value_serializer': lambda x : json.dumps(x.__dict__,default=str).encode()
    }
    producer = JsonProducer(props=config)
    data = producer.get_data_from_df(df)
    producer.push_to_kafka(topic=topic,data=data)

    

