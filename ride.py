from typing import List,Dict
from decimal import Decimal
from datetime import datetime


class Ride():
    def __init__(self,arr: List[str]):
        self.time = arr[0]
        self.open = float(arr[1])
        self.high = float(arr[2])
        self.low = float(arr[3])
        self.close = float(arr[4])
        self.volume = int(arr[5])
        self.id = str(arr[6])

    @classmethod
    def from_json(cls,dict:Dict):
        return cls(
            arr=[
                dict['time'],
                dict['open'],
                dict['high'],
                dict['low'],
                dict['close'],
                dict['volume']
            ]
        )
