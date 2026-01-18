# -*- coding: utf-8 -*-
from zvt.domain import Stock

def main():
    df = Stock.query_data(provider='qmt', return_type='df')
    if df is not None and not df.empty:
        print(f"当前 QMT 数据库中的股票总数: {len(df)}")
    else:
        print("当前 QMT 数据库中没有股票数据。")

if __name__ == "__main__":
    main()
