# coding=utf-8
import sys
import os
import gc
import pandas as pd
import tqdm
import sqlite3
import argparse

import cybosAPI
from utils import is_market_open, available_latest_date, preformat_cjk


class CybosDatareaderCLI:
    def __init__(self):
        self.objStockChart = cybosAPI.CpStockChart()
        self.objCodeMgr = cybosAPI.CpCodeMgr()
        self.rcv_data = dict()  # RQ후 받아온 데이터 저장 멤버

        self.sv_code_df = pd.DataFrame()
        self.db_code_df = pd.DataFrame()

        sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({'종목코드': sv_code_list, '종목명': sv_name_list},columns=('종목코드', '종목명'))

    def update_price_db(self, db_path):
        """
        db_path: db 파일 경로.
        """
        # 로컬 DB에 저장된 종목 정보 가져와서 dataframe으로 저장
        con = sqlite3.connect(db_path)
        cursor = con.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        db_code_list = cursor.fetchall()
        for i in range(len(db_code_list)):
            db_code_list[i] = db_code_list[i][0]
        db_name_list = list(map(self.objCodeMgr.get_code_name, db_code_list))

        db_latest_list = []
        for db_code in db_code_list:
            cursor.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(db_code))
            db_latest_list.append(cursor.fetchall()[0][0])

        # 현재 db에 저장된 column 명 확인
        if db_latest_list:
            cursor.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 2".format(db_code_list[0]))
            date0, date1 = cursor.fetchall()


        db_code_df = pd.DataFrame({'종목코드': db_code_list, '종목명': db_name_list, '갱신날짜': db_latest_list},
                                  columns=('종목코드', '종목명', '갱신날짜'))
        fetch_code_df = db_code_df

        if not is_market_open():
            latest_date = available_latest_date()
            # 이미 DB 데이터가 최신인 종목들은 가져올 목록에서 제외한다
            already_up_to_date_codes = db_code_df.loc[db_code_df['갱신날짜'] == latest_date]['종목코드'].values
            fetch_code_df = fetch_code_df.loc[fetch_code_df['종목코드'].apply(lambda x: x not in already_up_to_date_codes)]

        tick_unit = '1tick'
        count = 1300000 # 130만개정도면 보통 서버에 저장된끝까지 가져옴
        tick_range = 1
        columns = ['close', 'volume', '누적체결매도', '누적체결매수']

        with sqlite3.connect(db_path) as con:
            cursor = con.cursor()
            tqdm_range = tqdm.trange(len(fetch_code_df), ncols=100)
            for i in tqdm_range:
                code = fetch_code_df.iloc[i]
                update_status_msg = '[{}] {}'.format(code[0], code[1])
                tqdm_range.set_description(preformat_cjk(update_status_msg, 25))

                from_date = 0
                if code[0] in db_code_df['종목코드'].tolist():
                    cursor.execute("SELECT date FROM {} ORDER BY date DESC LIMIT 1".format(code[0]))
                    from_date = cursor.fetchall()[0][0]

                self.objStockChart.RequestT(code[0], ord('T'), tick_range, count, self, from_date)
                df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])

                # 기존 DB와 겹치는 부분 제거
                if from_date != 0:
                    df = df.loc[:from_date]
                    df = df.iloc[:-1]

                # 뒤집어서 저장 (결과적으로 date 기준 오름차순으로 저장됨)
                df = df.iloc[::-1]
                df.to_sql(code[0], con, if_exists='append', index_label='date')

                # 메모리 overflow 방지
                del df
                gc.collect()


def main_cli():
    parser = argparse.ArgumentParser(description='cybos datareader CLI')
    parser.add_argument('--db_file_path', required=True, type=str)
    args = parser.parse_args()

    cybos = CybosDatareaderCLI()
    cybos.update_price_db(args.db_file_path)


if __name__ == "__main__":
    main_cli()