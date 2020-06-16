@ECHO ON
title crawl start

cd C:\Users\Hoon\PycharmProjects\tickdata

call activate py37_32
python cybos_data.py --db_file_path ./db/stock_price(tick).db
cmd.exe