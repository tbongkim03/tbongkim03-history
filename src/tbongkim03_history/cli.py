import pandas as pd
import sys
import argparse


def argp():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count",type=str, help="tbongkim03-history -c <cmd>")
    parser.add_argument("-t", "--top", nargs=2, help="tbongkim03-history -t <num> <YYYY-MM-DD>")

    args = parser.parse_args()
    return args

def top(num, date):
    df = pd.read_parquet('~/data/parquet')
    fdf = df[df['dt'] == date]
    sdf = fdf.sort_values(by='cnt', ascending=False).head(num)
    ddf = sdf.drop(columns=['dt'])
    return ddf.to_string(index=False)
    

def cnt(q): 
    df = read_parquet()
    df = read_parquet('~/data/parquet/')
    fdf = df[df['cmd'].str.contains(q)]
    cnt = fdf['cnt'].sum()
    return cnt

def read_parquet(path='~/data/parquet/'):
    df = pd.read_parquet(path)
    return df

def query():
    args = argp()
    if args.count:
        command = args.count
        count = cnt(command)
        print(f'cmd : {command}를 사용한 횟수는 {count}회 입니다.')
    elif args.top:
        num, date = int(args.top[0]), args.top[1]
        topN = top(num, date)
        print(topN)
    else:
        #parser.print_help()
        pass
    
