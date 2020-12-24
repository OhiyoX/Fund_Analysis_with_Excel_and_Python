import pandas as pd

def correlation():
    # 相关系数分析
    df = pd.read_csv("resource/csv/fund-net-worth-data.csv",
                     encoding="UTF-8",
                     dtype={"DATE": str})
    df.corr().to_csv("resource/analysis-data/corr.csv",encoding="UTF-8")

if __name__ == '__main__':
    correlation()