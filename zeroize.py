#对不完整的股票代码补零single_code为str类型
def zeroize(single_code):
    single_code_prs = list(single_code)
    while len(single_code_prs) < 6:
        single_code_prs.insert(0,'0')
    single_code_prs = "".join(single_code)
    single_code_prs = str(single_code_prs)

    return single_code_prs