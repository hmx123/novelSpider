
import re



CN_NUM = {
    '〇': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '零': 0,
    '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9, '貮': 2, '两': 2,
}
CN_NUM_STR = {
    '〇': '0', '一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '零': '0',
    '壹': '1', '贰': '2', '叁': '3', '肆': '4', '伍': '5', '陆': '6', '柒': '7', '捌': '8', '玖': '9', '貮': '2', '两': '2',
}

CN_UNIT = {
    '十': 10,
    '拾': 10,
    '百': 100,
    '佰': 100,
    '千': 1000,
    '仟': 1000,
    '万': 10000,
    '萬': 10000,
    '亿': 100000000,
    '億': 100000000,
    '兆': 1000000000000,
}

# 大写数字 带位数
def chinese_to_arabic(cn: str) -> int:
    unit = 0  # current
    ldig = []  # digest
    for cndig in reversed(cn):
        if cndig in CN_UNIT:
            unit = CN_UNIT.get(cndig)
            if unit == 10000 or unit == 100000000:
                ldig.append(unit)
                unit = 1
        else:
            dig = CN_NUM.get(cndig)
            if unit:
                dig *= unit
                unit = 0
            ldig.append(dig)
    if unit == 10:
        ldig.append(10)
    val, tmp = 0, 0
    for x in reversed(ldig):
        if x == 10000 or x == 100000000:
            val += tmp * x
            tmp = 0
        else:
            tmp += x
    val += tmp
    return val

def chinese_to_num(str):
    str_list = []
    for st in str:
        str_list.append(CN_NUM_STR[st])
    new_str = ''.join(x for x in str_list)
    return new_str


def SearchNum(str):
    reg = '第(.*)章'
    num = re.search(reg, str)
    return num.group(1)



def chin_to_num(str):
    key_num = SearchNum(str)
    try:
        # 判断这个字符是否是数字
        if not key_num.isdigit():
            # 判断这个字符串是否是中文位
            if '十' in key_num or '拾' in key_num or '百' in key_num or '佰' in key_num:
                new_num = chinese_to_arabic(key_num)
            else:
                new_num = chinese_to_num(key_num)
        else:
            new_num = key_num
        return new_num
    except Exception as e:
        print(e)
        print(key_num)




if __name__ == '__main__':
    str = '第九零二章 守护神'
    print(chin_to_num(str))

