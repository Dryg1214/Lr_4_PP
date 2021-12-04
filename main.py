import pandas as pd
from matplotlib import pyplot as plt
from multiprocessing import Pool
from time import time
from matplotlib import style


def max_ill(df, left_index: int, right_index: int):
    if (left_index > right_index):
        raise ValueError("Левый индекс не должен быть больше правого!")
    max_value = df['Заболели'].values[left_index]
    while(left_index < right_index):
        if df['Заболели'].values[left_index] > max_value:
            max_value = df['Заболели'].values[left_index]
        left_index += 1
    return max_value


def get_iterable(num_processes, object):
    ln = len(df.index) - 1
    step = int(ln / num_processes) + 1
    left = 0
    right = step
    lst = list()
    if right == ln or step > ln:
        lst.append(tuple((object, left, ln)))
    while(right < ln):
        lst.append(tuple((object, left, right)))
        if (right + step) > ln:
            lst.append(tuple((object, left + step, ln)))
        right += step
        left += step
    return lst


def work_num_processes(num_processes, obj):
    with Pool(processes=num_processes) as p:
        rez = p.starmap(max_ill, get_iterable(num_processes, obj))
        # p.close()
        # p.join()
    return max(rez)


def get_time_pr(num_processes, obj):
    start = time()
    print(work_num_processes(num_processes, obj))
    end = time()
    return end - start


"""
def work_num_PROC(num_processes, obj):
    with Pool(processes=num_processes) as p:
        rez = p.starmap(max_ill, iterable=[(df, 0, 54), (df, 54, 108), (df, 108, 162),
                                           (df, 162, 216), (df, 216, 217)])
        p.close()
        p.join()
    return max(rez)
"""

if __name__ == '__main__':
    path_file = "data.csv"
    df = pd.read_csv(path_file, encoding='cp1251')
    print("Число столбцов таблицы: ", len(df.columns))
    print(df.columns)
    print(df.dtypes)
    print("Число строк в таблице: ", len(df.index))
    df = df.fillna(0)
    # print(os.cpu_count())
    print("Максимальное число заболевших среди всех стран")
    df['Заболели'] = df['Заболели'].apply(
        lambda _str: int(_str.replace("'", "").strip()))

    style.use('ggplot')
    x = [1, 2, 4, 8, 16, 24]
    y = [
        get_time_pr(
            1, df), get_time_pr(
            2, df), get_time_pr(
                4, df), get_time_pr(
                    8, df), get_time_pr(
                        16, df), get_time_pr(
                            24, df)]
    plt.plot(x, y, 'b', label='line one', linewidth=5)
    plt.title('График времени поиска')
    plt.ylabel('Затраченное время')
    plt.xlabel('Количество задействованных процессов')
    plt.show()


"""
    start = time()
    max_ill(df, 0, 127)
    end = time()
    print(end - start)
    print(get_time_pr(1, df))
    print(get_time_pr(2, df))
    print(get_time_pr(4, df))
    print(get_time_pr(8, df))
    print(get_time_pr(16, df))
    print(get_time_pr(24, df))
"""
