import multiprocessing
import os
import time


def f1(x,y,z,w):
    return x,y,z,w

if __name__=="__main__":
    pool = multiprocessing.Pool(os.cpu_count())
    list1 = range(1, 2000000)
    list2 = list(zip(list1, list1, list1, list1)) # [ (int, int, int , int) ]
    list3 = [ ( [x[0]+2, x[1]*2], x[2], { x[3]: x[3]+2 }, x[3] ) for x in list2] # [ (list, int, dict , int) ]

    start = time.time()
    result = pool.starmap(f1, list2)
    end = time.time()
    print('Parallel with simple tuple took ' + str(end-start) + ' seconds')

    pool.close()
    pool.join()
    pool = multiprocessing.Pool(os.cpu_count())

    start = time.time()
    result2 = pool.starmap(f1, list3)
    end = time.time()
    print('Parallel with complex tuple took ' + str(end-start) + ' seconds')


