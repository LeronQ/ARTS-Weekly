
# coding: utf-8

def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr)//2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left,right)


def merge(left,right):
    """合并操作，将两个有序数组left，right合并成一个有序数组
    """
    res = []
    l = 0
    r = 0
    while l < len(left) and r < len(right):
        if left[l] < right[r]:
            res.append(left[l])
            l += 1
        else:
            res.append(right[r])
            r += 1
    # 只要一个数列中还有值，直接添加
    res += left[l:]
    res += right[r:]
    return res

if __name__ =="__main__":
    arr = [1,3,15,7,20,45,35]
    res = merge_sort(arr)
    print(res)            
    

