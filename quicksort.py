"""
快速排序算法实现
Quick Sort Algorithm Implementation
"""


def quicksort(arr):
    """
    快速排序 - 返回新排序数组
    
    Args:
        arr: 待排序的列表
        
    Returns:
        排序后的新列表
    """
    if len(arr) <= 1:
        return arr
    
    # 选择中间元素作为基准值
    pivot = arr[len(arr) // 2]
    
    # 分区：小于、等于、大于基准值的元素
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    
    # 递归排序并合并
    return quicksort(left) + middle + quicksort(right)


def quicksort_inplace(arr, low=0, high=None):
    """
    快速排序 - 原地排序版本
    
    Args:
        arr: 待排序的列表
        low: 起始索引
        high: 结束索引
        
    Returns:
        排序后的原列表
    """
    if high is None:
        high = len(arr) - 1
    
    if low < high:
        # 分区操作，返回基准值的最终位置
        pivot_index = partition(arr, low, high)
        
        # 递归排序基准值左右两侧
        quicksort_inplace(arr, low, pivot_index - 1)
        quicksort_inplace(arr, pivot_index + 1, high)
    
    return arr


def partition(arr, low, high):
    """
    分区函数 - 将数组分为小于和大于基准值的两部分
    
    Args:
        arr: 待分区列表
        low: 起始索引
        high: 结束索引
        
    Returns:
        基准值的最终位置索引
    """
    # 选择最右侧元素作为基准值
    pivot = arr[high]
    i = low - 1  # i 指向小于 pivot 区域的最后一个元素
    
    for j in range(low, high):
        if arr[j] <= pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]
    
    # 将基准值放到正确位置
    arr[i + 1], arr[high] = arr[high], arr[i + 1]
    return i + 1


if __name__ == "__main__":
    # 测试示例
    test_cases = [
        [64, 34, 25, 12, 22, 11, 90],
        [5, 2, 8, 1, 9],
        [1],
        [],
        [3, 3, 3, 3],
        [9, 7, 5, 3, 1],
    ]
    
    print("快速排序测试:")
    print("=" * 50)
    
    for i, test in enumerate(test_cases, 1):
        # 测试返回新数组的版本
        result1 = quicksort(test.copy())
        # 测试原地排序版本
        test_copy = test.copy()
        result2 = quicksort_inplace(test_copy)
        
        print(f"测试 {i}:")
        print(f"  原数组: {test}")
        print(f"  排序后: {result1}")
        print(f"  原地排序: {result2}")
        print()
    
    print("所有测试完成!")
