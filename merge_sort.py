"""
归并排序算法实现
Merge Sort Algorithm Implementation
"""


def merge_sort(arr):
    """
    归并排序主函数
    
    参数:
        arr: 待排序的列表
    
    返回:
        排序后的新列表
    """
    # 基本情况：如果数组长度为 0 或 1，直接返回
    if len(arr) <= 1:
        return arr
    
    # 将数组分成两半
    mid = len(arr) // 2
    left = arr[:mid]
    right = arr[mid:]
    
    # 递归地对两半进行排序
    left = merge_sort(left)
    right = merge_sort(right)
    
    # 合并两个已排序的数组
    return merge(left, right)


def merge(left, right):
    """
    合并两个已排序的数组
    
    参数:
        left: 左侧已排序数组
        right: 右侧已排序数组
    
    返回:
        合并后的已排序数组
    """
    result = []
    i = j = 0
    
    # 比较两个数组的元素，将较小的添加到结果中
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    
    # 将剩余的元素添加到结果中
    result.extend(left[i:])
    result.extend(right[j:])
    
    return result


def merge_sort_inplace(arr, left=0, right=None):
    """
    原地归并排序（使用索引而非创建新列表）
    
    参数:
        arr: 待排序的列表
        left: 左边界索引
        right: 右边界索引
    
    返回:
        排序后的列表（原地修改）
    """
    if right is None:
        right = len(arr) - 1
    
    if left < right:
        mid = (left + right) // 2
        
        # 递归排序左右两半
        merge_sort_inplace(arr, left, mid)
        merge_sort_inplace(arr, mid + 1, right)
        
        # 合并
        merge_inplace(arr, left, mid, right)
    
    return arr


def merge_inplace(arr, left, mid, right):
    """
    原地合并两个已排序的子数组
    
    参数:
        arr: 原数组
        left: 左边界
        mid: 中间位置
        right: 右边界
    """
    # 创建临时数组
    left_arr = arr[left:mid + 1]
    right_arr = arr[mid + 1:right + 1]
    
    i = j = 0
    k = left
    
    # 合并到原数组
    while i < len(left_arr) and j < len(right_arr):
        if left_arr[i] <= right_arr[j]:
            arr[k] = left_arr[i]
            i += 1
        else:
            arr[k] = right_arr[j]
            j += 1
        k += 1
    
    # 复制剩余元素
    while i < len(left_arr):
        arr[k] = left_arr[i]
        i += 1
        k += 1
    
    while j < len(right_arr):
        arr[k] = right_arr[j]
        j += 1
        k += 1


if __name__ == "__main__":
    # 测试用例
    test_cases = [
        [64, 34, 25, 12, 22, 11, 90],
        [5, 2, 8, 1, 9],
        [1],
        [],
        [3, 3, 3, 3],
        [9, 7, 5, 3, 1],
    ]
    
    print("归并排序测试")
    print("=" * 50)
    
    for i, test in enumerate(test_cases, 1):
        original = test.copy()
        sorted_arr = merge_sort(test)
        print(f"测试 {i}:")
        print(f"  原始数组: {original}")
        print(f"  排序结果: {sorted_arr}")
        print()
    
    # 测试原地排序
    print("原地归并排序测试")
    print("=" * 50)
    test_arr = [64, 34, 25, 12, 22, 11, 90]
    print(f"原始数组: {test_arr}")
    merge_sort_inplace(test_arr)
    print(f"排序结果: {test_arr}")
