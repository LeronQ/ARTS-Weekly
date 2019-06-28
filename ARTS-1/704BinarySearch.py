class Solution:
    def search(self, nums: List[int], target: int) -> int:
        if len(nums) == 0: return None
        
        start = 0
        end = len(nums) - 1
        index = 0
        
        while start <= end :
            mid = start + (end - start)//2
            if nums[mid] < target:
                start = mid + 1
            elif nums[mid] == target:
                index = mid
                return index
            else:
                end = mid -1
        return -1