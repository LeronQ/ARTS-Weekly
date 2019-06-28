class Solution:
    def nextGreatestLetter(self, letters: List[str], target: str) -> str:
         
        # if the letters is empty,and then return None
        if len (letters) == 0:
            return None
        
        # if the letters is not empty,and then computing
        start = 0
        end = len (letters) - 1
        res = 0
        
        while start <= end:
            mid = start +  (end - start)//2
            if letters[mid] >target:
                res = mid
                end = mid -1
            else:  
                start = mid + 1
        return letters[res]